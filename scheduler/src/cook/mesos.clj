;;
;; Copyright (c) Two Sigma Open Source, LLC
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;  http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(ns cook.mesos
  (:require [chime :refer [chime-ch]]
            [clj-http.client :as http]
            [clj-time.core :as time]
            [clj-time.periodic :as periodic]
            [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [cook.curator :as curator]
            [cook.datomic :refer (transact-with-retries)]
            [cook.mesos.heartbeat]
            [cook.mesos.monitor]
            [cook.mesos.rebalancer]
            [cook.mesos.scheduler :as sched]
            [cook.util]
            [datomic.api :as d :refer (q)]
            [mesomatic.scheduler]
            [mesomatic.types]
            [metatransaction.core :as mt :refer (db)]
            [metatransaction.utils :as dutils]
            [metrics.counters :as counters]
            [swiss.arrows :refer :all])
  (:import [org.apache.curator.framework.recipes.leader LeaderSelector LeaderSelectorListener]
           org.apache.curator.framework.state.ConnectionState))

;; ============================================================================
;; mesos scheduler etc.

(defn submit-to-mesos
  "Takes a sequence of jobs in jobsystem format and submits them to the mesos
   DB."
  ([conn user jobs]
   (submit-to-mesos conn user jobs []))
  ([conn user jobs additional-txns-per-job]
   (log/info "Submitting jobs to mesos" conn)
   (try
     (let [submit-time (java.util.Date.)]
       (doseq [{:keys [uuid command ncpus memory name retry-count priority]} jobs
               :let [txn {:db/id (d/tempid :db.part/user)
                          :job/command command
                          :job/name "cooksim"
                          :job/max-retries retry-count
                          :job/priority priority
                          :job/resource [{:resource/type :resource.type/cpus
                                          :resource/amount (double ncpus)}
                                         {:resource/type :resource.type/mem
                                          :resource/amount (double memory)}]
                          :job/state :job.state/waiting
                          :job/submit-time submit-time
                          :job/user user
                          :job/uuid uuid}
                     retries 5
                     base-wait 500 ; millis
                     opts {:retry-schedule (cook.util/rand-exponential-seq retries base-wait)}]]
         (if (and (<= memory 200000) (<= ncpus 32))
           (dutils/transact-with-retries!! conn opts (into [txn] additional-txns-per-job))
           (log/error "We chose not to schedule the job" uuid
                      "because it required too many resources:" ncpus
                      "cpus and" memory "MB of memory"))))
     (catch Exception e
       (log/error e "Occurred while trying to 'submit to mesos' (transact with mesos datomic)")
       (throw (ex-info "Exception occurred while trying to submit to mesos"
                       {:exception e
                        :conn conn}
                       e))))))

(counters/defcounter [cook-mesos mesos mesos-leader])

(defn make-mesos-driver
  "Creates a mesos driver

   Parameters:
   mesos-framework-name     -- string, name to use when connecting to mesos.
                               Will be appended with version of cook
   mesos-role               -- string, (optional) The role to connect to mesos with
   mesos-principal          -- string, (optional) principal to connect to mesos with
   gpu-enabled?             -- boolean, (optional) whether cook will schedule gpu jobs
   mesos-failover-timeout   -- long, (optional) time in milliseconds mesos will wait
                               for framework to reconnect.
                               See http://mesos.apache.org/documentation/latest/high-availability-framework-guide/
                               and search for failover_timeout
   mesos-master             -- str, (optional) connection string for mesos masters
   scheduler                -- mesos scheduler implementation
   framework-id             -- str, (optional) Id of framework if it has connected to mesos before
   "
  ([config scheduler]
   (make-mesos-driver config scheduler nil))
  ([{:keys [mesos-framework-name mesos-role mesos-principal gpu-enabled?
            mesos-failover-timeout mesos-master] :as config}
    scheduler framework-id]
   (apply mesomatic.scheduler/scheduler-driver
          scheduler
          (merge
            {:user ""
             :name (str mesos-framework-name
                        "-"
                        cook.util/version)
             :checkpoint true}
            (when mesos-role
              {:role mesos-role})
            (when mesos-principal
              {:principal mesos-principal})
            (when gpu-enabled?
              {:capabilities [{:type :framework-capability-gpu-resources}]})
            (when mesos-failover-timeout
              {:failover-timeout mesos-failover-timeout})
            (when framework-id
              {:id {:value framework-id}}))
          mesos-master
          (when mesos-principal
            [{:principal mesos-principal}]))))

(defn get-mesos-utilization
  "Queries the mesos master to get the utilization.
   Returns the max of cpu and mem utilization as a decimal (e.g. 0.92)"
  [mesos-master-hosts]
  (let [mesos-master-urls (map #(str "http://" % ":5050/metrics/snapshot") mesos-master-hosts)
        get-stats (fn [url] (some->> url
                                     (http/get)
                                     (:body)
                                     (json/read-str)))
        utilization (some-<>> mesos-master-urls
                              (map get-stats)
                              (filter #(pos? (get % "master/elected")))
                              (first)
                              (select-keys <> ["master/cpus_percent" "master/mem_percent"])
                              (vals)
                              (apply max))]
    utilization))

(defn make-trigger-chans
  "Creates a map of of the trigger channels expected by `start-mesos-scheduler`
   Each channel receives chime triggers at particular intervals and it is
   possible to send additional events as desired"
  [rebalancer-config
   {:keys [timeout-interval-minutes]
    :or {timeout-interval-minutes 1}
    :as task-constraints}]
  (letfn [(prepare-trigger-chan [interval]
            (let [ch (async/chan (async/sliding-buffer 1))]
              (async/pipe (chime-ch (periodic/periodic-seq (time/now) interval))
                          ch)
              ch))]
    {:cancelled-task-trigger-chan (prepare-trigger-chan (time/seconds 3))
     :lingering-task-trigger-chan (prepare-trigger-chan (time/minutes timeout-interval-minutes))
     :match-trigger-chan (prepare-trigger-chan (time/seconds 1))
     :rank-trigger-chan (prepare-trigger-chan (time/seconds 5))
     :rebalancer-trigger-chan (prepare-trigger-chan (time/seconds (:interval-seconds rebalancer-config)))
     :straggler-trigger-chan (prepare-trigger-chan (time/minutes timeout-interval-minutes))}))

(defn start-mesos-scheduler
  "Starts a leader elector. When the process is leader, it starts the mesos
   scheduler and associated threads to interact with mesos.

   Parameters
   make-mesos-driver-fn     -- fn, function that accepts a mesos scheduler and framework id
                                   and returns a mesos driver
   get-mesos-utilization    -- fn, function with no parameters, returns utilization of cluster [0,1]
   mesos-master-hosts       -- seq[strings], url of mesos masters to query for cluster info
   curator-framework        -- curator object, object for interacting with zk
   mesos-datomic-conn       -- datomic conn, connection to datomic db for interacting with datomic
   mesos-datomic-mult       -- async channel, feed of db writes
   zk-prefix                -- str, prefix in zk for cook data
   offer-incubate-time-ms   -- long, time in millis that offers are allowed to sit before they are declined
   mea-culpa-failure-limit  -- long, max failures of mea culpa reason before it is considered a 'real' failure
                                     see scheduler/docs/configuration.asc for more details
   task-constraints         -- map, constraints on task. See scheduler/docs/configuration.asc for more details
   riemann-host             -- str, dns name of riemann
   riemann-port             -- int, port for riemann
   mesos-pending-jobs-atom  -- atom, Populate (and update) list of pending jobs into atom
   offer-cache              -- atom, map from host to most recent offer. Used to get attributes
   gpu-enabled?             -- boolean, whether cook will schedule gpus
   rebalancer-config        -- map, config for rebalancer. See scheduler/docs/rebalancer-config.asc for details
   framework-id-promise     -- promise object to which we will deliver the Mesos framework id
   fenzo-config             -- map, config for fenzo, See scheduler/docs/configuration.asc for more details"
  [make-mesos-driver-fn get-mesos-utilization curator-framework mesos-datomic-conn mesos-datomic-mult zk-prefix
   offer-incubate-time-ms mea-culpa-failure-limit task-constraints riemann-host riemann-port mesos-pending-jobs-atom
   offer-cache gpu-enabled? rebalancer-config framework-id-promise
   {:keys [fenzo-fitness-calculator fenzo-floor-iterations-before-reset fenzo-floor-iterations-before-warn
           fenzo-max-jobs-considered fenzo-scaleback good-enough-fitness]
    :as fenzo-config}
   {:keys [cancelled-task-trigger-chan lingering-task-trigger-chan rebalancer-trigger-chan straggler-trigger-chan]
    :as trigger-chans}]
  (let [zk-framework-id (str zk-prefix "/framework-id")
        datomic-report-chan (async/chan (async/sliding-buffer 4096))
        mesos-heartbeat-chan (async/chan (async/buffer 4096))
        current-driver (atom nil)
        framework-id (when-let [bytes (curator/get-or-nil curator-framework zk-framework-id)]
                       (String. bytes))
        leader-selector (LeaderSelector.
                          curator-framework
                          zk-prefix
                          ;(ThreadUtils/newThreadFactory "mesos-leader-selector")
                          ;clojure.lang.Agent/pooledExecutor
                          (reify LeaderSelectorListener
                            (takeLeadership [_ client]
                              (log/warn "Taking mesos leadership")
                              ;; TODO: get the framework ID and try to reregister
                              (let [normal-exit (atom true)]
                                (try
                                  (let [{:keys [scheduler view-incubating-offers]}
                                        (sched/create-datomic-scheduler
                                          mesos-datomic-conn
                                          (fn set-or-create-framework-id [framework-id]
                                            (let [value (-> framework-id mesomatic.types/pb->data :value)]
                                              (deliver framework-id-promise value)
                                              (curator/set-or-create
                                                curator-framework
                                                zk-framework-id
                                                (.getBytes value "UTF-8"))))
                                          current-driver
                                          mesos-pending-jobs-atom
                                          offer-cache
                                          mesos-heartbeat-chan
                                          offer-incubate-time-ms
                                          mea-culpa-failure-limit
                                          fenzo-max-jobs-considered
                                          fenzo-scaleback
                                          fenzo-floor-iterations-before-warn
                                          fenzo-floor-iterations-before-reset
                                          fenzo-fitness-calculator
                                          task-constraints
                                          gpu-enabled?
                                          good-enough-fitness
                                          trigger-chans)
                                        driver (make-mesos-driver-fn scheduler framework-id)]
                                    (mesomatic.scheduler/start! driver)
                                    (reset! current-driver driver)

                                    (when riemann-host
                                      (cook.mesos.monitor/riemann-reporter mesos-datomic-conn :riemann-host riemann-host :riemann-port riemann-port))
                                    #_(cook.mesos.scheduler/reconciler mesos-datomic-conn driver)
                                    (cook.mesos.scheduler/lingering-task-killer mesos-datomic-conn driver task-constraints lingering-task-trigger-chan)
                                    (cook.mesos.scheduler/straggler-handler mesos-datomic-conn driver straggler-trigger-chan)
                                    (cook.mesos.scheduler/cancelled-task-killer mesos-datomic-conn driver cancelled-task-trigger-chan)
                                    (cook.mesos.heartbeat/start-heartbeat-watcher! mesos-datomic-conn mesos-heartbeat-chan)
                                    (cook.mesos.rebalancer/start-rebalancer! {:config rebalancer-config
                                                                              :conn  mesos-datomic-conn
                                                                              :driver driver
                                                                              :get-mesos-utilization get-mesos-utilization
                                                                              :offer-cache offer-cache
                                                                              :pending-jobs-atom mesos-pending-jobs-atom
                                                                              :trigger-chan rebalancer-trigger-chan
                                                                              :view-incubating-offers view-incubating-offers})
                                    (counters/inc! mesos-leader)
                                    (async/tap mesos-datomic-mult datomic-report-chan)
                                    (cook.mesos.scheduler/monitor-tx-report-queue datomic-report-chan mesos-datomic-conn current-driver)
                                    (mesomatic.scheduler/join! driver)
                                    (reset! current-driver nil))
                                  (catch Throwable e
                                    (log/error e "Lost mesos leadership due to exception")
                                    (reset! normal-exit false))
                                  (finally
                                    (counters/dec! mesos-leader)
                                    (when @normal-exit
                                      (log/warn "Lost mesos leadership naturally"))
                                    ;; Better to fail over and rely on start up code we trust then rely on rarely run code
                                    ;; to make sure we yield leadership correctly (and fully)
                                    (log/fatal "Lost mesos leadership. Exitting. Expecting a supervisor to restart me!")
                                    (System/exit 0)))))
                            (stateChanged [_ client newState]
                              ;; We will give up our leadership whenever it seems that we lost
                              ;; ZK connection
                              (when (#{ConnectionState/LOST ConnectionState/SUSPENDED} newState)
                                (when-let [driver @current-driver]
                                  (counters/dec! mesos-leader)
                                  ;; Better to fail over and rely on start up code we trust then rely on rarely run code
                                  ;; to make sure we yield leadership correctly (and fully)
                                  (log/fatal "Lost leadership in zookeeper. Exitting. Expecting a supervisor to restart me!")
                                  (System/exit 0))))))]
    (.setId leader-selector (str (java.net.InetAddress/getLocalHost) \- (java.util.UUID/randomUUID)))
    (.autoRequeue leader-selector)
    (.start leader-selector)
    (log/info "Started the mesos leader selector")
    {:submitter (partial submit-to-mesos mesos-datomic-conn)
     :driver current-driver
     :leader-selector leader-selector
     :framework-id framework-id}))

(defn kill-job
  "Kills jobs. It works by marking them completed, which will trigger the subscription
   monitor to attempt to kill any instances"
  [conn job-uuids]
  (when (seq job-uuids)
    (log/info "Killing some jobs!!")
    (doseq [uuids (partition-all 50 job-uuids)]
      (async/<!!
        (transact-with-retries conn
                               (mapv
                                 (fn [job-uuid]
                                   [:db/add [:job/uuid job-uuid] :job/state :job.state/completed])
                                 uuids)
                               (into (repeat 10 500) (repeat 10 1000)))))))

(defn kill-instances
  "Kills instances.  Marks them as cancelled in datomic;
  the cancelled-task-killer will notice this and kill the actual Mesos tasks."
  [conn task-uuids]
  (let [txns (mapv (fn [task-uuid]
                     [:db/add
                      [:instance/task-id (str task-uuid)]
                      :instance/cancelled true])
                   task-uuids)]
    @(d/transact conn txns)))
