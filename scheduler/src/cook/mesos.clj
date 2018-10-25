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
            [clj-time.core :as time]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [cook.config :as config]
            [cook.datomic :refer (transact-with-retries)]
            [cook.mesos.data-locality :as dl]
            [cook.mesos.heartbeat]
            [cook.mesos.monitor]
            [cook.mesos.optimizer :as optimizer]
            [cook.mesos.rebalancer]
            [cook.mesos.scheduler :as sched]
            [cook.mesos.util :as util]
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
          (cond-> {:checkpoint true
                   :name (str mesos-framework-name "-" @cook.util/version "-" @cook.util/commit)
                   :user ""}
                  framework-id (assoc :id {:value framework-id})
                  gpu-enabled? (assoc :capabilities [{:type :framework-capability-gpu-resources}])
                  mesos-failover-timeout (assoc :failover-timeout mesos-failover-timeout)
                  mesos-principal (assoc :principal mesos-principal)
                  mesos-role (assoc :role mesos-role))
          mesos-master
          (when mesos-principal
            [{:principal mesos-principal}]))))

(defn make-trigger-chans
  "Creates a map of of the trigger channels expected by `start-mesos-scheduler`
   Each channel receives chime triggers at particular intervals and it is
   possible to send additional events as desired"
  [rebalancer-config progress-config optimizer-config
   {:keys [timeout-interval-minutes]
    :or {timeout-interval-minutes 1}
    :as task-constraints}]
  (let [{:keys [update-interval-ms]} (config/data-local-fitness-config)
        prepare-trigger-chan (fn prepare-trigger-chan [interval]
                               (let [ch (async/chan (async/sliding-buffer 1))]
                                 (async/pipe (chime-ch (util/time-seq (time/now) interval))
                                             ch)
                                 ch))]
    (cond->
        {:cancelled-task-trigger-chan (prepare-trigger-chan (time/seconds 3))
         :lingering-task-trigger-chan (prepare-trigger-chan (time/minutes timeout-interval-minutes))
         :match-trigger-chan (prepare-trigger-chan (time/seconds 1))
         :optimizer-trigger-chan (prepare-trigger-chan (time/seconds (:optimizer-interval-seconds optimizer-config 10)))
         :progress-updater-trigger-chan (prepare-trigger-chan (time/millis (:publish-interval-ms progress-config)))
         :rank-trigger-chan (prepare-trigger-chan (time/seconds 5))
         :rebalancer-trigger-chan (prepare-trigger-chan (time/seconds (:interval-seconds rebalancer-config)))
         :straggler-trigger-chan (prepare-trigger-chan (time/minutes timeout-interval-minutes))}
      update-interval-ms
      (assoc :update-data-local-costs-trigger-chan (prepare-trigger-chan (time/millis update-interval-ms))))))

(defn start-mesos-scheduler
  "Starts a leader elector. When the process is leader, it starts the mesos
   scheduler and associated threads to interact with mesos.

   Parameters
   make-mesos-driver-fn          -- fn, function that accepts a mesos scheduler and framework id
                                    and returns a mesos driver
   curator-framework             -- curator object, object for interacting with zk
   mesos-datomic-conn            -- datomic conn, connection to datomic db for interacting with datomic
   mesos-datomic-mult            -- async channel, feed of db writes
   zk-prefix                     -- str, prefix in zk for cook data
   offer-incubate-time-ms        -- long, time in millis that offers are allowed to sit before they are declined
   mea-culpa-failure-limit       -- long, max failures of mea culpa reason before it is considered a 'real' failure
                                    see scheduler/docs/configuration.adoc for more details
   task-constraints              -- map, constraints on task. See scheduler/docs/configuration.adoc for more details
   pool-name->pending-jobs-atom  -- atom, Populate (and update) map from pool name to list of pending jobs into atom
   agent-attributes-cache        -- atom, map from agent id to most recent agent attributes
   gpu-enabled?                  -- boolean, whether cook will schedule gpus
   rebalancer-config             -- map, config for rebalancer. See scheduler/docs/rebalancer-config.adoc for details
   progress-config               -- map, config for progress publishing. See scheduler/docs/configuration.adoc
   framework-id                  -- str, the Mesos framework id from the cook settings
   fenzo-config                  -- map, config for fenzo, See scheduler/docs/configuration.adoc for more details
   sandbox-syncer-state          -- map, representing the sandbox syncer object"
  [{:keys [curator-framework exit-code-syncer-state fenzo-config framework-id gpu-enabled? make-mesos-driver-fn
           mea-culpa-failure-limit mesos-datomic-conn mesos-datomic-mult mesos-leadership-atom pool-name->pending-jobs-atom
           mesos-run-as-user agent-attributes-cache offer-incubate-time-ms optimizer-config progress-config rebalancer-config
           sandbox-syncer-state server-config task-constraints trigger-chans zk-prefix]}]
  (let [{:keys [fenzo-fitness-calculator fenzo-floor-iterations-before-reset fenzo-floor-iterations-before-warn
                fenzo-max-jobs-considered fenzo-scaleback good-enough-fitness]} fenzo-config
        {:keys [cancelled-task-trigger-chan lingering-task-trigger-chan optimizer-trigger-chan
                rebalancer-trigger-chan straggler-trigger-chan]} trigger-chans
        {:keys [hostname server-port server-https-port]} server-config
        datomic-report-chan (async/chan (async/sliding-buffer 4096))
        mesos-heartbeat-chan (async/chan (async/buffer 4096))
        current-driver (atom nil)
        pool-name->optimizer-suggested-job-ids-atom (atom {})
        rebalancer-reservation-atom (atom {})
        leader-selector (LeaderSelector.
                          curator-framework
                          zk-prefix
                          ;(ThreadUtils/newThreadFactory "mesos-leader-selector")
                          ;clojure.lang.Agent/pooledExecutor
                          (reify LeaderSelectorListener
                            (takeLeadership [_ client]
                              (log/warn "Taking mesos leadership")
                              (reset! mesos-leadership-atom true)
                              ;; TODO: get the framework ID and try to reregister
                              (let [normal-exit (atom true)]
                                (try
                                  (let [{:keys [scheduler view-incubating-offers]}
                                        (sched/create-datomic-scheduler
                                         {:conn mesos-datomic-conn
                                          :driver-atom current-driver
                                          :exit-code-syncer-state exit-code-syncer-state
                                          :fenzo-fitness-calculator fenzo-fitness-calculator
                                          :fenzo-floor-iterations-before-reset fenzo-floor-iterations-before-reset
                                          :fenzo-floor-iterations-before-warn fenzo-floor-iterations-before-warn
                                          :fenzo-max-jobs-considered fenzo-max-jobs-considered
                                          :fenzo-scaleback fenzo-scaleback
                                          :framework-id framework-id
                                          :good-enough-fitness good-enough-fitness
                                          :gpu-enabled? gpu-enabled?
                                          :heartbeat-ch mesos-heartbeat-chan
                                          :mea-culpa-failure-limit mea-culpa-failure-limit
                                          :mesos-run-as-user mesos-run-as-user
                                          :agent-attributes-cache agent-attributes-cache
                                          :offer-incubate-time-ms offer-incubate-time-ms
                                          :pool-name->optimizer-suggested-job-ids-atom pool-name->optimizer-suggested-job-ids-atom
                                          :pool-name->pending-jobs-atom pool-name->pending-jobs-atom
                                          :progress-config progress-config
                                          :rebalancer-reservation-atom rebalancer-reservation-atom
                                          :sandbox-syncer-state sandbox-syncer-state
                                          :task-constraints task-constraints
                                          :trigger-chans trigger-chans})
                                        driver (make-mesos-driver-fn scheduler framework-id)]
                                    (mesomatic.scheduler/start! driver)
                                    (reset! current-driver driver)

                                    (cook.mesos.monitor/start-collecting-stats)
                                    (cook.mesos.scheduler/lingering-task-killer mesos-datomic-conn driver task-constraints lingering-task-trigger-chan)
                                    (cook.mesos.scheduler/straggler-handler mesos-datomic-conn driver straggler-trigger-chan)
                                    (cook.mesos.scheduler/cancelled-task-killer mesos-datomic-conn driver cancelled-task-trigger-chan)
                                    (cook.mesos.heartbeat/start-heartbeat-watcher! mesos-datomic-conn mesos-heartbeat-chan)
                                    (cook.mesos.rebalancer/start-rebalancer! {:config rebalancer-config
                                                                              :conn mesos-datomic-conn
                                                                              :driver driver
                                                                              :agent-attributes-cache agent-attributes-cache
                                                                              :pool-name->pending-jobs-atom pool-name->pending-jobs-atom
                                                                              :rebalancer-reservation-atom rebalancer-reservation-atom
                                                                              :trigger-chan rebalancer-trigger-chan
                                                                              :view-incubating-offers view-incubating-offers})
                                    (when (seq optimizer-config)
                                      (optimizer/start-optimizer-cycles!
                                        optimizer-config
                                        optimizer-trigger-chan
                                        (fn get-pool-names []
                                          (keys @pool-name->pending-jobs-atom))
                                        (fn pool-name->queue [pool-name]
                                          (get @pool-name->pending-jobs-atom pool-name))
                                        (fn pool-name->running [pool-name]
                                          (->> (util/get-running-task-ents (d/db mesos-datomic-conn))
                                               (filter #(= pool-name (-> % :job/_instance util/job->pool-name)))))
                                        pool-name->optimizer-suggested-job-ids-atom))
                                    (when (:update-data-local-costs-trigger-chan trigger-chans)
                                      (dl/start-update-cycles! mesos-datomic-conn (:update-data-local-costs-trigger-chan trigger-chans)))
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
                                    (log/fatal "Lost mesos leadership. Exiting. Expecting a supervisor to restart me!")
                                    (System/exit 0)))))
                            (stateChanged [_ client newState]
                              ;; We will give up our leadership whenever it seems that we lost
                              ;; ZK connection
                              (when (#{ConnectionState/LOST ConnectionState/SUSPENDED} newState)
                                (reset! mesos-leadership-atom false)
                                (when-let [driver @current-driver]
                                  (counters/dec! mesos-leader)
                                  ;; Better to fail over and rely on start up code we trust then rely on rarely run code
                                  ;; to make sure we yield leadership correctly (and fully)
                                  (if (-> "COOK.SIMULATION" System/getProperty str Boolean/parseBoolean)
                                    (log/warn "Lost leadership in zookeeper. Not exiting as simulation is running.")
                                    (do
                                      (log/fatal "Lost leadership in zookeeper. Exiting. Expecting a supervisor to restart me!")
                                      (System/exit 0))))))))]
    (.setId leader-selector (str hostname \#
                                 (or server-port server-https-port) \#
                                 (if server-port "http" "https") \#
                                 (java.util.UUID/randomUUID)))
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
