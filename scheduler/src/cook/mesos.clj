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
            [cook.compute-cluster :as cc]
            [cook.config :as config]
            [cook.datomic :refer [transact-with-retries]]
            [cook.mesos.heartbeat]
            [cook.monitor]
            [cook.rebalancer]
            [cook.scheduler.data-locality :as dl]
            [cook.scheduler.optimizer]
            [cook.scheduler.scheduler :as sched]
            [cook.tools :as tools]
            [cook.util :as util]
            [datomic.api :as d :refer [q]]
            [mesomatic.scheduler]
            [mesomatic.types]
            [metatransaction.core :refer [db]]
            [metatransaction.utils :as dutils]
            [metrics.counters :as counters]
            [plumbing.core :refer [map-from-keys]]
            [swiss.arrows :refer :all])
  (:import (java.util.concurrent Executors ExecutorService ScheduledExecutorService TimeUnit)
           (org.apache.curator.framework.recipes.leader LeaderSelector LeaderSelectorListener)
           (org.apache.curator.framework.state ConnectionState)))

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
                     opts {:retry-schedule (util/rand-exponential-seq retries base-wait)}]]
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

(defn make-trigger-chans
  "Creates a map of of the trigger channels expected by `start-leader-selector`
   Each channel receives chime triggers at particular intervals and it is
   possible to send additional events as desired"
  [rebalancer-config progress-config optimizer-config
   {:keys [timeout-interval-minutes]
    :or {timeout-interval-minutes 1}
    :as task-constraints}]
  (let [{:keys [update-interval-ms]} (config/data-local-fitness-config)
        prepare-trigger-chan (fn prepare-trigger-chan [interval]
                               (let [ch (async/chan (async/sliding-buffer 1))]
                                 (async/pipe (chime-ch (tools/time-seq (time/now) interval))
                                             ch)
                                 ch))]
    (cond->
        {:cancelled-task-trigger-chan (prepare-trigger-chan (time/seconds 3))
         :lingering-task-trigger-chan (prepare-trigger-chan (time/minutes timeout-interval-minutes))
         :match-trigger-chan (async/chan (async/sliding-buffer 1))
         :optimizer-trigger-chan (prepare-trigger-chan (time/seconds (:optimizer-interval-seconds optimizer-config 10)))
         :progress-updater-trigger-chan (prepare-trigger-chan (time/millis (:publish-interval-ms progress-config)))
         :rank-trigger-chan (prepare-trigger-chan (time/seconds 5))
         :rebalancer-trigger-chan (prepare-trigger-chan (time/seconds (:interval-seconds rebalancer-config)))
         :straggler-trigger-chan (prepare-trigger-chan (time/minutes timeout-interval-minutes))}
      update-interval-ms
      (assoc :update-data-local-costs-trigger-chan (prepare-trigger-chan (time/millis update-interval-ms))))))

(def ^ScheduledExecutorService compute-cluster-config-updater-executor (Executors/newSingleThreadScheduledExecutor))

(defn make-compute-cluster-config-updater-task
  "Create a Runnable that periodically checks for updated compute cluster configurations"
  [conn new-cluster-configurations-fn]
  (reify
    Runnable
    (run [_]
      (try
        (cc/update-compute-clusters conn nil (new-cluster-configurations-fn) false)
        (catch Exception ex
          (log/error ex "Failed to update cluster configurations"))))))

(defn dynamic-compute-cluster-configurations-setup
  "Set up dynamic compute cluster configurations. This includes loading existing configurations from the database
  at startup, and launching a background worker to periodically check for new configurations"
  [conn {:keys [cluster-update-period-seconds new-cluster-configurations-fn]}]
  {:pre [new-cluster-configurations-fn]}
  (let [cluster-update-period-seconds (or cluster-update-period-seconds 60)
        db (d/db conn)
        saved-cluster-configurations (cc/db-config-ents->configs (cc/db-config-ents db))
        cluster-name->running-task-ents (->> db cook.tools/get-running-task-ents
                                             (group-by #(-> %
                                                          :instance/compute-cluster
                                                          :compute-cluster/cluster-name)))
        [missing-cluster-names _ _] (tools/diff-map-keys cluster-name->running-task-ents saved-cluster-configurations)]
    (when missing-cluster-names
      (log/error "Can't find cluster configurations for some of the running jobs!"
                 {:missing-cluster-names missing-cluster-names
                  :instances (->> missing-cluster-names (map-from-keys #(take 10 (cluster-name->running-task-ents %))))}))
    (cc/update-compute-clusters conn nil saved-cluster-configurations false)
    (.scheduleAtFixedRate compute-cluster-config-updater-executor
                          (make-compute-cluster-config-updater-task conn (util/lazy-load-var new-cluster-configurations-fn))
                          cluster-update-period-seconds ;initial delay
                          cluster-update-period-seconds ;period
                          TimeUnit/SECONDS)))

(defn start-leader-selector
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
  [{:keys [curator-framework fenzo-config mea-culpa-failure-limit mesos-datomic-conn mesos-datomic-mult
           mesos-heartbeat-chan leadership-atom pool-name->pending-jobs-atom mesos-run-as-user agent-attributes-cache
           offer-incubate-time-ms optimizer-config rebalancer-config server-config task-constraints trigger-chans
           zk-prefix]}]
  (let [{:keys [fenzo-fitness-calculator fenzo-floor-iterations-before-reset fenzo-floor-iterations-before-warn
                fenzo-max-jobs-considered fenzo-scaleback good-enough-fitness]} fenzo-config
        {:keys [cancelled-task-trigger-chan lingering-task-trigger-chan optimizer-trigger-chan
                rebalancer-trigger-chan straggler-trigger-chan]} trigger-chans
        {:keys [hostname server-port server-https-port]} server-config
        datomic-report-chan (async/chan (async/sliding-buffer 4096))
        cluster-name->compute-cluster @cc/cluster-name->compute-cluster-atom
        rebalancer-reservation-atom (atom {})
        _ (log/info "Using path" zk-prefix "for leader selection")
        leader-selector (LeaderSelector.
                          curator-framework
                          zk-prefix
                          ;(ThreadUtils/newThreadFactory "mesos-leader-selector")
                          ;clojure.lang.Agent/pooledExecutor
                          (reify LeaderSelectorListener
                            (takeLeadership [_ client]
                              (log/warn "Taking leadership")
                              (reset! leadership-atom true)
                              ;; TODO: get the framework ID and try to reregister
                              (let [normal-exit (atom true)]
                                (try
                                  (let [{:keys [pool-name->fenzo view-incubating-offers] :as scheduler}
                                        (sched/create-datomic-scheduler
                                          {:conn mesos-datomic-conn
                                           :cluster-name->compute-cluster-atom cook.compute-cluster/cluster-name->compute-cluster-atom
                                           :fenzo-fitness-calculator fenzo-fitness-calculator
                                           :fenzo-floor-iterations-before-reset fenzo-floor-iterations-before-reset
                                           :fenzo-floor-iterations-before-warn fenzo-floor-iterations-before-warn
                                           :fenzo-max-jobs-considered fenzo-max-jobs-considered
                                           :fenzo-scaleback fenzo-scaleback
                                           :good-enough-fitness good-enough-fitness
                                           :mea-culpa-failure-limit mea-culpa-failure-limit
                                           :mesos-run-as-user mesos-run-as-user
                                           :agent-attributes-cache agent-attributes-cache
                                           :offer-incubate-time-ms offer-incubate-time-ms
                                           :pool-name->pending-jobs-atom pool-name->pending-jobs-atom
                                           :rebalancer-reservation-atom rebalancer-reservation-atom
                                           :task-constraints task-constraints
                                           :trigger-chans trigger-chans})
                                        cluster-connected-chans (->> cluster-name->compute-cluster
                                                                     (map (fn [[compute-cluster-name compute-cluster]]
                                                                            (try
                                                                              (cc/initialize-cluster compute-cluster
                                                                                                     pool-name->fenzo)
                                                                              (catch Throwable t
                                                                                (log/error t "Error launching compute cluster" compute-cluster-name)
                                                                                ; Return a chan that never gets a message on it.
                                                                                (async/chan 1)))))
                                                                     ; Note: This doall has a critical side effect of actually initializing
                                                                     ; all of the clusters.
                                                                     doall)]
                                    (deliver cc/scheduler-promise scheduler)
                                    (cook.monitor/start-collecting-stats)
                                    ; Many of these should look at the compute-cluster of the underlying jobs, and not use driver at all.
                                    (cook.scheduler.scheduler/lingering-task-killer mesos-datomic-conn
                                                                                    task-constraints lingering-task-trigger-chan)
                                    (cook.scheduler.scheduler/straggler-handler mesos-datomic-conn straggler-trigger-chan)
                                    (cook.scheduler.scheduler/cancelled-task-killer mesos-datomic-conn
                                                                                    cancelled-task-trigger-chan)
                                    (cook.mesos.heartbeat/start-heartbeat-watcher! mesos-datomic-conn mesos-heartbeat-chan)
                                    (cook.rebalancer/start-rebalancer! {:config rebalancer-config
                                                                        :conn mesos-datomic-conn
                                                                        :agent-attributes-cache agent-attributes-cache
                                                                        :pool-name->pending-jobs-atom pool-name->pending-jobs-atom
                                                                        :rebalancer-reservation-atom rebalancer-reservation-atom
                                                                        :trigger-chan rebalancer-trigger-chan
                                                                        :view-incubating-offers view-incubating-offers})
                                    (when (seq optimizer-config)
                                      (cook.scheduler.optimizer/start-optimizer-cycles! (fn get-queue []
                                                                                      ;; TODO Use filter of queue that scheduler uses to filter to considerable.
                                                                                      ;;      Specifically, think about filtering to jobs that are waiting and
                                                                                      ;;      think about how to handle quota
                                                                                      @pool-name->pending-jobs-atom)
                                                                                    (fn get-running []
                                                                                      (tools/get-running-task-ents (d/db mesos-datomic-conn)))
                                                                                    view-incubating-offers
                                                                                    optimizer-config
                                                                                    optimizer-trigger-chan))
                                    (when (:update-data-local-costs-trigger-chan trigger-chans)
                                      (dl/start-update-cycles! mesos-datomic-conn (:update-data-local-costs-trigger-chan trigger-chans)))
                                    ; This counter exists so that the cook leader has a '1' and the others have a '0'.
                                    ; So we can see who the leader is in the graphs by graphing the value.
                                    (counters/inc! mesos-leader)
                                    (async/tap mesos-datomic-mult datomic-report-chan)
                                    (cook.scheduler.scheduler/monitor-tx-report-queue datomic-report-chan mesos-datomic-conn)
                                    (when-let [compute-cluster-update-options (config/compute-cluster-update-options)]
                                      (dynamic-compute-cluster-configurations-setup mesos-datomic-conn compute-cluster-update-options))
                                    ; Curator expects takeLeadership to block until voluntarily surrendering leadership.
                                    ; We need to block here until we're willing to give up leadership.
                                    ;
                                    ; We block until any of the cluster-connected-chans unblock. This happens when the compute cluster
                                    ; loses connectivity to the backend. For now, we want to treat mesos as special. When we lose our mesos
                                    ; driver connection to the backend, we want cook to suicide. If we lose any of our kubernetes connections
                                    ; we ignore it and work with the remaining cluster.
                                    ;
                                    ; WARNING: This code is very misleading. It looks like we'll suicide if ANY of the clusters lose leadership.
                                    ; However, the kubernetes compute clusters never put anything on their chan, so this is the equivalent of only looking at mesos.
                                    ; We didn't want to implement the special case for mesos.
                                    (let [res (async/<!! (async/merge (or (seq cluster-connected-chans) [(async/chan 1)])))]
                                      (when (instance? Throwable res)
                                        (throw res))))
                                  (catch Throwable e
                                    (log/error e "Lost leadership due to exception")
                                    (reset! normal-exit false))
                                  (finally
                                    (counters/dec! mesos-leader)
                                    (when @normal-exit
                                      (log/warn "Lost leadership naturally"))
                                    ;; Better to fail over and rely on start up code we trust then rely on rarely run code
                                    ;; to make sure we yield leadership correctly (and fully)
                                    (log/fatal "Lost leadership. Exiting. Expecting a supervisor to restart me!")
                                    (System/exit 0)))))
                            (stateChanged [_ client newState]
                              ;; We will give up our leadership whenever it seems that we lost
                              ;; ZK connection
                              (when (#{ConnectionState/LOST ConnectionState/SUSPENDED} newState)
                                (when @leadership-atom
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
    (log/info "Started the leader selector")
    {:submitter (partial submit-to-mesos mesos-datomic-conn)
     :leader-selector leader-selector}))

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
