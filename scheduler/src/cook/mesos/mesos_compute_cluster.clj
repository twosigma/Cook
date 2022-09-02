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
(ns cook.mesos.mesos-compute-cluster
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [cook.cached-queries :as cached-queries]
            [cook.compute-cluster :as cc]
            [cook.compute-cluster.metrics :as ccmetrics]
            [cook.config :as config]
            [cook.mesos.heartbeat :as heartbeat]
            [cook.mesos.sandbox :as sandbox]
            [cook.mesos.task :as task]
            [cook.plugins.definitions :as plugins]
            [cook.plugins.pool :as pool-plugin]
            [cook.pool :as pool]
            [cook.progress :as progress]
            [cook.prometheus-metrics :as prom]
            [cook.scheduler.scheduler :as sched]
            [cook.tools :as tools]
            [datomic.api :as d]
            [mesomatic.scheduler :as mesos]
            [metrics.counters :as counters]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [plumbing.core :as pc])
  (:import (java.net URLEncoder)
           (java.util.concurrent.locks ReentrantReadWriteLock)
           (org.apache.mesos Protos$TaskStatus$Reason)))

(meters/defmeter [cook-mesos scheduler mesos-error])
(meters/defmeter [cook-mesos scheduler handle-framework-message-rate])
(meters/defmeter [cook-mesos scheduler handle-status-update-rate])
(counters/defcounter [cook-mesos scheduler offer-chan-depth])

(def offer-chan-size 100)

(defn conditionally-sync-sandbox
  "For non cook executor tasks, call sync-agent-sandboxes-fn"
  [conn task-id task-state sync-agent-sandboxes-fn]
  (let [instance-ent (d/entity (d/db conn) [:instance/task-id task-id])]
    (when (and (#{:task-starting :task-running} task-state)
               (not= :executor/cook (:instance/executor instance-ent)))
      ;; cook executor tasks should automatically get sandbox directory updates
      (sync-agent-sandboxes-fn (:instance/hostname instance-ent) task-id))))

(defn handle-status-update
  "Handles a status update from mesos. When a task/job is in an inconsistent state it may kill the task. It also writes the
  status back to datomic."
  [conn compute-cluster sync-agent-sandboxes-fn pool-name->fenzo-state {:keys [reason state] :as status}]
  (let [task-id (-> status :task-id :value)
        instance (d/entity (d/db conn) [:instance/task-id task-id])
        prior-job-state (:job/state (:job/_instance instance))
        prior-instance-status (:instance/status instance)
        pool-name (cached-queries/job->pool-name (:job/_instance instance))]
    (if (and
            (or (nil? instance) ; We could know nothing about the task, meaning a DB error happened and it's a waste to finish
                (= prior-job-state :job.state/completed) ; The task is attached to a failed job, possibly due to instances running on multiple hosts
                (= prior-instance-status :instance.status/failed)) ; The kill-task message could've been glitched on the network
            (contains? #{:task-running
                         :task-staging
                         :task-starting}
                       state)) ; killing an unknown task causes a TASK_LOST message. Break the cycle! Only kill non-terminal tasks
      (do
        (log/info "In compute cluster" (cc/compute-cluster-name compute-cluster)
                  ", attempting to kill task" task-id "should've been put down already"
                  {:instance instance
                   :pool pool-name
                   :prior-instance-status prior-instance-status
                   :prior-job-state prior-job-state
                   :state state})
        (prom/inc prom/mesos-tasks-killed-in-status-update)
        (meters/mark! (meters/meter (sched/metric-title "tasks-killed-in-status-update" pool-name)))
        (cc/safe-kill-task compute-cluster task-id))
      ; Mesomatic doesn't have a mapping for REASON_TASK_KILLED_DURING_LAUNCH
      ; (http://mesos.apache.org/documentation/latest/task-state-reasons/#for-state-task_killed),
      ; so we're rolling our own mapping for it here. There is an open issue with Mesomatic:
      ; https://github.com/clojusc/mesomatic/issues/53
      (let [status' (cond-> status
                      (= reason Protos$TaskStatus$Reason/REASON_TASK_KILLED_DURING_LAUNCH)
                      (assoc :reason :reason-killed-during-launch))]
        (sched/write-status-to-datomic conn pool-name->fenzo-state status')))
    (conditionally-sync-sandbox conn task-id (:state status) sync-agent-sandboxes-fn)))

(defn create-mesos-scheduler
  "Creates the mesos scheduler which processes status updates asynchronously but in order of receipt."
  [gpu-enabled? conn heartbeat-ch pool-name->fenzo-state pool->offers-chan match-trigger-chan
   handle-exit-code handle-progress-message sandbox-syncer-state framework-id compute-cluster]
  (let [sync-agent-sandboxes-fn #(sandbox/sync-agent-sandboxes sandbox-syncer-state framework-id %1 %2)
        message-handlers {:handle-exit-code handle-exit-code
                          :handle-progress-message handle-progress-message}]
    (mesos/scheduler
      (registered
        [this driver mesos-framework-id master-info]
        (log/info "Registered with mesos with framework-id " mesos-framework-id)
        (let [value (-> mesos-framework-id mesomatic.types/pb->data :value)]
          (when (not= framework-id value)
            (let [message (str "The framework-id provided by Mesos (" value ") "
                               "does not match the one Cook is configured with (" framework-id ")")]
              (log/error message)
              (throw (ex-info message {:framework-id-mesos value :framework-id-cook framework-id})))))
        (when (and gpu-enabled? (not (re-matches #"1\.\d+\.\d+" (:version master-info))))
          (binding [*out* *err*]
            (println "Cannot enable GPU support on pre-mesos 1.0. The version we found was " (:version master-info)))
          (log/error "Cannot enable GPU support on pre-mesos 1.0. The version we found was " (:version master-info))
          (Thread/sleep 1000)
          (System/exit 1))
        ;; Use future because the thread that runs mesos/scheduler doesn't load classes correctly. for reasons.
        ;; As Sophie says, you want to future proof your code.
        (future
          (try
            (sched/reconcile-jobs conn)
            (sched/reconcile-tasks (d/db conn) compute-cluster driver pool-name->fenzo-state)
            (catch Exception e
              (log/error e "Reconciliation error")))))
      (reregistered
        [this driver master-info]
        (log/info "Reregistered with new master")
        (future
          (try
            (sched/reconcile-jobs conn)
            (sched/reconcile-tasks (d/db conn) compute-cluster driver pool-name->fenzo-state)
            (catch Exception e
              (log/error e "Reconciliation error")))))
      ;; Ignore this--we can just wait for new offers
      (offer-rescinded
        [this driver offer-id]
        (comment "TODO: Rescind the offer in fenzo"))
      (framework-message
        [this driver executor-id slave-id message]
        (prom/inc prom/mesos-handle-framework-message)
        (meters/mark! handle-framework-message-rate)
        (try
          (let [{:strs [task-id type] :as parsed-message} (json/read-str (String. ^bytes message "UTF-8"))]
            (case type
              "directory" (sandbox/update-sandbox sandbox-syncer-state parsed-message)
              "heartbeat" (heartbeat/notify-heartbeat heartbeat-ch executor-id slave-id parsed-message)
              (sched/async-in-order-processing
                task-id #(sched/handle-framework-message conn message-handlers parsed-message))))
          (catch Exception e
            (log/error e "Unable to process framework message"
                       {:executor-id executor-id, :message message, :slave-id slave-id}))))
      (disconnected
        [this driver]
        (log/error "Disconnected from the previous master"))
      ;; We don't care about losing slaves or executors--only tasks
      (slave-lost [this driver slave-id])
      (executor-lost [this driver executor-id slave-id status])
      (error
        [this driver message]
        (meters/mark! mesos-error)
        (prom/inc prom/mesos-error)
        (log/error "Got a mesos error!!!!" message))
      (resource-offers
        [this driver raw-offers]
        (log/debug "Got offers:" raw-offers)
        (let [offers (map #(assoc % :compute-cluster compute-cluster
                                    :offer-match-timer (timers/start (ccmetrics/timer "offer-match-timer" (cc/compute-cluster-name compute-cluster)))
                                    :offer-match-timer-prom-stop-fn (prom/start-timer prom/offer-match-timer {:compute-cluster compute-cluster}))
                          raw-offers)
              pool->offers (group-by (fn [o] (plugins/select-pool pool-plugin/plugin o)) offers)
              using-pools? (config/default-pool)]
          (log/info "Offers by pool:" (pc/map-vals count pool->offers))
          (run!
            (fn [[pool-name offers]]
              (let [offer-count (count offers)]
                (if using-pools?
                  (if-let [offers-chan (get pool->offers-chan pool-name)]
                    (do
                      (log/info "Processing" offer-count "offer(s) for known pool" pool-name)
                      (sched/receive-offers offers-chan match-trigger-chan compute-cluster pool-name offers))
                    (do
                      (log/warn "Declining" offer-count "offer(s) for non-existent pool" pool-name)
                      (sched/decline-offers-safe compute-cluster offers)))
                  (if-let [offers-chan (get pool->offers-chan "no-pool")]
                    (do
                      (log/info "Processing" offer-count "offer(s) for pool" pool-name "(not using pools)")
                      (sched/receive-offers offers-chan match-trigger-chan compute-cluster pool-name offers))
                    (do
                      (log/error "Declining" offer-count "offer(s) for pool" pool-name "(missing no-pool offer chan)")
                      (sched/decline-offers-safe compute-cluster offers))))))
            pool->offers)
          (log/debug "Finished receiving offers for all pools")))
      (status-update
        [this driver status]
        (prom/inc prom/mesos-handle-status-update)
        (meters/mark! handle-status-update-rate)
        (let [task-id (-> status :task-id :value)]
          (sched/async-in-order-processing
            task-id (fn [] (handle-status-update conn compute-cluster sync-agent-sandboxes-fn pool-name->fenzo-state status))))))))


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
   (let [mesos-config (cond-> {:checkpoint true
                               :name (str mesos-framework-name "-" @cook.util/version "-" @cook.util/commit)
                               :user ""}
                              framework-id (assoc :id {:value framework-id})
                              gpu-enabled? (assoc :capabilities [{:type :framework-capability-gpu-resources}])
                              mesos-failover-timeout (assoc :failover-timeout mesos-failover-timeout)
                              mesos-principal (assoc :principal mesos-principal)
                              mesos-role (assoc :role mesos-role))]
     (if mesos-principal
       (mesomatic.scheduler/scheduler-driver scheduler mesos-config mesos-master {:principal mesos-principal})
       (mesomatic.scheduler/scheduler-driver scheduler mesos-config mesos-master)))))

(defrecord MesosComputeCluster [compute-cluster-name framework-id db-id driver-atom
                                sandbox-syncer-state exit-code-syncer-state mesos-heartbeat-chan
                                progress-update-chans trigger-chans mesos-config pool->offers-chan container-defaults
                                compute-cluster-launch-rate-limiter kill-lock-object]
  cc/ComputeCluster
  (compute-cluster-name [this]
    compute-cluster-name)

  (launch-tasks [_ pool-name matches process-task-post-launch-fn]
    (doseq [{:keys [leases task-metadata-seq]} matches
            :let [offers (mapv :offer leases)]]
      (log/info "In" pool-name "pool, launching" (count offers)
                "offers for" compute-cluster-name "compute cluster")
      (mesos/launch-tasks! @driver-atom
                           (mapv :id offers)
                           (task/compile-mesos-messages framework-id offers task-metadata-seq))
      (run! process-task-post-launch-fn task-metadata-seq)))

  (kill-task [this task-id]
    (mesos/kill-task! @driver-atom {:value task-id}))

  (decline-offers [this offer-ids]
    (doseq [id offer-ids]
      (mesos/decline-offer @driver-atom id)))

  (db-id [this]
    db-id)

  (initialize-cluster [this pool-name->fenzo-state]
    (log/info "Initializing Mesos compute cluster" compute-cluster-name)
    (let [conn cook.datomic/conn
          {:keys [match-trigger-chan]} trigger-chans
          {:keys [progress-aggregator-chan]} progress-update-chans
          handle-progress-message (fn handle-progress-message-curried [db task-id progress-message-map]
                                    (progress/handle-progress-message!
                                      db task-id progress-aggregator-chan progress-message-map))
          handle-exit-code (fn handle-exit-code [task-id exit-code]
                             (sandbox/aggregate-exit-code exit-code-syncer-state task-id exit-code))
          scheduler (create-mesos-scheduler (:gpu-enabled? mesos-config)
                                            conn
                                            mesos-heartbeat-chan
                                            pool-name->fenzo-state
                                            pool->offers-chan
                                            match-trigger-chan
                                            handle-exit-code
                                            handle-progress-message
                                            sandbox-syncer-state
                                            framework-id
                                            this)
          _ (log/info "Initializing mesos driver with config: " mesos-config)
          driver (make-mesos-driver mesos-config scheduler framework-id)]
      (mesomatic.scheduler/start! driver)
      (reset! driver-atom driver)
      (async/thread
        (try
          ; scheduler/join! is a blocking call which returns or throws when the driver loses it's connection to mesos.
          ; Run this in an async thread which will deliver either the Status on a normal exit, or an exception if thrown.
          (mesomatic.scheduler/join! driver)
          (log/info "In" compute-cluster-name "compute cluster, mesomatic scheduler join unblocked; we appear to have lost driver connection")
          (catch Exception e
            (log/warn e "In" compute-cluster-name "compute cluster, lost mesos driver with exception")
            e)
          (finally
            (reset! driver-atom nil))))))

  (pending-offers [this pool-name]
    (log/info "In" compute-cluster-name "compute cluster, looking for offers for pool" pool-name)
    (->> (tools/read-chan (pool->offers-chan pool-name) offer-chan-size)
         ((fn decrement-offer-chan-depth [offer-lists]
            (counters/dec! offer-chan-depth (count offer-lists))
            (prom/dec prom/mesos-offer-chan-depth {:pool pool-name} (count offer-lists))
            offer-lists))
         (reduce into [])))

  (restore-offers [this pool-name offers]
    (async/go
      (async/>! (pool->offers-chan pool-name) offers)))

  (set-synthetic-pods-counters [_ _ ])

  (autoscaling? [_ _] false)

  (max-launchable [_ _])

  (autoscale! [_ _ _ _])

  (use-cook-executor? [_] true)

  (container-defaults [_]
    container-defaults)

  (max-tasks-per-host [_])

  (num-tasks-on-host [_ _])

  (retrieve-sandbox-url-path
    ;; Constructs a URL to query the sandbox directory of the task.
    ;; Uses the sandbox-directory to determine the sandbox directory.
    ;; Hard codes fun stuff like the port we run the agent on.
    ;; Users will need to add the file path & offset to their query.
    ;; Refer to the 'Using the output_url' section in docs/scheduler-rest-api.adoc for further details.
    [_ {:keys [instance/hostname instance/task-id instance/sandbox-directory]}]
    (try
      (when sandbox-directory
        (str "http://" hostname ":5051" "/files/read.json?path="
             (URLEncoder/encode sandbox-directory "UTF-8")))
      (catch Exception e
        (log/debug e "Unable to retrieve directory path for" task-id "on agent" hostname)
        nil)))

  (launch-rate-limiter [_] compute-cluster-launch-rate-limiter)

  (kill-lock-object [_] kill-lock-object))

; Internal method
(defn mesos-cluster->compute-cluster-map-for-datomic
  "Given a mesos cluster dictionary, determine the datomic entity it should correspond to."
  [{:keys [compute-cluster-name framework-id]}]
  {:compute-cluster/type :compute-cluster.type/mesos
   :compute-cluster/cluster-name compute-cluster-name
   :compute-cluster/mesos-framework-id framework-id})

; Internal method
(defn get-mesos-cluster-entity-id
  "Given a configuration map for a mesos cluster, return the datomic entity-id corresponding to the cluster,
  if it exists. Internal helper function."
  [unfiltered-db {:keys [compute-cluster-name framework-id]}]
  {:pre [compute-cluster-name
         framework-id]}
  (let [query-result
        (d/q '[:find [?c]
               :in $ ?cluster-name? ?framework-id?
               :where
               [?c :compute-cluster/type :compute-cluster.type/mesos]
               [?c :compute-cluster/cluster-name ?cluster-name?]
               [?c :compute-cluster/mesos-framework-id ?framework-id?]]
             unfiltered-db compute-cluster-name framework-id)]
    (first query-result)))

(defn get-or-create-cluster-entity-id
  "Checks datomic for a compute cluster with the given name and framework-id. If present, returns the entity id.
   If missing, installs in datomic and returns the entity id."
  [conn compute-cluster-name framework-id]
  (let [compute-cluster-entity-id (get-mesos-cluster-entity-id (d/db conn)
                                                               {:compute-cluster-name compute-cluster-name
                                                                :framework-id framework-id})]
    (if compute-cluster-entity-id
      compute-cluster-entity-id
      (cc/write-compute-cluster conn (mesos-cluster->compute-cluster-map-for-datomic
                                       {:compute-cluster-name compute-cluster-name
                                        :framework-id framework-id})))))

(defn factory-fn
  "Constructs a new MesosComputeCluster and registers it."
  [{:keys [compute-cluster-name
           framework-id
           master
           failover-timeout
           principal
           role
           framework-name
           gpu-enabled?
           container-defaults
           compute-cluster-launch-rate-limits]}
   {:keys [exit-code-syncer-state
           mesos-agent-query-cache
           mesos-heartbeat-chan
           sandbox-syncer-config
           progress-update-chans
           trigger-chans]}]
  (try
    (let [conn cook.datomic/conn
          cluster-entity-id (get-or-create-cluster-entity-id conn compute-cluster-name framework-id)
          {:keys [max-consecutive-sync-failure
                  publish-batch-size
                  publish-interval-ms
                  sync-interval-ms]} sandbox-syncer-config
          sandbox-syncer-state (sandbox/prepare-sandbox-publisher framework-id conn publish-batch-size
                                                                  publish-interval-ms sync-interval-ms
                                                                  max-consecutive-sync-failure mesos-agent-query-cache)
          mesos-config {:mesos-master master
                        :mesos-failover-timeout failover-timeout
                        :mesos-principal principal
                        :mesos-role (or role "*")
                        :mesos-framework-name (or framework-name "Cook")
                        :gpu-enabled? gpu-enabled?}
          db-pools (pool/all-pools (d/db conn))
          synthesized-pools (if (-> db-pools count pos?)
                              db-pools
                              [{:pool/name "no-pool"}])
          pool->offer-chan (pc/map-from-keys (fn [_]
                                               (async/chan offer-chan-size))
                                             (map :pool/name synthesized-pools))
          compute-cluster-launch-rate-limiter (cook.rate-limit/create-compute-cluster-launch-rate-limiter
                                                compute-cluster-name compute-cluster-launch-rate-limits)
          mesos-compute-cluster (->MesosComputeCluster compute-cluster-name
                                                       framework-id
                                                       cluster-entity-id
                                                       (atom nil)
                                                       sandbox-syncer-state
                                                       exit-code-syncer-state
                                                       mesos-heartbeat-chan
                                                       progress-update-chans
                                                       trigger-chans
                                                       mesos-config
                                                       pool->offer-chan
                                                       container-defaults
                                                       compute-cluster-launch-rate-limiter
                                                       ; cluster-level kill-lock. See cc/kill-lock-object
                                                       (ReentrantReadWriteLock. true))]
      (log/info "Registering compute cluster" mesos-compute-cluster)
      (cc/register-compute-cluster! mesos-compute-cluster)
      mesos-compute-cluster)
    (catch Throwable t
      (log/error t "Failed to construct mesos compute cluster")
      (throw t))))
