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
            [cook.compute-cluster :as cc]
            [cook.config :as config]
            [cook.mesos.heartbeat :as heartbeat]
            [cook.mesos.sandbox :as sandbox]
            [cook.mesos.task :as task]
            [cook.plugins.definitions :as plugins]
            [cook.plugins.pool :as pool-plugin]
            [cook.progress :as progress]
            [cook.scheduler.scheduler :as sched]
            [datomic.api :as d]
            [mesomatic.scheduler :as mesos]
            [metrics.meters :as meters]
            [plumbing.core :as pc]))

(meters/defmeter [cook-mesos scheduler mesos-error])
(meters/defmeter [cook-mesos scheduler handle-framework-message-rate])
(meters/defmeter [cook-mesos scheduler handle-status-update-rate])


(defn create-mesos-scheduler
  "Creates the mesos scheduler which processes status updates asynchronously but in order of receipt."
  [gpu-enabled? conn heartbeat-ch pool->fenzo pool->offers-chan match-trigger-chan
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
            (sched/reconcile-tasks (d/db conn) driver pool->fenzo)
            (catch Exception e
              (log/error e "Reconciliation error")))))
      (reregistered
        [this driver master-info]
        (log/info "Reregistered with new master")
        (future
          (try
            (sched/reconcile-jobs conn)
            (sched/reconcile-tasks (d/db conn) driver pool->fenzo)
            (catch Exception e
              (log/error e "Reconciliation error")))))
      ;; Ignore this--we can just wait for new offers
      (offer-rescinded
        [this driver offer-id]
        (comment "TODO: Rescind the offer in fenzo"))
      (framework-message
        [this driver executor-id slave-id message]
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
        (log/error "Got a mesos error!!!!" message))
      (resource-offers
        [this driver raw-offers]
        (log/debug "Got offers:" raw-offers)
        (let [offers (map #(assoc % :compute-cluster compute-cluster) raw-offers)
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
                      (sched/receive-offers offers-chan match-trigger-chan driver pool-name offers))
                    (do
                      (log/warn "Declining" offer-count "offer(s) for non-existent pool" pool-name)
                      (sched/decline-offers-safe driver offers)))
                  (if-let [offers-chan (get pool->offers-chan "no-pool")]
                    (do
                      (log/info "Processing" offer-count "offer(s) for pool" pool-name "(not using pools)")
                      (sched/receive-offers offers-chan match-trigger-chan driver pool-name offers))
                    (do
                      (log/error "Declining" offer-count "offer(s) for pool" pool-name "(missing no-pool offer chan)")
                      (sched/decline-offers-safe driver offers))))))
            pool->offers)
          (log/debug "Finished receiving offers for all pools")))
      (status-update
        [this driver status]
        (meters/mark! handle-status-update-rate)
        (let [task-id (-> status :task-id :value)]
          (sched/async-in-order-processing
            task-id #(sched/handle-status-update conn compute-cluster pool->fenzo sync-agent-sandboxes-fn status)))))))


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
   (mesomatic.scheduler/scheduler-driver
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

(defrecord MesosComputeCluster [compute-cluster-name framework-id db-id driver-atom
                                sandbox-syncer-state exit-code-syncer-state mesos-heartbeat-chan
                                trigger-chans]
  cc/ComputeCluster
  (compute-cluster-name [this]
    compute-cluster-name)
  (get-mesos-driver-hack [this]
    @driver-atom)
  (launch-tasks [this offers task-metadata-seq]
    (mesos/launch-tasks! @driver-atom
                         (mapv :id offers)
                         (task/compile-mesos-messages framework-id offers task-metadata-seq)))
  (db-id [this]
    db-id)

  (current-leader? [this]
    (not (nil? @driver-atom)))

  (initialize-cluster [this pool->fenzo pool->offers-chan]
    (let [settings (:settings config/config)
          mesos-config (select-keys settings [:mesos-master
                                              :mesos-failover-timeout
                                              :mesos-principal
                                              :mesos-role
                                              :mesos-framework-name
                                              :gpu-enabled?])
          progress-config (:progress settings)
          conn cook.datomic/conn
          {:keys [match-trigger-chan progress-updater-trigger-chan]} trigger-chans
          {:keys [batch-size]} progress-config
          {:keys [progress-state-chan]} (progress/progress-update-transactor progress-updater-trigger-chan batch-size conn)
          progress-aggregator-chan (progress/progress-update-aggregator progress-config progress-state-chan)
          handle-progress-message (fn handle-progress-message-curried [progress-message-map]
                                    (progress/handle-progress-message! progress-aggregator-chan progress-message-map))
          handle-exit-code (fn handle-exit-code [task-id exit-code]
                             (sandbox/aggregate-exit-code exit-code-syncer-state task-id exit-code))
          scheduler (create-mesos-scheduler (:gpu-enabled? mesos-config)
                                            conn
                                            mesos-heartbeat-chan
                                            pool->fenzo
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
          (catch Exception e
            e)
          (finally
            (reset! driver-atom nil)))))))

; Internal method
(defn- mesos-cluster->compute-cluster-map-for-datomic
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
               :in $ ?cluster-name? ?mesos-id?
               :where
               [?c :compute-cluster/type :compute-cluster.type/mesos]
               [?c :compute-cluster/cluster-name ?cluster-name?]
               [?c :compute-cluster/mesos-framework-id ?framework-id?]]
             unfiltered-db compute-cluster-name framework-id)]
    (first query-result)))

; Internal method.
(defn get-mesos-compute-cluster
  "Process one mesos cluster specification, returning the entity id of the corresponding compute-cluster,
  creating the cluster if it does not exist. Warning: Not idempotent. Only call once "
  ([conn mesos-compute-cluster-factory mesos-cluster]
    (get-mesos-compute-cluster conn mesos-compute-cluster-factory mesos-cluster nil))
  ([conn mesos-compute-cluster-factory {:keys [compute-cluster-name framework-id] :as mesos-cluster} driver] ; driver argument for unit tests
   {:pre [compute-cluster-name
          framework-id]}
   (let [cluster-entity-id (get-mesos-cluster-entity-id (d/db conn) mesos-cluster)]
     (when-not cluster-entity-id
       (cc/write-compute-cluster conn (mesos-cluster->compute-cluster-map-for-datomic mesos-cluster)))
     (mesos-compute-cluster-factory compute-cluster-name
                                   framework-id
                                   (or cluster-entity-id (get-mesos-cluster-entity-id (d/db conn) mesos-cluster))
                                   (atom driver)))))

(defn- get-mesos-clusters-from-config
  "Get all of the mesos clusters defined in the configuration.
  In config.edn, we put all of the mesos keys under one toplevel dictionary.

  E.g.:

  {:failover-timeout-ms nil
   :framework-id #config/env \"COOK_FRAMEWORK_ID\"
   :master #config/env \"MESOS_MASTER\"
   ...
   }

  However, in config.clj, we split this up into lots of different keys at the toplevel:

  :mesos-master (fnk [[:config {mesos nil}]]
      ...)
  :mesos-framework-id (fnk [[:config {mesos ....

  This function undoes this shattering of the :mesos {...} into separate keys that
  occurs in config.clj. Long term, we need to fix config.clj to not to that, probably
  as part of global cook, at which time, this probably won't need to exist. Until then however....."
  [{:keys [mesos-compute-cluster-name mesos-framework-id]}]
  [{:compute-cluster-name mesos-compute-cluster-name :framework-id mesos-framework-id}])


(defn setup-compute-cluster-map-from-config
  "Setup the cluster-map configs, linking a cluster name to the associated metadata needed
  to represent/process it."
  [conn settings create-mesos-compute-cluster]
  (let [compute-clusters (->> (get-mesos-clusters-from-config settings)
                              (map (partial get-mesos-compute-cluster conn create-mesos-compute-cluster))
                              (map cc/register-compute-cluster!))]
    (doall compute-clusters)))


; A hack to store the mesos cluster, until we refactor the code so that we support multiple clusters. In the long term future
; this is probably replaced with a function from driver->cluster-id, or the cluster name is propagated by function arguments and
; closed over.
(defn mesos-cluster-hack
  "A hack to store the mesos cluster, until we refactor the code so that we support multiple clusters. In the
  long term future the cluster is propagated by function arguments and closed over."
  []
  {:post [%]} ; Never returns nil.
  (-> config/config
      :settings
      :mesos-compute-cluster-name
      cc/compute-cluster-name->ComputeCluster))


