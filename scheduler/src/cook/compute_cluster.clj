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
(ns cook.compute-cluster
  (:require [clojure.tools.logging :as log]
            [cook.config :as config]
            [cook.datomic]
            [datomic.api :as d]
            [clojure.core.async :as async]
            [cook.mesos.sandbox :as sandbox]
            [cook.plugins.pool :as pool-plugin]
            [mesomatic.scheduler :as mesos]
            [clojure.data.json :as json]
            [cook.mesos.heartbeat :as heartbeat]
            [cook.plugins.definitions :as plugins]
            [metrics.meters :as meters]
            [plumbing.core :as pc]
            [metrics.histograms :as histograms]
            [metrics.counters :as counters]))

(defprotocol ComputeCluster
  ; These methods should accept bulk data and process in batches.
  ;(kill-tasks [this task]
  (launch-tasks [this offers task-metadata-seq])
  (compute-cluster-name [this])

  (db-id [this]
    "Get a database entity-id for this compute cluster (used for putting it into a task structure).")

  (initialize-cluster [this pool->fenzo pool->offers-chan])

  (get-mesos-driver-hack [this]
    "Get the mesos driver. Hack; any funciton invoking this should be put within the compute-cluster implementation")
  (set-mesos-driver-atom-hack! [this driver]
    "Hack to overwrite the driver. Used until we fix the initialization order of compute-cluster"))

(meters/defmeter [cook-mesos scheduler scheduler-offer-declined])

(defn decline-offers
  "declines a collection of offer ids"
  [driver offer-ids]
  (log/debug "Declining offers:" offer-ids)
  (doseq [id offer-ids]
    (meters/mark! scheduler-offer-declined)
    (mesos/decline-offer driver id)))

(defn decline-offers-safe
  "Declines a collection of offers, catching exceptions"
  [driver offers]
  (try
    (decline-offers driver (map :id offers))
    (catch Exception e
      (log/error e "Unable to decline offers!"))))

(defn metric-title
  [metric-name pool]
  ["cook-mesos" "scheduler" metric-name (str "pool-" pool)])

(counters/defcounter [cook-mesos scheduler offer-chan-depth])
(meters/defmeter [cook-mesos scheduler mesos-error])
(meters/defmeter [cook-mesos scheduler offer-chan-full-error])

(defn receive-offers
  [offers-chan match-trigger-chan driver pool-name offers]
  (doseq [offer offers]
    (histograms/update! (histograms/histogram (metric-title "offer-size-cpus" pool-name)) (get-in offer [:resources :cpus] 0))
    (histograms/update! (histograms/histogram (metric-title "offer-size-mem" pool-name)) (get-in offer [:resources :mem] 0)))
  (if (async/offer! offers-chan offers)
    (do
      (counters/inc! offer-chan-depth)
      (async/offer! match-trigger-chan :trigger)) ; :trigger is arbitrary, the value is ignored
    (do (log/warn "Offer chan is full. Are we not handling offers fast enough?")
        (meters/mark! offer-chan-full-error)
        (future
          (decline-offers-safe driver offers)))))

(meters/defmeter [cook-mesos scheduler handle-framework-message-rate])

(defn create-mesos-scheduler
  "Creates the mesos scheduler which processes status updates asynchronously but in order of receipt."
  [gpu-enabled? conn heartbeat-ch pool->fenzo pool->offers-chan match-trigger-chan sandbox-syncer-state compute-cluster
   reconcile-jobs-fn reconcile-tasks-fn handle-framework-message-fn handle-status-update-fn]
  (let [configured-framework-id (cook.config/framework-id-config)]
    (mesos/scheduler
      (registered
        [this driver framework-id master-info]
        (log/info "Registered with mesos with framework-id " framework-id)
        (let [value (-> framework-id mesomatic.types/pb->data :value)]
          (when (not= configured-framework-id value)
            (let [message (str "The framework-id provided by Mesos (" value ") "
                               "does not match the one Cook is configured with (" configured-framework-id ")")]
              (log/error message)
              (throw (ex-info message {:framework-id-mesos value :framework-id-cook configured-framework-id})))))
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
            (reconcile-jobs-fn conn)
            (reconcile-tasks-fn (d/db conn) driver pool->fenzo)
            (catch Exception e
              (log/error e "Reconciliation error")))))
      (reregistered
        [this driver master-info]
        (log/info "Reregistered with new master")
        (future
          (try
            (reconcile-jobs-fn conn)
            (reconcile-tasks-fn (d/db conn) driver pool->fenzo)
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
              (handle-framework-message-fn parsed-message)))
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
                      (receive-offers offers-chan match-trigger-chan driver pool-name offers))
                    (do
                      (log/warn "Declining" offer-count "offer(s) for non-existent pool" pool-name)
                      (decline-offers-safe driver offers)))
                  (if-let [offers-chan (get pool->offers-chan "no-pool")]
                    (do
                      (log/info "Processing" offer-count "offer(s) for pool" pool-name "(not using pools)")
                      (receive-offers offers-chan match-trigger-chan driver pool-name offers))
                    (do
                      (log/error "Declining" offer-count "offer(s) for pool" pool-name "(missing no-pool offer chan)")
                      (decline-offers-safe driver offers))))))
            pool->offers)
          (log/debug "Finished receiving offers for all pools")))
      (status-update
        [this driver status]
        (handle-status-update-fn status)))))

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

(defrecord MesosComputeCluster [compute-cluster-name framework-id db-id driver-atom
                                sandbox-syncer-state reconcile-jobs-fn reconcile-tasks-fn handle-framework-message-fn
                                handle-status-update-fn mesos-heartbeat-chan]
  ComputeCluster
  (compute-cluster-name [this]
    compute-cluster-name)
  (get-mesos-driver-hack [this]
    @driver-atom)
  (db-id [this]
    db-id)
  (get-mesos-framework-id-hack [this]
    framework-id)
  (set-mesos-driver-atom-hack! [this driver]
    (reset! driver-atom driver))
  (initialize-cluster [this pool->fenzo pool->offers-chan]
    (let [mesos-config (select-keys config/config [:mesos-master
                                                   :mesos-failover-timeout
                                                   :mesos-principal
                                                   :mesos-role
                                                   :mesos-framework-name
                                                   :gpu-enabled?])
          trigger-chans {}
          {:keys [match-trigger-chan]} trigger-chans
          scheduler (create-mesos-scheduler (:gpu-enabled? mesos-config)
                                            cook.datomic/conn
                                            mesos-heartbeat-chan
                                            pool->fenzo
                                            pool->offers-chan
                                            match-trigger-chan
                                            sandbox-syncer-state
                                            this
                                            reconcile-jobs-fn
                                            reconcile-tasks-fn
                                            handle-framework-message-fn
                                            handle-status-update-fn)
          framework-id (config/framework-id-config)
          _ (log/info "Initializing mesos driver with config: " mesos-config)
          driver (make-mesos-driver mesos-config scheduler framework-id)]
      (mesomatic.scheduler/start! driver)
      (set-mesos-driver-atom-hack! this driver))))

; Internal method
(defn write-compute-cluster
  "Create a missing compute-cluster for one that's not yet in the database."
  [conn compute-cluster]
  (log/info "Installing a new compute cluster in datomic for " compute-cluster)
  @(d/transact
     conn
     [(assoc compute-cluster :db/id (d/tempid :db.part/user))]))

; Internal variable
(def cluster-name->compute-cluster-atom (atom {}))

(defn register-compute-cluster!
  "Register a compute cluster "
  [compute-cluster]
  (let [compute-cluster-name (compute-cluster-name compute-cluster)]
    (when (contains? @cluster-name->compute-cluster-atom compute-cluster-name)
      (throw (IllegalArgumentException.
               (str "Multiple compute-clusters have the same name: " compute-cluster
                    " and " (get @cluster-name->compute-cluster-atom compute-cluster-name)
                    " with name " compute-cluster-name))))
    (log/info "Setting up compute cluster: " compute-cluster)
    (swap! cluster-name->compute-cluster-atom assoc compute-cluster-name compute-cluster)
    nil))

(defn compute-cluster-name->ComputeCluster
  "From the name of a compute cluster, return the object. Throws if not found. Does not return nil."
  [compute-cluster-name]
  (let [result (get @cluster-name->compute-cluster-atom compute-cluster-name)]
    (when-not result
      (log/error "Was asked to lookup db-id for" compute-cluster-name "and got nil"))
    result))

(defn get-default-cluster-for-legacy
  "What cluster name to put on for legacy jobs when generating their compute-cluster.
  TODO: Will want this to be configurable when we support multiple mesos clusters."
  []
  {:post [%]} ; Never returns nil.
  (-> config/config
      :settings
      :mesos-compute-cluster-name
      compute-cluster-name->ComputeCluster))

