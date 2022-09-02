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
  (:require [clojure.data :as data]
            [clojure.tools.logging :as log]
            [cook.config :as config]
            [cook.datomic :as datomic]
            [cook.prometheus-metrics :as prom]
            [cook.util :as util]
            [datomic.api :as d]
            [metrics.timers :as timers]
            [plumbing.core :refer [map-from-keys map-from-vals map-vals]]))

(defprotocol ComputeCluster
  (launch-tasks [this pool-name matches process-task-post-launch-fn]
    "Launches the tasks contained in the given matches collection")

  (compute-cluster-name [this]
    "Returns the name of this compute cluster")

  (db-id [this]
    "Get a database entity-id for this compute cluster (used for putting it into a task structure).")

  (initialize-cluster [this pool-name->fenzo-state]
    "Initializes the cluster. Returns a channel that will be delivered on when the cluster loses leadership.
     We expect Cook to give up leadership when a compute cluster loses leadership, so leadership is not expected to be regained.
     The channel result will be an exception if an error occurred, or a status message if leadership was lost normally.")

  (kill-task [this task-id]
    "Kill the task with the given task id")

  (decline-offers [this offer-ids]
    "Decline the given offer ids")

  (pending-offers [this pool-name]
    "Retrieve pending offers for the given pool")

  (restore-offers [this pool-name offers]
    "Called when offers are not processed to ensure they're still available.")

  (set-synthetic-pods-counters [this pool-name]
    "Sets the counter metrics for number of and max synthetic pods.")

  (autoscaling? [this pool-name]
    "Returns true if this compute cluster should autoscale the provided pool to satisfy pending jobs")
  
  (max-launchable [this pool-name]
    "Returns the maximum number of launchable jobs in the pool by considering current scheduling pods.")

  (autoscale! [this pool-name jobs adjust-job-resources-for-pool-fn]
    "Autoscales the provided pool to satisfy the provided pending jobs")

  (use-cook-executor? [this]
    "Returns true if this compute cluster makes use of the Cook executor for running tasks")

  (container-defaults [this]
    "Default values to use for containers launched in this compute cluster")

  (max-tasks-per-host [this]
    "The maximum number of tasks that a given host should run at the same time")

  (num-tasks-on-host [this hostname]
    "The number of tasks currently running on the given hostname")

  (retrieve-sandbox-url-path [this instance-entity]
    "Constructs a URL to query the sandbox directory of the task.
     Users will need to add the file path & offset to their query.
     Refer to the 'Using the output_url' section in docs/scheduler-rest-api.adoc for further details.")

  (launch-rate-limiter [this]
    "Return the RateLimiter that should be used to limit launches to this compute cluster")

  ; There's an ugly race where the core cook scheduler can kill a job before it tries to launch it.
  ; What happens is:
  ;   1. In launch-matched-tasks, we write instance objects to datomic for everything that matches,
  ;      we have not submitted these to the compute cluster backends yet.
  ;   2. A kill command arrives to kill the job. The job is put into completed.
  ;   3. The monitor-tx-queue happens to notice the job just completed. It sees the instance written in step 1.
  ;   4. We submit a kill-task to the compute cluster backend.
  ;   5. Kill task processes. There's not much to do, as there's no task to kill.
  ;   6. launch-matched-tasks now visits the task and submits it to the compute cluster backend.
  ;   7. Task executes and is not killed.
  ;
  ; At the core the bug is an atomicity bug. The intermediate state of written-to-datomic but not yet sent (via launch-task)
  ; to the backend. We work around this race by having a lock around of all launch-matched-tasks that contains the database
  ; update and the submit to kubernetes. We re-use the same lock to wrap kill-task to force an ordering relationship, so
  ; that kill-task must happen after the write-to-datomic and launch-task have been invoked.
  ;
  ; ComputeCluster/kill-task cannot be invoked before we write the task to datomic. If it is invoked after the write to
  ; datomic, the lock ensures that it won't be acted upon until after launch-task has been invoked on the compute cluster.
  ;
  ; So, we must grab this lock before calling kill-task in the compute cluster API. As all of our invocations to it are via
  ; safe-kill-task, we add the lock there.
  ;
  ; We use a ReaderWriterLock where launch tasks grab the readers, so multiple pools can launch at the same time, and a writer lock
  ; here so that kills are blocked until nobody is in the middle of launching. Note that we don't actually need to hold this lock while
  ; we kill, just to delay the kill until after any task-id written in the database is also guaranteed to be into the backend.
  (kill-lock-object [this]
    "Returns the cluster's read-write lock used for ordering launch and kill operations"))

(def kill-lock-timer-for-kill (timers/timer ["cook-mesos" "scheduler" "kill-lock-acquire-for-kill"]))

(defn safe-kill-task
  "A safe version of kill task that never throws. This reduces the risk that errors in one compute cluster propagate and cause problems in another compute cluster."
  [{:keys [name kill-lock-object] :as compute-cluster} task-id]
  ; Yes, this is an empty lock body. It is to create an ordering relationship so that if we're in the process of writing to datomic and launching,
  ; we defer killing anything until we've told mesos/k8s about the pod. See further explanation at kill-lock-object.
  (let [prom-timer-stop-fn (prom/start-timer prom/acquire-kill-lock-for-kill-duration)
        kill-lock-timer-context (timers/start kill-lock-timer-for-kill)]
    (.. kill-lock-object writeLock lock)
    (.. kill-lock-object writeLock unlock)
    (prom-timer-stop-fn)
    (.stop kill-lock-timer-context))
  (try
    (kill-task compute-cluster task-id)
    (catch Throwable t
      (log/error t "In compute cluster" name ", error killing task" task-id))))

(defn kill-task-if-possible
  "If compute cluster is nil, print a warning instead of killing the task. There are cases, in particular,
  lingering tasks, stragglers, or cancelled tasks where the task might outlive the compute cluster it was
  member of. When this occurs, the looked up compute cluster is null and trying to kill via it would cause an NPE,
  when in reality, it's relatively innocuous. So, we have this wrapper to use in those circumstances."
  [compute-cluster task-id]
  (if compute-cluster
    (safe-kill-task compute-cluster task-id)
    (log/warn "Unable to kill task" task-id "because compute-cluster is nil")))

; Internal method
(defn write-compute-cluster
  "Create a missing compute-cluster for one that's not yet in the database."
  [conn compute-cluster]
  (log/info "Installing a new compute cluster in datomic for " compute-cluster)
  (let [tempid (d/tempid :db.part/user)
        result @(d/transact
                  conn
                  [(assoc compute-cluster :db/id tempid)])]
    (d/resolve-tempid (d/db conn) (:tempids result) tempid)))

; Internal variable
(def cluster-name->compute-cluster-atom (atom {}))

(defn register-compute-cluster!
  "Register a compute cluster "
  [compute-cluster]
  (let [compute-cluster-name (compute-cluster-name compute-cluster)]
    (when (contains? @cluster-name->compute-cluster-atom compute-cluster-name)
      (throw (IllegalArgumentException.
               (str "Multiple compute-clusters have the same name: " compute-cluster
                    " and " (@cluster-name->compute-cluster-atom compute-cluster-name)
                    " with name " compute-cluster-name))))
    (log/info "Setting up compute cluster: " compute-cluster)
    (swap! cluster-name->compute-cluster-atom assoc compute-cluster-name compute-cluster)
    nil))

(defn compute-cluster-name->ComputeCluster
  "From the name of a compute cluster, return the object. May return nil if not found."
  [compute-cluster-name]
  (let [result (@cluster-name->compute-cluster-atom compute-cluster-name)]
    (when-not result
      (log/error (ex-info (str "Was asked to lookup db-id for " compute-cluster-name " and got nil") {})))
    result))

(defn get-default-cluster-for-legacy
  "What cluster name to put on for legacy jobs when generating their compute-cluster.
  TODO: Will want this to be configurable when we support multiple mesos clusters."
  []
  {:post [%]} ; Never returns nil.
  (let [first-cluster-name (->> config/config
                                :settings
                                :compute-clusters
                                (map (fn [{:keys [config]}] (:compute-cluster-name config)))
                                first)]
    (compute-cluster-name->ComputeCluster first-cluster-name)))

(defn compute-cluster-config-ent->compute-cluster-config
  "Convert Datomic dynamic cluster configuration entity to an object"
  [{:keys [compute-cluster-config/name
           compute-cluster-config/template
           compute-cluster-config/base-path
           compute-cluster-config/ca-cert
           compute-cluster-config/state
           compute-cluster-config/state-locked?
           compute-cluster-config/location
           compute-cluster-config/features]}]
  {:name name
   :template template
   :base-path base-path
   :ca-cert ca-cert
   :state (case state
            :compute-cluster-config.state/running :running
            :compute-cluster-config.state/draining :draining
            :compute-cluster-config.state/deleted :deleted)
   :state-locked? state-locked?
   :location location
   :features (map (fn [{:keys [compute-cluster-config.feature/key
                               compute-cluster-config.feature/value]}]
                    {:key key :value value})
                  features)})

(defn compute-cluster-config->compute-cluster-config-ent
  "Convert dynamic cluster configuration to a Datomic entity"
  [{:keys [name template base-path ca-cert state state-locked? location features]}]
  (cond->
    {:compute-cluster-config/name name
     :compute-cluster-config/template template
     :compute-cluster-config/base-path base-path
     :compute-cluster-config/ca-cert ca-cert
     :compute-cluster-config/state (case state
                                     :running :compute-cluster-config.state/running
                                     :draining :compute-cluster-config.state/draining
                                     :deleted :compute-cluster-config.state/deleted)
     :compute-cluster-config/state-locked? state-locked?}
    location (assoc :compute-cluster-config/location location)
    features (assoc :compute-cluster-config/features
                    (map (fn [{:keys [key value]}]
                           {:compute-cluster-config.feature/key key
                            :compute-cluster-config.feature/value value
                            :db/id (d/tempid :db.part/user)})
                         features))))

(defn get-db-config-ents
  "Get the current dynamic cluster configuration entities from the database"
  [db]
  (let [configs (map #(d/entity db %)
                     (d/q '[:find [?compute-cluster-config ...]
                            :where
                            [?compute-cluster-config :compute-cluster-config/name ?name]]
                          db))]
    (map-from-vals :compute-cluster-config/name configs)))

(defn db-config-ents->configs
  "Convert cluster configuration database entities to plain configurations"
  [current-db-config-ents]
  (map-vals compute-cluster-config-ent->compute-cluster-config current-db-config-ents))

(defn get-db-configs
  "Get the current dynamic cluster configurations from the database"
  [db]
  (->> (get-db-config-ents db)
       (map-vals compute-cluster-config-ent->compute-cluster-config)))

(defn get-db-configs-latest
  "Delegates to get-db-configs with a fresh db"
  []
  (get-db-configs (d/db datomic/conn)))

;TODO: when all clusters have state, change the ComputeCluster protocol to add state operations
(defn compute-cluster->compute-cluster-config
  "Calculate dynamic cluster configuration from a compute cluster"
  [{:keys [state-atom state-locked?-atom name]
    {{:keys [template base-path ca-cert location features]} :config} :cluster-definition}]
  {:name name
   :template template
   :base-path base-path
   :ca-cert ca-cert
   :state @state-atom
   :state-locked? @state-locked?-atom
   :location location
   :features features})

;TODO: in the future all clusters will be dynamic and we shouldn't need this
(defn get-dynamic-clusters
  "Get the current in-memory dynamic clusters"
  []
  (->> @cluster-name->compute-cluster-atom
       (filter (fn [[_ {:keys [dynamic-cluster-config?]}]] dynamic-cluster-config?))
       (into {})))

(defn get-in-mem-configs
  "Get the current in-memory dynamic cluster configurations"
  []
  (->> (get-dynamic-clusters)
       (map-vals compute-cluster->compute-cluster-config)))

(defn compute-current-configs
  "Synthesize the current view of cluster configurations by looking at the current configurations in the database
  and the current configurations in memory. Alert on any inconsistencies. Database wins on inconsistencies."
  ([current-db-configs current-in-mem-configs]
   (let [[only-db-keys only-in-mem-keys both-keys] (util/diff-map-keys current-db-configs current-in-mem-configs)]
     (doseq [only-db-key only-db-keys]
       (when (not= :deleted (-> only-db-key current-db-configs :state))
         (log/info "Database cluster configuration is missing from in-memory configs."
                   "Cluster is only in the database and is not deleted."
                   "This is normal at startup, but shouldn't happen otherwise."
                   {:cluster-name only-db-key :cluster (current-db-configs only-db-key)})))
     (doseq [only-in-mem-key only-in-mem-keys]
       (when (not= :deleted (-> only-in-mem-key current-in-mem-configs :state))
         (log/error "In-memory cluster configuration is missing from the database."
                    "Cluster is only in memory and is not deleted."
                    "This should not have happened! A database update was missed!"
                    "The cluster configuration will be added to the database on the next update."
                    {:cluster-name only-in-mem-key :cluster (current-in-mem-configs only-in-mem-key)})))
     (doseq [key both-keys]
       (let [current-db-config
             (get current-db-configs key)
             {current-in-mem-location :location current-in-mem-features :features :as current-in-mem-config}
             (get current-in-mem-configs key)
             keys-to-keep-synced
             (cond-> [:base-path :ca-cert :state]
                     ; The location field is treated specially when comparing db and in-mem configs --
                     ; since this is a new field, it can be nil in-memory and non-nil in the db for a
                     ; limited time after the db has been populated and before the leader is restarted.
                     current-in-mem-location (conj :location))]
         (when (not= (select-keys current-db-config keys-to-keep-synced)
                     (select-keys current-in-mem-config keys-to-keep-synced))
           (log/error keys-to-keep-synced "differ between in-memory and database cluster configurations."
                      "Ensure that the database contains the correct configuration and then change leadership."
                      {:cluster-name key
                       :in-memory-cluster (current-in-mem-configs key)
                       :db-cluster (current-db-configs key)})))))
   (merge current-in-mem-configs current-db-configs))
  ([db]
   (compute-current-configs (get-db-configs db) (get-in-mem-configs))))

(defn get-job-instance-ids-for-cluster-name
  "Get the datomic ids of job instances that are running on the given compute cluster"
  [db cluster-name]
  (d/q '[:find [?job-instance ...]
         :in $ ?cluster-name [?status ...]
         :where
         [?job-instance :instance/status ?status]
         [?cluster :compute-cluster/cluster-name ?cluster-name]
         [?job-instance :instance/compute-cluster ?cluster]]
       db cluster-name [:instance.status/running :instance.status/unknown]))

(defn cluster-state-change-valid?
  "Check that the cluster state transition is valid."
  [db current-state new-state cluster-name]
  (case current-state
    :running (case new-state
               :running true
               :draining true
               :deleted false
               false)
    :draining (case new-state
                :running true
                :draining true
                :deleted (empty? (get-job-instance-ids-for-cluster-name db cluster-name))
                false)
    :deleted (case new-state
               :running false
               :draining false
               :deleted true
               false)
    false))

(defn config=?
  "Returns true if we can consider current-config and new-config to be equal.
   The location and state fields are special in this regard -- location can
   only transition from nil to non-nil, and state is validated in
   cluster-state-change-valid?."
  [{current-location :location :as current-config}
   {new-location :location :as new-config}]
  (cond (and (some? current-location)
             (not= current-location new-location))
        false

        :else
        (let [dissoc-non-comparable-fields
              #(-> %
                   (dissoc :state)
                   (dissoc :location)
                   (dissoc :features))]
          (= (dissoc-non-comparable-fields current-config)
             (dissoc-non-comparable-fields new-config)))))

(defn compute-config-update
  "Add validation info to a dynamic cluster configuration update."
  [db
   {current-name :name current-state :state current-state-locked? :state-locked? :as current-config}
   {new-name :name new-state :state :as new-config}
   force?]
  {:pre [(= current-name new-name)]}
  (let [update
        (cond
          (not (cluster-state-change-valid? db current-state new-state current-name))
          {:valid? false
           :reason (str "Cluster state transition from " current-state " to " new-state " is not valid.")}

          force?
          {:valid? true}

          (and (not= current-state new-state) current-state-locked?)
          {:valid? false
           :reason (str "Attempting to change cluster state from "
                        current-state " to " new-state " but not able because it is locked.")}

          (not (config=? current-config new-config))
          {:valid? false
           :reason (str "Attempting to change a comparable field when force? is false. Diff is "
                        (pr-str (data/diff (dissoc current-config :state) (dissoc new-config :state))))}

          :else
          {:valid? true})]
    (assoc update :goal-config new-config :differs? (not= current-config new-config) :cluster-name new-name)))

(defn validate-template
  "Validates the given template definition"
  [template-name {:keys [factory-fn] :as cluster-definition-template}]
  (cond
    (not cluster-definition-template)
    {:valid? false
     :reason (str "Attempting to create cluster with unknown template: " template-name)}
    (not factory-fn)
    {:valid? false
     :reason (str "Template for cluster has no factory-fn: " cluster-definition-template)}
    :else
    {:valid? true}))

(defn compute-config-insert
  "Add validation info to a new dynamic cluster configuration."
  [{:keys [name template] :as new-config}]
  (let [cluster-definition-template ((config/compute-cluster-templates) template)
        update (validate-template template cluster-definition-template)]
    (assoc update :goal-config new-config :differs? true :cluster-name name)))

(defn check-for-unique-constraint-violations
  "Check that the proposed resulting configurations don't collide on fields that should be unique, e.g. :base-path"
  [updates resulting-active-configs unique-constraint-field]
  (let [unique-constraint-value->cluster-names (->> resulting-active-configs
                                                    (group-by #(-> % :goal-config unique-constraint-field))
                                                    (map-vals #(map :cluster-name %)))
        duplicated-constraint-values (->> unique-constraint-value->cluster-names
                                          (filter #(-> (second %) count (> 1)))
                                          (map first)
                                          set)]

    (->> updates
         (map #(let [unique-constraint-value (-> % :goal-config unique-constraint-field)]
                 (cond-> %
                   (contains? duplicated-constraint-values unique-constraint-value)
                   (assoc :valid? false
                          :reason (str unique-constraint-field " is not unique between clusters: "
                                       (-> unique-constraint-value unique-constraint-value->cluster-names set)))))))))

(defn compute-config-updates
  "Take the current and desired configurations and compute the updates. Validate all updates that would occur,
  including unique constraint violations. .e.g base-path must be unique to each cluster.
  Returns a collection of 'update' objects. Each one describes an update to a cluster:
  [
    {
      :cluster-name - cluster for which we want to update the config
      :valid? - update is valid and can be made. when there is a mix of valid and invalid updates we will still attempt
                to make the valid updates. we will alert on any invalid updates.
      :reason - when invalid, reason explaining why it's invalid
      :goal-config - the config that we want the cluster to have
      :differs? - the goal config is different from the current config in the database
    }
    ...
  ]"
  [db current-configs new-configs force?]
  (let [[deletes-keys inserts-keys updates-keys] (util/diff-map-keys current-configs new-configs)
        updates (->> (concat
                       (map #(let [current (current-configs %)]
                               (compute-config-update db current (assoc current :state :deleted) force?)) deletes-keys)
                       (map #(compute-config-insert (new-configs %)) inserts-keys)
                       (map #(compute-config-update db (current-configs %) (new-configs %) force?) updates-keys))
                     (map (fn [{:keys [goal-config] :as update}] (assoc update :active? (not= :deleted (:state goal-config))))))
        resulting-active-configs (->> updates (filter :valid?) (filter :active?))]
    ;TODO check that number of running clusters is not less than a configured value. and make no updates if so
    (-> updates
      (check-for-unique-constraint-violations resulting-active-configs :base-path)
      (check-for-unique-constraint-violations resulting-active-configs :ca-cert))))

; we need to save pool-name->fenzo-state and exit-code-syncer so that clusters created later have access to them
(def exit-code-syncer-state-atom (atom nil))
(def pool-name->fenzo-state-atom (atom nil))

(defn initialize-cluster!
  "Create and initialize a ComputeCluster"
  [{:keys [template] :as config}]
  (let [cluster-definition-template ((config/compute-cluster-templates) template)
        _ (when-not cluster-definition-template (throw (ex-info "Attempting to create cluster with unknown template" {:config config})))
        factory-fn (:factory-fn cluster-definition-template)
        _ (when-not factory-fn (throw (ex-info "Template for cluster has no factory-fn" {:config config})))
        resolved (cook.util/lazy-load-var factory-fn)
        dynamic-cluster-config? (:dynamic-cluster-config? cluster-definition-template)
        _ (when (and (some? dynamic-cluster-config?) (not dynamic-cluster-config?))
            (throw (ex-info "dynamic-cluster-config? is set to false in cluster definition template but we are trying to add a cluster dynamically"
                            {:template template})))
        full-cluster-config (-> (:config cluster-definition-template) (merge config) (assoc :dynamic-cluster-config? true))
        cluster (resolved full-cluster-config {:exit-code-syncer-state @exit-code-syncer-state-atom})]
    (initialize-cluster cluster @pool-name->fenzo-state-atom)))

(defn execute-update!
  "Attempt to execute a valid cluster configuration update.
  Change cluster state if there is an existing cluster in memory.
  Create clusters if there is an active update an no corresponding cluster in memory. This can happen at startup or
  when a brand new cluster is added.
  Reflect updates in the database."
  [conn {:keys [valid? differs? active?] {:keys [name state state-locked?] :as goal-config} :goal-config}]
  {:pre [valid?]}
  (try
    (when differs?
      ; :db.unique/identity gives upsert behavior (update on insert), so we don't need to specify an entity ID when updating
      @(d/transact
         conn
         [; We need to retract existing features; otherwise,
          ; features simply get added to the existing list
          [:db.fn/resetAttribute
           [:compute-cluster-config/name name]
           :compute-cluster-config/features
           nil]
          (assoc (compute-cluster-config->compute-cluster-config-ent goal-config)
            :db/id (d/tempid :db.part/user))]))
    (let [{:keys [state-atom state-locked?-atom] :as cluster} (@cluster-name->compute-cluster-atom name)]
      (if cluster
        (do
          (reset! state-atom state)
          (reset! state-locked?-atom state-locked?))
        (when active? (initialize-cluster! goal-config))))
    {:update-succeeded true}
    (catch Throwable t
      (log/error t "Failed to update cluster" goal-config)
      {:update-succeeded false :error-message (.toString t)})))

(defn update-compute-clusters-helper
  "Helper function for update-compute-cluster and update-compute-clusters. See those functions for full description."
  [conn db current-configs new-configs force?]
  (let [updates (compute-config-updates db current-configs new-configs force?)
        dissoc-ca-cert
        (fn [config]
          (dissoc config :ca-cert))
        dissoc-ca-cert-vals
        (fn [configs]
          (map-vals dissoc-ca-cert configs))]
    (doseq [[cluster-name config] (dissoc-ca-cert-vals current-configs)]
      (log/info "Updating dynamic cluster current config for" cluster-name config))
    (doseq [[cluster-name config] (dissoc-ca-cert-vals new-configs)]
      (log/info "Updating dynamic cluster new config for" cluster-name config))
    (doseq [{:keys [goal-config] :as updated-config} (map #(update % :goal-config dissoc-ca-cert) updates)]
      (log/info "Updating dynamic cluster updated config for" (:name goal-config) (assoc updated-config :force? force?)))
    (let [updates-with-results (map
                                 #(assoc % :update-result
                                           (when (:valid? %) (execute-update! conn %)))
                                 updates)]
      (doseq [update updates-with-results
              :let [update-result (:update-result update)]]
        (if (:valid? update)
          (if (:update-succeeded update-result)
            (log/info "Update for cluster" (:cluster-name update) "successful")
            (log/error "Update failed!" update))
          (log/warn "Invalid update!" update)))
      updates-with-results)))

(defn update-compute-clusters
  "This function allows adding or updating the current compute cluster configurations. Takes
  in a map of configurations with cluster name as the key. These configurations represent the only known
  configurations, and clusters that are missing from this map are set to the 'deleted' state.
  Only the state of an existing cluster and its configuration can be changed unless force? is set to true."
  ([conn new-configs force?]
   (locking cluster-name->compute-cluster-atom
     (let [db (d/db conn)
           current-configs (compute-current-configs db)]
       (update-compute-clusters-helper conn db current-configs new-configs force?)))))

(defn update-compute-cluster
  "This function allows adding or updating the current compute cluster configurations. Takes
  in a single configuration and updates or creates it.
  Only the state of an existing cluster and its configuration can be changed unless force? is set to true."
  [conn new-config force?]
  (locking cluster-name->compute-cluster-atom
    (let [db (d/db conn)
          current-configs (compute-current-configs db)]
      (update-compute-clusters-helper conn db current-configs (assoc current-configs (:name new-config) new-config) force?))))

(defn get-compute-clusters
  "Get the current dynamic compute clusters. Returns both the in-memory cluster configs and the configurations in the database.
  The configurations in the database might be different and will not take effect until restart."
  [conn]
  {:in-mem-configs (->> (get-dynamic-clusters)
                        vals
                        (map #(let [config (compute-cluster->compute-cluster-config %)]
                                (assoc config :cluster-definition (:cluster-definition %)))))
   :db-configs (->> (get-db-configs (d/db conn)) vals)})

(defn delete-compute-cluster
  "Delete a dynamic compute clusters from the database."
  [conn {:keys [name]}]
  (d/transact conn [[:db.fn/retractEntity [:compute-cluster-config/name name]]]))

(defn dummy-cluster-configurations-fn
  "This function is used to periodically get cluster configurations, but it always returns the same list from
  the compute-cluster-options settings section"
  []
  (->> (:compute-cluster-configurations (config/compute-cluster-options))
       (map #(let [kubeconfig (slurp (:config-file %))
                   ca-cert (->> kubeconfig (re-find #"certificate-authority-data: (.+)") second)
                   base-path (->> kubeconfig (re-find #"server: (.+)") second)]
               (merge {:base-path base-path :ca-cert ca-cert} (dissoc % :config-file))))
       (map-from-vals :name)))

(def dummy-cluster-configurations-fn-memo
  (memoize dummy-cluster-configurations-fn))

(defn compute-cluster->location
  "Given a compute cluster, returns its location"
  [compute-cluster]
  (-> compute-cluster
      :cluster-definition
      :config
      :location))
