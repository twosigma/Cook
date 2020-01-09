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
            [datomic.api :as d]))

(defprotocol ComputeCluster
  ; These methods should accept bulk data and process in batches.
  ;(kill-tasks [this task]
  (launch-tasks [this offers task-metadata-seq])
  (compute-cluster-name [this])

  (db-id [this]
    "Get a database entity-id for this compute cluster (used for putting it into a task structure).")

  (initialize-cluster [this pool->fenzo running-task-ents]
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

  (use-cook-executor? [this]
    "Returns true if this compute cluster makes use of the Cook executor for running tasks")

  (container-defaults [this]
    "Default values to use for containers launched in this compute cluster")

  (max-tasks-per-host [this]
    "The maximum number of tasks that a given host should run at the same time")

  (num-tasks-on-host [this hostname]
    "The number of tasks currently running on the given hostname"))

(defn safe-kill-task
  "A safe version of kill task that never throws. This reduces the risk that errors in one compute cluster propagate and cause problems in another compute cluster."
  [compute-cluster task-id]
  (try
    (kill-task compute-cluster task-id)
    (catch Throwable t
      (log/error t "In compute cluster" compute-cluster "error killing task" task-id))))

(defn kill-task-if-possible [compute-cluster task-id]
  "If compute cluster is nil, print a warning instead of killing the task. There are cases, in particular,
  lingering tasks, stragglers, or cancelled tasks where the task might outlive the compute cluster it was
  member of. When this occurs, the looked up compute cluster is null and trying to kill via it would cause an NPE,
  when in reality, it's relatively innocuous. So, we have this wrapper to use in those circumstances."
  (if compute-cluster
    (safe-kill-task compute-cluster task-id)
    (log/warn "Unable to kill task " task-id " because compute-cluster is nil")))

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
  (let [first-cluster-name (->> config/config
                                :settings
                                :compute-clusters
                                (map (fn [{:keys [config]}] (:compute-cluster-name config)))
                                first)]
    (compute-cluster-name->ComputeCluster first-cluster-name)))

