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
            [datomic.api :as d]
                        [mesomatic.scheduler :as mesos]))

(defprotocol ComputeCluster
  ; These methods should accept bulk data and process in batches.
  ;(kill-tasks [this task]
  (launch-tasks [this offers task-metadata-seq])
  (compute-cluster-name [this])

  (db-id [this]
    "Get a database entity-id for this compute cluster (used for putting it into a task structure).")

  (get-mesos-driver-hack [this]
    "Get the mesos driver. Hack; any funciton invoking this should be put within the compute-cluster implementation")
  (get-mesos-framework-id-hack [this]
    "Short term hack to get the framework ID. When we migrate/hide all mesos functionality behind the ComputeClusterAPI, this will go away.")
  (set-mesos-driver-atom-hack! [this driver]
    "Hack to overwrite the driver. Used until we fix the initialization order of compute-cluster"))

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

