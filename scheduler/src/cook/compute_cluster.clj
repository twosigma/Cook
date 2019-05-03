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
            [datomic.api :as d]))

(defn- write-compute-cluster
  "Create a missing compute-cluster for one that's not yet in the database."
  [conn compute-cluster]
  (log/info "Installing a new compute cluster in datomic for " compute-cluster)
  @(d/transact
     conn
     [(assoc compute-cluster :db/id (d/tempid :db.part/user))]))

(defn- mesos-cluster->compute-cluster-map-for-datomic
  "Given a mesos cluster dictionary, determine the datomic entity it should correspond to."
  [{:keys [compute-cluster-name framework-id]}]
  {:compute-cluster/type :compute-cluster.type/mesos
   :compute-cluster/cluster-name compute-cluster-name
   :compute-cluster/mesos-framework-id framework-id})

(defn get-mesos-cluster-entity-id
  "Given a configuration map for a mesos cluster, return the datomic entity-id corresponding to the cluster, if it exists. Internal helper function."
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

(defn get-mesos-cluster-map
  "Process one mesos cluster specification, returning the entity id of the corresponding compute-cluster, creating the cluster if it does not exist."
  [conn {:keys [compute-cluster-name framework-id] :as mesos-cluster}]
  {:pre [compute-cluster-name
         framework-id]}
  (let [cluster-entity (get-mesos-cluster-entity-id (d/db conn) mesos-cluster)]
    (when-not cluster-entity
      (write-compute-cluster conn (mesos-cluster->compute-cluster-map-for-datomic mesos-cluster)))
    {:cluster-type :mesos-cluster
     :compute-cluster-name compute-cluster-name
     :mesos-framework-id framework-id
     :db-id (get-mesos-cluster-entity-id (d/db conn) mesos-cluster)}))

(defn get-mesos-clusters-from-config
  "Get all of the mesos clusters defined in the configuration.
  This is a wart that undo'ed the shattering of the :mesos {...} into separate keys that
  occurs in config.edn, and consolidates it into one dictionary for downstream consumption."
  [{:keys [mesos-compute-cluster-name mesos-framework-id]}]
  [{:compute-cluster-name mesos-compute-cluster-name :framework-id mesos-framework-id}])

(def cluster-name->cluster-dict-atom (atom nil))

(defn cluster-name->db-id
  "Given a cluster name, return the db-id we should use to refer to that compute cluster
  when we put it within a task structure."
  [cluster-name]
  (let [{:keys [db-id]} (get @cluster-name->cluster-dict-atom cluster-name)]
    ; All clusters referenced by name must have been installed in the db previously.
    (when-not db-id (throw (IllegalStateException. (str "Was asked to lookup db-id for " cluster-name " and got nil"))))
    db-id))

(defn setup-cluster-map-config
  "Setup the cluster-map configs, linking a cluster name to the associated metadata needed
  to represent/process it."
  [conn settings]
  (let [compute-clusters (->> (get-mesos-clusters-from-config settings)
                              (map (partial get-mesos-cluster-map conn)))
        reduce-fn (fn [accum {:keys [compute-cluster-name] :as cluster-dict}]
                    (when (contains? accum compute-cluster-name)
                      (throw (IllegalArgumentException. (str "Multiple compute-clusters have the same name: " compute-cluster-name))))
                    (assoc accum compute-cluster-name cluster-dict))]
    (doseq [ii compute-clusters]
      (log/info "Setting up compute cluster: " ii))
    (reset! cluster-name->cluster-dict-atom (reduce reduce-fn {} compute-clusters))))

; TODO: Until we thread the compute-cluster-name through fenzo, we'll need this hack to remember
; the cluster name so that we can find the compute cluster when generating the fenzo result.
(defn cluster-name-hack
  "A hack that today returns the default cluster name. This is used e.g., in processing
  fenzo responses. In the future it will need to exist in order to fill in compute cluster
  objects in legacy task entities."
  []
  (-> @cluster-name->cluster-dict-atom
      keys
      first))