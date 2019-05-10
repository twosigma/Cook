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

(defn get-mesos-cluster-map
  "Process one mesos cluster specification, returning the entity id of the corresponding compute-cluster,
  creating the cluster if it does not exist."
  [conn {:keys [compute-cluster-name framework-id] :as mesos-cluster}]
  {:pre [compute-cluster-name
         framework-id]}
  (let [cluster-entity-id (get-mesos-cluster-entity-id (d/db conn) mesos-cluster)]
    (when-not cluster-entity-id
      (write-compute-cluster conn (mesos-cluster->compute-cluster-map-for-datomic mesos-cluster)))
    {:compute-cluster-type :mesos-cluster
     :compute-cluster-name compute-cluster-name
     :mesos-framework-id framework-id
     :db-id (or cluster-entity-id (get-mesos-cluster-entity-id (d/db conn) mesos-cluster))}))

(defn get-default-cluster-name-for-legacy
  "What cluster name to put on for legacy jobs when generating their compute-cluster."
  []
  {:post [%]} ; Never returns nil.
  (-> config/config :settings :mesos-compute-cluster-name))

; A hack to store the mesos cluster name, until we refactor the code so that we support multiple clusters. In the long term future
; this is probably replaced with a function from driver->cluster-id, or the cluster name is propagated by function arguments and
; closed over.
(defn get-mesos-cluster-name-hack
  []
  {:post [%]} ; Never returns nil.
  (-> config/config :settings :mesos-compute-cluster-name))

(defn get-mesos-clusters-from-config
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

(def cluster-name->cluster-dict-atom (atom nil))

(defn cluster-name->db-id
  "Given a cluster name, return the db-id we should use to refer to that compute cluster
  when we put it within a task structure."
  [cluster-name]
  {:post [%]} ; Never returns nil.
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
                      (throw (IllegalArgumentException.
                               (str "Multiple compute-clusters have the same name: " compute-cluster-name))))
                    (assoc accum compute-cluster-name cluster-dict))]
    (run! (fn [compute-cluster]
            (log/info "Setting up compute cluster: " compute-cluster)) compute-clusters)
    (reset! cluster-name->cluster-dict-atom (reduce reduce-fn {} compute-clusters))))
