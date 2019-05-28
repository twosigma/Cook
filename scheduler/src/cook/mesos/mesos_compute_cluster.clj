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
  (:require [clojure.tools.logging :as log]
            [cook.config :as config]
            [cook.compute-cluster :as cc]
            [datomic.api :as d]
                        [mesomatic.scheduler :as mesos]))

(defrecord MesosComputeCluster [compute-cluster-name framework-id db-id driver-atom]
  ComputeCluster
  (compute-cluster-name [this]
    compute-cluster-name)
  (get-mesos-driver-hack [this]
    @driver-atom)
  (launch-tasks [this offers task-metadata-seq]
    (mesos/launch-tasks! @driver-atom
                         (mapv :id offers)
                         (task/compile-mesos-messages offers task-metadata-seq)))
  (db-id [this]
    db-id)
  (get-mesos-framework-id-hack [this]
    framework-id)
  (set-mesos-driver-atom-hack! [this driver]
    (reset! driver-atom driver)))

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
  [conn {:keys [compute-cluster-name framework-id] :as mesos-cluster}]
  {:pre [compute-cluster-name
         framework-id]}
  (let [cluster-entity-id (get-mesos-cluster-entity-id (d/db conn) mesos-cluster)]
    (when-not cluster-entity-id
      (write-compute-cluster conn (mesos-cluster->compute-cluster-map-for-datomic mesos-cluster)))
    (->MesosComputeCluster
      compute-cluster-name
      framework-id
      (or cluster-entity-id (get-mesos-cluster-entity-id (d/db conn) mesos-cluster))
      (atom nil))))

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
  [conn settings]
  (let [compute-clusters (->> (get-mesos-clusters-from-config settings)
                              (map (partial get-mesos-compute-cluster conn))
                              (map register-compute-cluster!))]
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
      compute-cluster-name->ComputeCluster))


