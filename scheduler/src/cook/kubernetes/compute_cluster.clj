(ns cook.kubernetes.compute-cluster
  (:require [clj-time.core :as t]
            [clj-time.periodic :as tp]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [cook.compute-cluster :as cc]
            [cook.config :as config]
            [cook.kubernetes.api :as api]
            [cook.kubernetes.controller :as controller]
            [cook.pool]
            [cook.mesos.sandbox :as sandbox]
            [cook.scheduler.scheduler :as scheduler]
            [datomic.api :as d]
            [plumbing.core :as pc])
  (:import (io.kubernetes.client ApiClient)
           (io.kubernetes.client.apis CoreV1Api)
           (io.kubernetes.client.models V1PodStatus V1ContainerState)
           (io.kubernetes.client.util Config)
           (java.util UUID)))

(defn generate-offers
  [node-name->node pod-name->pod compute-cluster]
  (let [node-name->capacity (api/get-capacity node-name->node)
        node-name->consumed (api/get-consumption pod-name->pod)
        ; TODO(pschorf): Should also include recently launched jobs
        node-name->available (pc/map-from-keys (fn [node-name]
                                                 (merge-with -
                                                             (node-name->capacity node-name)
                                                             (node-name->consumed node-name)))
                                               (keys node-name->capacity))]
    (log/info "Capacity: " node-name->capacity "Consumption:" node-name->consumed)
    (map (fn [[node-name available]]
           {:id {:value (str (UUID/randomUUID))}
            :framework-id (cc/compute-cluster-name compute-cluster)
            :slave-id {:value node-name}
            :hostname node-name
            :resources [{:name "mem" :type :value-scalar :scalar (max 0.0 (:mem available))}
                        {:name "cpus" :type :value-scalar :scalar (max 0.0 (:cpus available))}
                        {:name "disk" :type :value-scalar :scalar 0.0}]
            :attributes []
            :executor-ids []
            :compute-cluster compute-cluster
            :reject-after-match-attempt true})
         node-name->available)))

(defn make-pod-watch-callback
  [kcc]
  (fn pod-watch-callback
    [prev-pod pod]
    (try
      (if (nil? pod)
        (controller/pod-deleted kcc prev-pod)
        (controller/pod-update kcc pod))
      (catch Exception e
        (log/error e "Error processing status update")))))

(defn task-ents->map-by-task-id
  [task-ents]
  (->> task-ents
       (map (fn [task-ent] [(str (:instance/task-id task-ent)) task-ent]))
       (into {})))

(defn task-ent->expected-state
  [task-ent]
  
  (case (:instance/status task-ent)
    :instance.status/unknown {:expected-state :expected/starting}
    :instance.status/running {:expected-state :expected/running}
    :instance.status/failed {:expected-state :expected/completed}
    :instance.status/success {:expected-state :expected/completed}))

(defn determine-expected-state
  "We need to determine everything we should be tracking when we construct the expected state. We should be tracking all tasks that are in the running state as well
  as all pods in kubernetes. We're given an already existing list of all running tasks entities (via (->> (cook.tools/get-running-task-ents)."
  [conn compute-cluster-name running-tasks-ents current-pods-atom]
  (let [db (d/db conn)
        all-tasks-ids-in-pods (->> @current-pods-atom keys (into #{}))
        _ (log/info "All tasks in pods: " all-tasks-ids-in-pods)
        running-tasks-in-cc-ents (filter
                                   #(-> % cook.task/task-entity->compute-cluster-name (= compute-cluster-name))
                                   running-tasks-ents)
        _ (log/info "Running tasks in cc in datomic: " running-tasks-in-cc-ents)
        cc-running-tasks-map (task-ents->map-by-task-id running-tasks-in-cc-ents)
        _ (log/info "Running tasks map: " cc-running-tasks-map)
        cc-running-tasks-ids (->> cc-running-tasks-map keys (into #{}))
        extra-tasks-map (->> (set/difference all-tasks-ids-in-pods cc-running-tasks-ids)
                             (map (fn [task-id] [task-id (cook.tools/retrieve-instance db task-id)]))
                             (filter (fn [[_ task-ent]] (some? task-ent)))
                             (into {}))
        all-tasks-ents-map (set/union extra-tasks-map cc-running-tasks-map)]
    (doseq [[k v] all-tasks-ents-map]
      (log/info "Doing processing for " k " ---> " v))
    (into {}
          (map (fn [[k v]] [k (task-ent->expected-state v)]) all-tasks-ents-map))))

(defrecord KubernetesComputeCluster [^ApiClient api-client name entity-id match-trigger-chan exit-code-syncer-state
                                     current-pods-atom current-nodes-atom expected-state-map existing-state-map
                                     pool->fenzo-atom]
  cc/ComputeCluster
  (launch-tasks [this offers task-metadata-seq]
    (let [api (CoreV1Api. api-client)]
      (doseq [task-metadata task-metadata-seq]
        (controller/update-expected-state
          this
          (:task-id task-metadata)
          {:expected-state :expected/starting :launch-pod (api/task-metadata->pod task-metadata)}))))

  (kill-task [this task-id]
    (controller/update-expected-state this task-id {:expected-state :expected/killed}))

  (decline-offers [this offer-ids]
    (log/debug "Rejecting offer ids" offer-ids))

  (db-id [this]
    entity-id)

  (compute-cluster-name [this]
    name)

  (initialize-cluster [this pool->fenzo running-task-ents]
    ; Initialize the pod watch path.
    (let [pod-callback (make-pod-watch-callback this)]
      (api/initialize-pod-watch api-client current-pods-atom pod-callback)
      ; We require that initialize-pod-watch sets current-pods-atom before completing.
      (determine-expected-state conn name running-task-ents current-pods-atom))

    ; Initialize the node watch path.
    (api/initialize-node-watch api-client current-nodes-atom)
    (reset! pool->fenzo-atom pool->fenzo)

    ; TODO(pschorf): Deliver when leadership lost
    (async/chan 1))

  (current-leader? [this]
    true)

  (pending-offers [this pool-name]
    (if (or (= pool-name (config/default-pool)) ; TODO(pschorf): Support pools
            (= pool-name "no-pool"))
      (let [nodes @current-nodes-atom
            pods @current-pods-atom]
        (generate-offers nodes pods this))
      []))

  (restore-offers [this pool-name offers]))

(defn get-or-create-cluster-entity-id
  [conn compute-cluster-name]
  (let [query-result (d/q '[:find [?c]
                            :in $ ?cluster-name
                            :where
                            [?c :compute-cluster/type :compute-cluster.type/kubernetes]
                            [?c :compute-cluster/cluster-name ?cluster-name]]
                          (d/db conn) compute-cluster-name)]
    (if (seq query-result)
      (first query-result)
      (cc/write-compute-cluster conn {:compute-cluster/type :compute-cluster.type/kubernetes
                                      :compute-cluster/cluster-name compute-cluster-name}))))

(defn factory-fn
  [{:keys [compute-cluster-name ^String config-file]} {:keys [exit-code-syncer-state
                                                              trigger-chans]}]
  (let [conn cook.datomic/conn
        cluster-entity-id (get-or-create-cluster-entity-id conn compute-cluster-name)
        api-client (Config/fromConfig config-file)
        compute-cluster (->KubernetesComputeCluster api-client compute-cluster-name cluster-entity-id
                                                    (:match-trigger-chan trigger-chans)
                                                    exit-code-syncer-state (atom {}) (atom {})
                                                    (atom {:type :expected-state-map})
                                                    (atom {:type :existing-state-map})
                                                    (atom nil))]
    (cc/register-compute-cluster! compute-cluster)
    compute-cluster))