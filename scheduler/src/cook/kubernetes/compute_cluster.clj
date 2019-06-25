(ns cook.kubernetes.compute-cluster
  (:require [clj-time.periodic :as tp]
            [cook.compute-cluster :as cc]
            [cook.datomic]
            [cook.scheduler.scheduler :as scheduler]
            [datomic.api :as d]
            [clojure.core.async :as async]
            [plumbing.core :as pc]
            [clojure.tools.logging :as log]
            [clj-time.core :as t])
  (:import (io.kubernetes.client ApiClient)
           (io.kubernetes.client.util Config Watch)
           (io.kubernetes.client.apis CoreV1Api)
           (io.kubernetes.client.models V1Pod V1Node V1Container)
           (com.twosigma.cook.kubernetes WatchHelper)
           (java.util.concurrent Executors ExecutorService)
           (io.kubernetes.client.custom Quantity)
           (java.util UUID)))

(def ^ExecutorService kubernetes-executor (Executors/newFixedThreadPool 2))

(defn handle-watch-updates
  [state-atom ^Watch watch key-fn]
  (while (.hasNext watch)
    (let [update (.next watch)
          item (.-object update)]
      (case (.-type update)
        "ADDED" (swap! state-atom (fn [m] (assoc m (key-fn item) item)))
        "MODIFIED" (swap! state-atom (fn [m] (assoc m (key-fn item) item)))
        "DELETED" (swap! state-atom (fn [m] (dissoc m (key-fn item))))))))

(let [current-pods-atom (atom {})]
  (defn initialize-pod-watch
    [^ApiClient api-client]
    (let [api (CoreV1Api. api-client)
          current-pods (.listPodForAllNamespaces api
                                                 nil ; continue
                                                 nil ; fieldSelector
                                                 nil ; includeUninitialized
                                                 nil ; labelSelector
                                                 nil ; limit
                                                 nil ; pretty
                                                 nil ; resourceVersion
                                                 nil ; timeoutSeconds
                                                 nil ; watch
                                                 )
          pod-name->pod (pc/map-from-vals (fn [^V1Pod pod]
                                            (-> pod
                                                .getMetadata
                                                .getName))
                                          (.getItems current-pods))]
      (log/info "Updating current-pods-atom with pods" (keys pod-name->pod))
      (reset! current-pods-atom pod-name->pod)
      (let [watch (WatchHelper/createPodWatch api-client (-> current-pods
                                                             .getMetadata
                                                             .getResourceVersion))]
        (.submit kubernetes-executor ^Callable
        (fn []
          (try
            (handle-watch-updates current-pods-atom watch (fn [p] (-> p .getMetadata .getName)))
            (catch Exception e
              (log/error e "Error during watch")
              (.close watch)
              (initialize-pod-watch api-client))))))))
  (defn get-pods
    []
    @current-pods-atom))


(let [current-nodes-atom (atom {})]
  (defn initialize-node-watch [^ApiClient api-client]
    (let [api (CoreV1Api. api-client)
          current-nodes (.listNode api
                                   nil ; includeUninitialized
                                   nil ; pretty
                                   nil ; continue
                                   nil ; fieldSelector
                                   nil ; labelSelector
                                   nil ; limit
                                   nil ; resourceVersion
                                   nil ; timeoutSeconds
                                   nil ; watch
                                   )
          node-name->node (pc/map-from-vals (fn [^V1Node node]
                                              (-> node .getMetadata .getName))
                                            (.getItems current-nodes))]
      (reset! current-nodes-atom node-name->node)
      (let [watch (WatchHelper/createNodeWatch api-client (-> current-nodes .getMetadata .getResourceVersion))]
        (.submit kubernetes-executor ^Callable
          (fn []
            (try
              (handle-watch-updates current-nodes-atom watch (fn [n] (-> n .getMetadata .getName)))
              (catch Exception e
                (log/warn e "Error during node watch")
                (initialize-node-watch api-client))
              (finally
                (.close watch))))))))

  (defn get-nodes
    []
    @current-nodes-atom))

(defn to-double
  [^Quantity q]
  (-> q .getNumber .doubleValue))

(defn convert-resource-map
  [m]
  {:mem (if (get m "memory")
          (-> m (get "memory") to-double (/ (* 1024 1024)))
          0.0)
   :cpus (if (get m "cpu")
           (-> m (get "cpu") to-double)
           0.0)})

(defn get-capacity
  [node-name->node]
  (pc/map-vals (fn [^V1Node node]
                 (-> node .getStatus .getCapacity convert-resource-map))
               node-name->node))

(defn get-consumption
  [pod-name->pod]
  (let [node-name->pods (group-by (fn [^V1Pod p] (-> p .getSpec .getNodeName))
                                  (vals pod-name->pod))
        node-name->requests (pc/map-vals (fn [pods]
                                           (->> pods
                                                (map (fn [^V1Pod pod]
                                                       (let [containers (-> pod .getSpec .getContainers)
                                                             container-requests (map (fn [^V1Container c]
                                                                                       (-> c
                                                                                           .getResources
                                                                                           .getRequests
                                                                                           convert-resource-map))
                                                                                     containers)]
                                                         (apply merge-with + container-requests))))
                                                (apply merge-with +)))
                                         node-name->pods)]
    node-name->requests))

(defn generate-offers
  [node-name->node pod-name->pod compute-cluster]
  (let [node-name->capacity (get-capacity node-name->node)
        node-name->consumed (get-consumption pod-name->pod)
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
            :reject-after-match true})
         node-name->available)))

(defrecord KubernetesComputeCluster [^ApiClient api-client name entity-id match-trigger-chan]
  cc/ComputeCluster
  (launch-tasks [this offers task-metadata-seq]
    (throw (UnsupportedOperationException. "Cannot launch tasks")))

  (kill-task [this task-id]
    (throw (UnsupportedOperationException. "Cannot kill tasks")))

  (decline-offer [this offer-id])

  (db-id [this]
    entity-id)

  (compute-cluster-name [this]
    name)

  (initialize-cluster [this pool->fenzo pool->offers-chan]
    (initialize-pod-watch api-client)
    (initialize-node-watch api-client)

    ; TODO(pschorf): Figure out a better way to plumb these through
    (chime/chime-at (tp/periodic-seq (t/now) (t/seconds 2))
                    (fn [_]
                      (try
                        (let [nodes (get-nodes)
                              pods (get-pods)
                              offers (generate-offers nodes pods this)
                              [pool chan] (first pool->offers-chan)] ; TODO(pschorf): Support pools
                          (log/info "Processing offers:" offers)
                          (scheduler/receive-offers chan match-trigger-chan this pool offers))
                        (catch Exception e
                          (log/error e "Exception while forwarding offers")))))
    ; TODO(pschorf): Deliver when leadership lost
    (async/chan 1))

  (current-leader? [this]
    true))

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
  [{:keys [compute-cluster-name ^String config-file]} {:keys [trigger-chans]}]
  (let [conn cook.datomic/conn
        cluster-entity-id (get-or-create-cluster-entity-id conn compute-cluster-name)
        api-client (Config/fromConfig config-file)
        compute-cluster (->KubernetesComputeCluster api-client compute-cluster-name cluster-entity-id
                                                    (:match-trigger-chan trigger-chans))]
    (cc/register-compute-cluster! compute-cluster)
    compute-cluster))