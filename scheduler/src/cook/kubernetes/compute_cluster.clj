(ns cook.kubernetes.compute-cluster
  (:require [clj-time.core :as t]
            [clj-time.periodic :as tp]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [cook.compute-cluster :as cc]
            [cook.config :as config]
            [cook.datomic]
            [cook.kubernetes.api :as api]
            [cook.pool]
            [cook.mesos.sandbox :as sandbox]
            [cook.scheduler.scheduler :as scheduler]
            [datomic.api :as d]
            [plumbing.core :as pc])
  (:import (com.twosigma.cook.kubernetes WatchHelper)
           (io.kubernetes.client ApiClient ApiException)
           (io.kubernetes.client.apis CoreV1Api)
           (io.kubernetes.client.custom Quantity Quantity$Format)
           (io.kubernetes.client.models V1Pod V1Node V1Container V1ObjectMeta V1EnvVar V1ResourceRequirements
                                        V1PodSpec V1PodStatus V1ContainerState)
           (io.kubernetes.client.util Config Watch)
           (java.util UUID)
           (java.util.concurrent Executors ExecutorService)))

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

(defn pod->pod-state
  [pod]
  (when pod
    (let [^V1PodStatus pod-status (.getStatus pod)
          container-statuses (.getContainerStatuses pod-status)
          job-status (first (filter (fn [c] (= "job" (.getName c)))
                                    container-statuses))]
      (if job-status
        (let [^V1ContainerState state (.getState job-status)]
          (cond
            (.getWaiting state)
            {:state :pod/waiting
             :reason (-> state .getWaiting .getReason)}
            (.getRunning state)
            {:state :pod/running
             :reason "Running"}
            (.getTerminated state)
            (let [exit-code (-> state .getTerminated .getExitCode)]
              (if (= 0 exit-code)
                {:state :pod/succeeded
                 :exit exit-code
                 :reason (-> state .getTerminated .getReason)}
                {:state :pod/failed
                 :exit exit-code
                 :reason (-> state .getTerminated .getReason)}))
            :default
            {:state :pod/unknown
             :reason "Unknown"}))

        {:state :pod/waiting
         :reason "Pending"}))))


(defn pod-state->task-status
  [pod {:keys [state reason]}]
  (let [task-state (case state
                     :pod/waiting :task-staging
                     :pod/running :task-running
                     :pod/succeeded :task-finished
                     :pod/failed :task-failed)
        task-id (-> pod .getMetadata .getName)]

    {:task-id {:value task-id}
     :reason reason ; TODO(pschorf): Map as best as possible to mesos reasons
     :state task-state}))

(defn make-pod-watch-callback
  [handle-status-update handle-exit-code]
  (fn pod-watch-callback
    [prev-pod pod]
    (try
      (let [prev-state (pod->pod-state prev-pod)
            state (pod->pod-state pod)
            task-id (-> (or pod prev-pod)
                        .getMetadata .getName)
            namespace (-> (or pod prev-pod)
                          .getMetadata
                          .getNamespace)]
        (if (= namespace "cook")
          (when (not (= prev-state state))
            (log/debug "Updating state for" task-id "from" prev-state "to" state)
            (case (:state state)
              :pod/running
              (handle-status-update (pod-state->task-status pod state))
              :pod/waiting
              (handle-status-update (pod-state->task-status pod state))
              :pod/succeeded
              (do
                (handle-status-update (pod-state->task-status pod state))
                (handle-exit-code task-id (:exit state)))
              :pod/failed
              (do
                (handle-status-update (pod-state->task-status pod state))
                (handle-exit-code task-id (:exit state)))
              :pod/unknown
              (log/error "Unable to determine pod status in callback")))
          (log/debug "Skipping state update for" task-id)))
      (catch Exception e
        (log/error e "Error processing status update")))))

(defrecord KubernetesComputeCluster [^ApiClient api-client name entity-id match-trigger-chan exit-code-syncer-state current-pods-atom current-nodes-atom expected-state-map existing-state-map]
  cc/ComputeCluster
  (launch-tasks [this offers task-metadata-seq]
    (let [api (CoreV1Api. api-client)]
      (doseq [task-metadata task-metadata-seq]
        (update-expected-state
          expected-state-map
          (:task/uuid task-metadata)
          {:expected-state :expected/running :launch-pod (task-metadata->pod pod)})
        (log/debug "Launching pod" pod)
        (try
          (.createNamespacedPod api "cook" pod nil nil nil)
          (catch ApiException e
            (log/error e "Error creating pod:" (.getResponseBody e))
            (throw e))))))

  (kill-task [this task-id]
    (throw (UnsupportedOperationException. "Cannot kill tasks")))

  (decline-offer [this offer-id])

  (db-id [this]
    entity-id)

  (compute-cluster-name [this]
    name)

  (initialize-cluster [this pool->fenzo pool->offers-chan]
    (let [conn cook.datomic/conn
          handle-exit-code (fn handle-exit-code [task-id exit-code]
                             (sandbox/aggregate-exit-code exit-code-syncer-state task-id exit-code))
          handle-status-update (fn handle-status-update [status]
                                 (scheduler/handle-status-update conn
                                                                 this
                                                                 pool->fenzo
                                                                 (constantly nil) ; no sandboxes to sync
                                                                 status))
          pod-callback (make-pod-watch-callback handle-status-update handle-exit-code)]
      (api/initialize-pod-watch api-client current-pods-atom pod-callback))
    (api/initialize-node-watch api-client current-nodes-atom)

    ; TODO(pschorf): Figure out a better way to plumb these through
    (chime/chime-at (tp/periodic-seq (t/now) (t/seconds 2))
                    (fn [_]
                      (try
                        (let [nodes @current-nodes-atom
                              pods @current-pods-atom
                              offers (generate-offers nodes pods this)
                              pool (config/default-pool)
                              chan (pool->offers-chan pool)] ; TODO(pschorf): Support pools
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
  [{:keys [compute-cluster-name ^String config-file]} {:keys [exit-code-syncer-state
                                                              trigger-chans]}]
  (let [conn cook.datomic/conn
        cluster-entity-id (get-or-create-cluster-entity-id conn compute-cluster-name)
        api-client (Config/fromConfig config-file)
        compute-cluster (->KubernetesComputeCluster api-client compute-cluster-name cluster-entity-id
                                                    (:match-trigger-chan trigger-chans)
                                                    exit-code-syncer-state (atom {}) (atom {}))]
    (cc/register-compute-cluster! compute-cluster)
    compute-cluster))