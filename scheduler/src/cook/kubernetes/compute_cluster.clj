(ns cook.kubernetes.compute-cluster
  (:require [chime :refer [chime-at chime-ch]]
            [clj-time.core :as time]
            [clj-time.periodic :refer [periodic-seq]]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [cook.cache :as cache]
            [cook.caches :as caches]
            [cook.compute-cluster :as cc]
            [cook.compute-cluster.metrics :as ccmetrics]
            [cook.config :as config]
            [cook.kubernetes.api :as api]
            [cook.kubernetes.controller :as controller]
            [cook.kubernetes.metrics :as metrics]
            [cook.log-structured :as log-structured]
            [cook.monitor :as monitor]
            [cook.pool]
            [cook.prometheus-metrics :as prom]
            [cook.scheduler.constraints :as constraints]
            [cook.tools :as tools]
            [cook.util :as util]
            [datomic.api :as d]
            [metrics.counters :as counters]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [opentracing-clj.core :as tracing])
  (:import (com.google.auth.oauth2 GoogleCredentials)
           (com.google.common.util.concurrent ThreadFactoryBuilder)
           (com.twosigma.cook.kubernetes TokenRefreshingAuthenticator ParallelWatchQueue)
           (io.kubernetes.client.openapi ApiClient)
           (io.kubernetes.client.openapi.models V1Node V1Pod)
           (io.kubernetes.client.util ClientBuilder KubeConfig)
           (java.nio.charset StandardCharsets)
           (java.io ByteArrayInputStream File FileInputStream InputStreamReader)
           (java.util.concurrent Executors ExecutorService ScheduledExecutorService TimeUnit)
           (java.util.concurrent.locks ReentrantLock ReentrantReadWriteLock)
           (java.util Base64 UUID)
           (okhttp3 Protocol)))

; Define a consistent value for the :component tags in opentracing spans
(def tracing-component-tag "cook.kubernetes")

(defn schedulable-node-filter
  "Is a node schedulable?"
  [compute-cluster node-name->node node-name->pods [node-name _]]
  (if-let [^V1Node node (node-name->node node-name)]
    (api/node-schedulable? compute-cluster node (cc/max-tasks-per-host compute-cluster) node-name->pods)
    (do
      (log-structured/error "Unable to get node from node name"
                            {:compute-cluster (cc/compute-cluster-name compute-cluster)
                             :node node-name})
      false)))

(defn total-resource
  "Given a map from node-name->resource-keyword->amount and a resource-keyword,
  returns the total amount of that resource for all nodes."
  [node-name->resource-map resource-keyword]
  (->> node-name->resource-map vals (map resource-keyword) (filter some?) (reduce +)))

(defn total-map-resource
  "Given a map from node-name->resource-keyword->amount,
  returns a map from model/type to count for all nodes."
  [node-name->resource-map resource-keyword]
  (->> node-name->resource-map vals (map resource-keyword) (apply merge-with +)))

(defn generate-offers
  "Given a compute cluster and maps with node capacity and existing pods, return a map from pool to offers."
  [compute-cluster node-name->node node-name->pods pool-name]
  (let [clobber-synthetic-pods (:clobber-synthetic-pods (config/kubernetes))
        compute-cluster-name (cc/compute-cluster-name compute-cluster)
        node-name->capacity (api/get-capacity node-name->node pool-name)
        ; node-name->node map, used to calculate node-name->capacity includes nodes from the one pool,
        ; gotten via the pools->node-name->node map.
        ;
        ; node-name->pods used to calculate node-name->consumption can include starting pods from all pools as it
        ; unions starting pods across all nodes and all pods in the pool (made via composing
        ; node-name->pod-name->pod map and the pool->node-name->node map)
        ;
        ; node-name->available can include extra nodes that are wrong-pool. However, when those wrong-pool nodes
        ; are passed to node-schedulable? they're not found when it looks at the node-name->node map, so it logs an ERROR.
        ;
        ; If we have consumption calculation on a node that doesn't have a capacity calculated, there's not not much
        ; point in further computation on it. We won't make an offer in any case. So we filter them out.
        ; This also cleanly avoids the logged ERROR.
        node-name->consumed (->> (api/get-consumption clobber-synthetic-pods node-name->pods pool-name)
                                 (filter #(node-name->capacity (first %)))
                                 (into {}))
        node-name->available (util/deep-merge-with - node-name->capacity node-name->consumed)
        ; Grab every unique GPU model being represented so that we can set counters for capacity and consumed for each GPU model
        gpu-models (->> node-name->capacity vals (map :gpus) (apply merge) keys set)
        ; The following variables are only being used setting counters for monitor
        gpu-model->total-capacity (total-map-resource node-name->capacity :gpus)
        gpu-model->total-consumed (total-map-resource node-name->consumed :gpus)

        ; Grab every unique disk type being represented to set counters for capacity and consumed for each disk type
        disk-types (->> node-name->capacity vals (map :disk) (apply merge) keys set)
        ; The following disk variables are only being used to set counters for monitor
        disk-type->total-capacity (total-map-resource node-name->capacity :disk)
        disk-type->total-consumed (total-map-resource node-name->consumed :disk)

        node-name->schedulable (filter #(schedulable-node-filter compute-cluster
                                                                 node-name->node
                                                                 node-name->pods
                                                                 %)
                                       node-name->available)
        number-nodes-schedulable (count node-name->schedulable)
        number-nodes-total (count node-name->node)
        total-cpus-capacity (total-resource node-name->capacity :cpus)
        total-mem-capacity (total-resource node-name->capacity :mem)
        total-cpus-consumption (total-resource node-name->consumed :cpus)
        total-mem-consumption (total-resource node-name->consumed :mem)]

    (log-structured/info "Generating offers"
                         {:compute-cluster compute-cluster-name
                          :pool pool-name
                          :number-nodes-not-schedulable (- number-nodes-total number-nodes-schedulable)
                          :number-nodes-schedulable number-nodes-schedulable
                          :number-nodes-total number-nodes-total
                          :first-ten-capacity (print-str (take 10 node-name->capacity))
                          :first-ten-consumed (print-str (take 10 node-name->consumed))})

    (prom/set prom/resource-capacity {:compute-cluster compute-cluster-name :pool pool-name :resource "nodes"} number-nodes-total)
    (prom/set prom/resource-capacity {:compute-cluster compute-cluster-name :pool pool-name :resource "cpu"} total-cpus-capacity)
    (prom/set prom/resource-capacity {:compute-cluster compute-cluster-name :pool pool-name :resource "mem"} total-mem-capacity)
    (prom/set prom/resource-consumption {:compute-cluster compute-cluster-name :resource "cpu"} total-cpus-consumption)
    (prom/set prom/resource-consumption {:compute-cluster compute-cluster-name :resource "mem"} total-mem-consumption)
    (monitor/set-counter! (metrics/counter "capacity-cpus" compute-cluster-name) total-cpus-capacity)
    (monitor/set-counter! (metrics/counter "capacity-mem" compute-cluster-name) total-mem-capacity)
    (monitor/set-counter! (metrics/counter "consumption-cpus" compute-cluster-name) total-cpus-consumption)
    (monitor/set-counter! (metrics/counter "consumption-mem" compute-cluster-name) total-mem-consumption)

    (doseq [gpu-model gpu-models]
      (prom/set prom/resource-capacity
                {:compute-cluster compute-cluster-name :pool pool-name :resource "gpu" :resource-subtype gpu-model}
                (get gpu-model->total-capacity gpu-model))
      (monitor/set-counter! (metrics/counter (str "capacity-gpu-" gpu-model) compute-cluster-name)
                            (get gpu-model->total-capacity gpu-model))
      (prom/set prom/resource-consumption
                {:compute-cluster compute-cluster-name :resource "gpu" :resource-subtype gpu-model}
                (get gpu-model->total-consumed gpu-model 0))
      (monitor/set-counter! (metrics/counter (str "consumption-gpu-" gpu-model) compute-cluster-name)
                            (get gpu-model->total-consumed gpu-model 0)))
    (doseq [disk-type disk-types]
      (prom/set prom/resource-capacity
                {:compute-cluster compute-cluster-name :pool pool-name :resource "disk" :resource-subtype disk-type}
                (get disk-type->total-capacity disk-type))
      (monitor/set-counter! (metrics/counter (str "capacity-disk-" disk-type) compute-cluster-name)
                            (get disk-type->total-capacity disk-type))
      (prom/set prom/resource-consumption
                {:compute-cluster compute-cluster-name :resource "disk" :resource-subtype disk-type}
                (get disk-type->total-consumed disk-type 0))
      (monitor/set-counter! (metrics/counter (str "consumption-disk-" disk-type) compute-cluster-name)
                            (get disk-type->total-consumed disk-type 0)))


    (->> node-name->schedulable
         (map (fn [[node-name available]]
                (let [node-label-attributes
                      ; Convert all node labels to offer
                      ; attributes so that job constraints
                      ; can use them in the matching process
                      (or
                        (some->> node-name
                                 ^V1Node (get node-name->node)
                                 .getMetadata
                                 .getLabels
                                 (map (fn [[key value]]
                                        {:name key
                                         :type :value-text
                                         :text value})))
                        [])]
                  {:id {:value (str (UUID/randomUUID))}
                   :framework-id compute-cluster-name
                   :slave-id {:value node-name}
                   :hostname node-name
                   :resources [{:name "mem" :type :value-scalar :scalar (max 0.0 (:mem available))}
                               {:name "cpus" :type :value-scalar :scalar (max 0.0 (:cpus available))}
                               {:name "disk" :type :value-text->scalar :text->scalar (:disk available {})}
                               {:name "gpus" :type :value-text->scalar :text->scalar (:gpus available {})}]
                   :attributes (conj node-label-attributes
                                     {:name "compute-cluster-type"
                                      :type :value-text
                                      :text "kubernetes"})
                   :executor-ids []
                   :compute-cluster compute-cluster
                   :reject-after-match-attempt true
                   :offer-match-timer (timers/start (ccmetrics/timer "offer-match-timer" compute-cluster-name))
                   :offer-match-timer-prom-stop-fn (prom/start-timer prom/offer-match-timer {:compute-cluster compute-cluster-name})}))))))

(defn taskids-to-scan
  "Determine all task ids to scan by union-ing task id's from cook expected state and existing task id maps. "
  [{:keys [cook-expected-state-map k8s-actual-state-map] :as kcc}]
  (->>
    (set/union (keys @cook-expected-state-map) (keys @k8s-actual-state-map))
    (into #{})))

(defn scan-tasks
  "Scan all taskids. Note: May block or be slow due to rate limits."
  [{:keys [name] :as kcc}]
  (log-structured/info "Starting taskid scan" {:compute-cluster name})
  ; TODO Add in rate limits; only visit non-running/running task so fast.
  ; TODO Add in maximum-visit frequency. Only visit a task once every XX seconds.
  (let [taskids (taskids-to-scan kcc)]
    (log-structured/info "Doing taskid scan"
                         {:compute-cluster name
                          :number-tasks (count taskids)})
    (doseq [^String taskid taskids]
      (try
        (controller/scan-process kcc taskid)
        (catch Exception e
          (log-structured/error "Encountered exception scanning task"
                                {:compute-cluster name
                                 :task-id taskid}
                                e))))
    (log-structured/info "Done with taskid scan"
                         {:compute-cluster name})))

(defn regular-scanner
  "Trigger a channel that scans all taskids (shortly) after this function is invoked and on a regular interval."
  [kcc interval]
  (let [ch (async/chan (async/sliding-buffer 1))]
    ; We scan on startup as we load the existing taskid in, so first scan should be scheduled for a while in the future.
    (async/pipe (chime-ch (periodic-seq (-> interval time/from-now) interval)) ch)
    (tools/chime-at-ch ch
                       (fn scan-taskids-function []
                         (scan-tasks kcc))
                       {:error-handler (fn [e]
                                         (log/error e "Scan taskids failed"))})))

(defn make-cook-pod-watch-callback
  "Make a callback function that is passed to the pod-watch callback. This callback forwards changes to the cook.kubernetes.controller."
  [{:keys [^ParallelWatchQueue parallel-watch-queue] :as kcc}]
  (fn pod-watch-callback
    [_ ^V1Pod prev-pod ^V1Pod pod]
    (try
      (let [name (or (some-> prev-pod .getMetadata .getName)
                     (some-> pod .getMetadata .getName))
            shardNum (mod (.hashCode name) (.getShardCount parallel-watch-queue))
            ^Runnable event (fn []
                              (try
                                (if (nil? pod)
                                  (controller/pod-deleted kcc prev-pod)
                                  (controller/pod-update kcc pod))
                                (catch Exception e
                                  (log/error e "Error processing status update on" name))))]
        (.submitEvent parallel-watch-queue event shardNum))
      (catch Exception e
        (log/error e "Error submitting pod status update")))))

(defn task-ents->map-by-task-id
  "Given seq of task entities from datomic, generate a map of task-id -> entity."
  [task-ents]
  (->> task-ents
       (map (fn [task-ent] [(str (:instance/task-id task-ent)) task-ent]))
       (into {})))

(defn task-ent->cook-expected-state
  "When we startup, we need to initialize the cook expected state from datomic. This implements a map from datomic's :instance.status/*
  to the cook expected state."
  [task-ent]
  (case (:instance/status task-ent)
    :instance.status/unknown {:cook-expected-state :cook-expected-state/starting}
    :instance.status/running {:cook-expected-state :cook-expected-state/running}
    :instance.status/failed {:cook-expected-state :cook-expected-state/completed}
    :instance.status/success {:cook-expected-state :cook-expected-state/completed}))

(defn determine-cook-expected-state-on-startup
  "We need to determine everything we should be tracking when we construct the cook expected state. We should be tracking
  all tasks that are in the running state as well as all pods in kubernetes. We query for a list of
  all running tasks entities."
  [conn api-client compute-cluster-name]
  (let [db (d/db conn)
        [_ pod-name->pod] (api/try-forever-get-all-pods-in-kubernetes api-client compute-cluster-name)
        all-tasks-ids-in-pods (into #{} (keys pod-name->pod))
        _ (log-structured/debug (print-str "All tasks in pods (for initializing cook expected state): " all-tasks-ids-in-pods)
                                {:compute-cluster compute-cluster-name})
        running-tasks-in-cc-ents (map #(d/entity db %) (cc/get-job-instance-ids-for-cluster-name db compute-cluster-name))
        running-task-id->task (task-ents->map-by-task-id running-tasks-in-cc-ents)
        cc-running-tasks-ids (->> running-task-id->task keys (into #{}))
        _ (log-structured/debug (print-str "Running tasks in compute cluster in datomic: " cc-running-tasks-ids)
                                {:compute-cluster compute-cluster-name})
        ; We already have task entities for everything running, in datomic.
        ; Now figure out what pods kubernetes has that aren't in that set, and then load those task entities too.
        extra-tasks-id->task (->> (set/difference all-tasks-ids-in-pods cc-running-tasks-ids)
                                  (map (fn [task-id] [task-id (cook.tools/retrieve-instance db task-id)]))
                                  ; TODO: this filter shouldn't be here. We should be pre-filtering pods
                                  ; to be cook pods. Then remove this filter as we should kill off anything
                                  ; unknown.
                                  (filter (fn [[_ task-ent]] (some? task-ent)))
                                  (into {}))
        all-task-id->task (merge extra-tasks-id->task running-task-id->task)]
    (log-structured/info (print-str "Initialized tasks on startup:"
                                    (count all-tasks-ids-in-pods) "tasks in pods and"
                                    (count running-task-id->task) "running tasks in datomic. "
                                    "We need to load an extra" (count extra-tasks-id->task) "pods that aren't running in datomic. "
                                    "For a total expected state size of" (count all-task-id->task) "tasks in cook expected state.")
                         {:compute-cluster compute-cluster-name})
    (doseq [[k v] all-task-id->task]
      (log-structured/debug (print-str "Setting cook expected state for " k " ---> " (task-ent->cook-expected-state v))
                            {:compute-cluster compute-cluster-name}))
    (into {}
          (map (fn [[k v]] [k (task-ent->cook-expected-state v)]) all-task-id->task))))

(defn- get-namespace-from-task-metadata
  [{:keys [kind namespace]} task-metadata]
  (case kind
    :static namespace
    :per-user (-> task-metadata
                  :command
                  :user)))

(defn add-starting-pods
  "Given a compute cluster and a map from namespaced pod name -> pod, returns a
  list of those pods with currently starting pods in the compute cluster added in"
  [compute-cluster pods]
  (let [starting-pods (controller/starting-namespaced-pod-name->pod compute-cluster)]
    (vals (into pods starting-pods))))

(defn add-starting-pods-reverse
  "Weird offshoot of add-starting-pods-reverse that is run when determining node capacity.
  (We want to add the smaller set onto the bigger set. In this context, the bigger set is
  the set of starting pods.)"
  [compute-cluster pods]
  (let [starting-pods (controller/starting-namespaced-pod-name->pod compute-cluster)]
    (vals (into starting-pods pods))))

(defn synthetic-pod->job-uuid
  "If the given pod is a synthetic pod for autoscaling, returns the job uuid
  that the pod corresponds to (stored in a pod label). Otherwise, returns nil."
  [^V1Pod pod]
  (some-> pod .getMetadata .getLabels (.get api/cook-synthetic-pod-job-uuid-label)))

(defn- launch-task!
  "Given a compute cluster and a single task-metadata,
  launches the task by pumping it into the k8s state machine"
  [{:keys [name namespace-config] :as compute-cluster} task-metadata]
  (let [timer-context (timers/start (metrics/timer "cc-launch-tasks" name))
        prom-stop-fn (prom/start-timer prom/launch-task-duration {:compute-cluster name})
        pod-namespace (get-namespace-from-task-metadata namespace-config task-metadata)
        pod-name (:task-id task-metadata)
        ^V1Pod pod (api/task-metadata->pod pod-namespace compute-cluster task-metadata)
        new-cook-expected-state-dict {:cook-expected-state :cook-expected-state/starting
                                      :launch-pod {:pod pod}}
        {:keys [job/uuid] :as job-map} (get-in task-metadata [:task-request :job])
        job-uuid (str uuid)]
    ; If a pod is not synthetic, cache a mapping of its instance-uuid to job-uuid in order to give all state machine passport events access to job-uuid
    (when-not (api/synthetic-pod? pod-name)
      (cache/put-cache! caches/instance-uuid->job-uuid identity pod-name job-uuid))
    ; Cache job-uuid->job-map for use when doing passport logging
    (cache/put-cache! caches/job-uuid->job-map identity job-uuid job-map)
    (try
      (controller/update-cook-expected-state compute-cluster pod-name new-cook-expected-state-dict)
      (.stop timer-context)
      (prom-stop-fn)
      (catch Exception e
        (log-structured/error "Encountered exception launching task"
                              {:pod-name pod-name
                               :pod-namespace pod-namespace
                               :task-metadata (print-str task-metadata)
                               :compute-cluster name}
                              e)))))

(defn get-name->node-for-pool
  "Given a pool->node-name->node and pool, return a node-name->pool, for any pool"
  [pool->node-name->node pool]
  (get pool->node-name->node pool {}))

(defn get-pods-in-pool
  "Given a compute cluster and a pool name, extract the pods that are in the current pool only."
  [{:keys [pool->node-name->node node-name->pod-name->pod]} pool]
  (->> (get-name->node-for-pool @pool->node-name->node pool)
       keys
       (map #(get @node-name->pod-name->pod % {}))
       (reduce into {})))

(defn get-outstanding-synthetic-pods
  "Get the current synthetic pods in a cluster."
  [compute-cluster pool-name]
  (->> (get-pods-in-pool compute-cluster pool-name)
       (add-starting-pods compute-cluster)
       (filter synthetic-pod->job-uuid)))

(defn set-synthetic-pods-counters-helper
  "Helper for setting the synthetic pod counters."
  ; This is the "external call" case; we need a synthetic-pods-config to extract the max-pods-outstanding metric,
  ; and we calculate the current number of synthetic pods explicitly.
  ([compute-cluster pool-name synthetic-pods-config]
   (let [num-synthetic-pods (count (get-outstanding-synthetic-pods compute-cluster pool-name))
         {:keys [max-pods-outstanding]} synthetic-pods-config]
     (set-synthetic-pods-counters-helper compute-cluster pool-name num-synthetic-pods max-pods-outstanding)))
  ; This is the "internal" case, in which we already have the number of synthetic pods and the max-pods-outstanding,
  ; so we just set the values as they are given to us.
  ([compute-cluster pool-name num-synthetic-pods max-pods-outstanding]
   (let [name (cc/compute-cluster-name compute-cluster)]
     (monitor/set-counter!
       (counters/counter ["cook-k8s" "total-synthetic-pods" (str "compute-cluster-" name) (str "pool-" pool-name)])
       num-synthetic-pods)
     (monitor/set-counter!
       (counters/counter ["cook-k8s" "max-total-synthetic-pods" (str "compute-cluster-" name) (str "pool-" pool-name)])
       max-pods-outstanding)
     (prom/set prom/total-synthetic-pods {:pool pool-name :compute-cluster name} num-synthetic-pods)
     (prom/set prom/max-synthetic-pods {:pool pool-name :compute-cluster name} max-pods-outstanding))))

(defn get-scheduling-pods
  "Get the pods that are still being scheduled by Kubernetes. This is 
   done by filtering pods not yet bound to a node from the set of 
   pods Cook thinks is running.

   This is particularly useful when utilizing the Kubernetes Scheduler
   to bin pack jobs. Pressure inside the scheduler is measured and then
   reduced by only having so many outstanding, unscheduled pods at any 
   given time.

   Filtering for node assignments is the best approximation of this 
   pressure. Depending on pods with 'Pending' status is problematic. 
   It includes time spent pulling images onto nodes, after they have 
   been assigned by the scheduler. This status does not accurately 
   reflect the pressure inside the Kubernetes Scheduler."
  [compute-cluster pool-name]
  (->> (get-pods-in-pool compute-cluster pool-name)
       (add-starting-pods compute-cluster)
       (remove api/pod->node-name)))

(defrecord KubernetesComputeCluster [^ApiClient api-client name entity-id exit-code-syncer-state
                                     all-pods-atom current-nodes-atom pool->node-name->node
                                     node-name->pod-name->pod cook-expected-state-map cook-starting-pods k8s-actual-state-map
                                     pool-name->fenzo-state-atom namespace-config scan-frequency-seconds-config max-pods-per-node
                                     synthetic-pods-config node-blocklist-labels
                                     ^ExecutorService controller-executor-service
                                     cluster-definition state-atom state-locked?-atom dynamic-cluster-config?
                                     compute-cluster-launch-rate-limiter cook-pool-taint-name cook-pool-taint-prefix
                                     cook-pool-taint2-name cook-pool-taint2-value
                                     cook-pool-label-name cook-pool-label-prefix
                                     controller-lock-objects kill-lock-object
                                     parallel-watch-queue]
  cc/ComputeCluster
  (launch-tasks [this pool-name matches process-task-post-launch-fn]
    (let [task-metadata-seq (mapcat :task-metadata-seq matches)]
      (tracing/with-span [s {:name "k8s.launch-tasks" :tags
                             {:pool pool-name :compute-cluster name :number-tasks (count task-metadata-seq)}}]
        (log-structured/info "Launching tasks"
                             {:pool pool-name
                              :compute-cluster name
                              :number-matches (count matches)
                              :number-tasks (count task-metadata-seq)})
        (let [futures
              (doall
                (map (fn [task-metadata]
                       (.submit
                         controller-executor-service
                         ^Callable (fn []
                                     (tracing/with-span [s {:name "k8s.launch-tasks.launch-task"
                                                            :tags {:pool pool-name :compute-cluster name
                                                                   :component tracing-component-tag}}]
                                       (launch-task! this task-metadata))
                                     (tracing/with-span [s {:name "k8s.launch-tasks.process-task-post-launch-fn"
                                                            :tags {:pool pool-name :compute-cluster name
                                                                   :component tracing-component-tag}}]
                                      (process-task-post-launch-fn task-metadata)))))
                     task-metadata-seq))]
          (run! deref futures)))))

  (kill-task [this task-id]
    (let [state @state-atom]
      (if (= state :deleted)
        (log-structured/error (print-str "Attempting to delete task, but the current cluster state is :deleted."
                                         "Can't perform any client API calls when the cluster has been deleted."
                                         "Will not attempt to kill task.")
                              {:compute-cluster name :task-id task-id})
        ; Note we can't use timer/time! because it wraps the body in a Callable, which rebinds 'this' to another 'this'
        ; causing breakage.
        (let [timer-context (timers/start (metrics/timer "cc-kill-tasks" name))
              prom-stop-fn (prom/start-timer prom/kill-task-duration {:compute-cluster name})]
          (controller/update-cook-expected-state this task-id {:cook-expected-state :cook-expected-state/killed})
          (.stop timer-context)
          (prom-stop-fn)))))

  (decline-offers [this offer-ids]
    (log/debug "Rejecting offer ids" offer-ids))

  (db-id [this]
    entity-id)

  (compute-cluster-name [this]
    name)

  (initialize-cluster [this pool-name->fenzo-state]
    ; We may iterate forever trying to bring up kubernetes. However, our caller expects us to eventually return,
    ; so we launch within a future so that our caller can continue initializing other clusters.
    (future
      (try
        (log-structured/info "Initializing Kubernetes compute cluster" {:compute-cluster name})

        ; We need to reset! the pool-name->fenzo-state atom before initializing the
        ; watches, because otherwise, we will start to see and handle events
        ; for pods, but we won't be able to update the corresponding Fenzo
        ; instance (e.g. unassigning a completed task from its host)
        (reset! pool-name->fenzo-state-atom pool-name->fenzo-state)

        ; Initialize the pod watch path.
        (let [conn cook.datomic/conn
              cook-pod-callback (make-cook-pod-watch-callback this)]
          ; We set cook expected state first because initialize-pod-watch sets (and invokes callbacks on and reacts to) the
          ; expected and the gradually discovered existing pods.
          (reset! cook-expected-state-map (determine-cook-expected-state-on-startup conn api-client name))

          (api/initialize-pod-watch this cook-pod-callback)
          (if scan-frequency-seconds-config
            (regular-scanner this (time/seconds scan-frequency-seconds-config))
            (log/info "State scan disabled because no interval has been set")))

        ; Initialize the node watch path.
        (api/initialize-node-watch this)

        (api/initialize-event-watch this)
        (catch Throwable e
          (log-structured/error "Failed to bring up compute cluster" {:compute-cluster name} e)
          (throw e))))

    ; We keep leadership indefinitely in kubernetes.
    (async/chan 1))

  (pending-offers [this pool-name]
    (tracing/with-span [s {:name "k8s.pending-offers" :tags {:pool pool-name :component tracing-component-tag}}]
      (let [state @state-atom]
        (if-not (= state :running)
          (log-structured/info "Skipping generating offers for pool because the current state is not :running."
                               {:compute-cluster name :pool pool-name :state state})
          (let [node-name->node (get @pool->node-name->node pool-name)]
            (if-not (or (cc/autoscaling? this pool-name) node-name->node)
              (log-structured/info "Not looking for offers for pool; it doesn't autoscale for, and has 0 nodes tainted"
                                   {:compute-cluster name :pool pool-name})
              (do
                (log-structured/info "Looking for offers for pool" {:compute-cluster name :pool pool-name})
                (let [timer (timers/start (metrics/timer "cc-pending-offers-compute" name))
                      prom-stop-fn (prom/start-timer prom/compute-pending-offers-duration {:compute-cluster name})
                      pods (add-starting-pods this @all-pods-atom)
                      nodes @current-nodes-atom
                      offers-this-pool (generate-offers this (or node-name->node {})
                                                        (->> (get-pods-in-pool this pool-name)
                                                             (add-starting-pods this)
                                                             (api/pods->node-name->pods))
                                                        pool-name)
                      offers-this-pool-for-logging
                      (->> offers-this-pool
                           (take 10)
                           tools/offers->resource-maps
                           (map util/format-resource-map))]
                  (log-structured/info "Generated offers for pool"
                                       {:compute-cluster name
                                        :pool pool-name
                                        :number-offers-this-pool (count offers-this-pool)
                                        :number-total-nodes-in-compute-cluster (count nodes)
                                        :number-total-pods-in-compute-cluster (count pods)
                                        :first-ten-offers-this-pool (print-str offers-this-pool-for-logging)})
                  (timers/stop timer)
                  (prom-stop-fn)
                  offers-this-pool))))))))

  (restore-offers [this pool-name offers])

  (set-synthetic-pods-counters [this pool-name]
    (set-synthetic-pods-counters-helper this pool-name synthetic-pods-config))

  (autoscaling? [_ pool-name]
    (and (-> synthetic-pods-config :pools (contains? pool-name))
         (= @state-atom :running)))
  
  (max-launchable
   [this pool-name]
   (let [scheduling-pods (get-scheduling-pods this pool-name)
         num-scheduling-pods (count scheduling-pods)
         total-pods (-> @all-pods-atom keys count)
         total-nodes (-> @current-nodes-atom keys count)
         ; NOTE: the synthetic pods config is co-opted here to determine the max outstanding
         ; pods in the compute cluster. This configuration, set on the compute cluster
         ; template, will be refactored in the future to more generally define autoscaling 
         ; parameters once synthetic pods are decommissioned.
         {:keys [max-pods-outstanding max-total-pods max-total-nodes]} synthetic-pods-config]

     (when (>= total-pods max-total-pods)
       (log-structured/warn "Total pods are maxed out"
                            {:compute-cluster name
                             :number-max-total-pods max-total-pods
                             :number-total-pods total-pods
                             :pool-name pool-name}))
     (when (>= total-nodes max-total-nodes)
       (log-structured/warn "Nodes are maxed out"
                            {:compute-cluster name
                             :number-max-total-nodes max-total-nodes
                             :number-total-nodes total-nodes
                             :pool-name pool-name}))
     (when (>= num-scheduling-pods max-pods-outstanding)
       (log-structured/warn "Outstanding pods are maxed out"
                            {:compute-cluster name
                             :number-max-synthetic-pods max-pods-outstanding
                             :number-outstanding-pods num-scheduling-pods
                             :pool-name pool-name}))

     (min (- max-pods-outstanding num-scheduling-pods)
          (- max-total-nodes total-nodes)
          (- max-total-pods total-pods))))

  (autoscale! [this pool-name jobs adjust-job-resources-for-pool-fn]
    (tracing/with-span [s {:name "k8s.autoscale" :tags {:compute-cluster name :pool pool-name :component tracing-component-tag}}]
      (if-not (cc/autoscaling? this pool-name)
        (log-structured/warn (print-str "Ignoring request to autoscale in pool because autoscaling? is now false."
                                        "This should almost never happen. But might benignly happen because of a race." {:state @state-atom})
                             {:compute-cluster name :pool pool-name})
        (try
          (assert (cc/autoscaling? this pool-name)
                  (str "In " name " compute cluster, request to autoscale despite invalid / missing config"))
          (let [timer-context-autoscale (timers/start (metrics/timer "cc-synthetic-pod-autoscale" name))
                prom-stop-fn (prom/start-timer prom/autoscale-duration {:compute-cluster name :pool pool-name})
                outstanding-synthetic-pods (get-outstanding-synthetic-pods this pool-name)
                num-synthetic-pods (count outstanding-synthetic-pods)
                total-pods (-> @all-pods-atom keys count)
                total-nodes (-> @current-nodes-atom keys count)
                {:keys [image user command max-pods-outstanding max-total-pods max-total-nodes]
                 :or {command "exit 0" max-total-pods 32000 max-total-nodes 1000}} synthetic-pods-config]

            (when (>= total-pods max-total-pods)
              (log-structured/warn "Total pods are maxed out"
                                   {:compute-cluster name
                                    :number-max-total-pods max-total-pods
                                    :number-total-pods total-pods}))
            (when (>= total-nodes max-total-nodes)
              (log-structured/warn "Nodes are maxed out"
                                   {:compute-cluster name
                                    :number-max-total-nodes max-total-nodes
                                    :number-total-nodes total-nodes}))
            (when (>= num-synthetic-pods max-pods-outstanding)
              (log-structured/warn "Synthetic pods are maxed out"
                                   {:compute-cluster name
                                    :number-max-synthetic-pods max-pods-outstanding
                                    :number-synthetic-pods num-synthetic-pods}))

            (set-synthetic-pods-counters-helper this pool-name num-synthetic-pods max-pods-outstanding)

            (let [max-launchable (min (- max-pods-outstanding num-synthetic-pods)
                                      (- max-total-nodes total-nodes)
                                      (- max-total-pods total-pods))]
              (if (not (pos? max-launchable))
                (log-structured/warn "Cannot launch more synthetic pods"
                                     {:compute-cluster name
                                      :number-max-synthetic-pods max-pods-outstanding
                                      :number-max-total-nodes max-total-nodes
                                      :number-max-total-pods max-total-pods
                                      :number-synthetic-pods num-synthetic-pods
                                      :number-total-nodes total-nodes
                                      :number-total-pods total-pods})
                (let [using-pools? (config/default-pool)
                      synthetic-task-pool-name (when using-pools? pool-name)
                      new-jobs (remove (fn [{:keys [job/uuid]}]
                                         (some #(= (str uuid) (synthetic-pod->job-uuid %))
                                               outstanding-synthetic-pods))
                                       jobs)
                      user-from-synthetic-pods-config user
                      task-metadata-seq
                      (->> new-jobs
                           (take max-launchable)
                           (map (fn [{:keys [job/name job/user job/uuid job/environment] :as job}]
                                  (let [pool-specific-resources
                                        ((adjust-job-resources-for-pool-fn pool-name) job (tools/job-ent->resources job))]
                                    (.put cook.caches/recent-synthetic-pod-job-uuids uuid uuid)
                                    {:command {:user (or user-from-synthetic-pods-config user)
                                               :value command}
                                     :container {:docker {:image image}}
                                     ; We need to *not* prevent the cluster autoscaler from
                                     ; removing a node just because it's running synthetic pods
                                     :pod-annotations {api/k8s-safe-to-evict-annotation "true"}
                                     ; Job constraints need to be expressed on synthetic
                                     ; pods so that we trigger the cluster autoscaler to
                                     ; spin up nodes that will end up satisfying them
                                     :pod-constraints (constraints/job->constraints job)
                                     ; Cook has a "novel host constraint", which disallows a job from
                                     ; running on the same host twice. So, we need to avoid running a
                                     ; synthetic pod on any of the hosts that the real job won't be able
                                     ; to run on. Otherwise, the synthetic pod won't trigger the cluster
                                     ; autoscaler.
                                     :pod-hostnames-to-avoid (constraints/job->previous-hosts-to-avoid job)
                                     ; We need to label the synthetic pods so that we
                                     ; can opt them out of some of the normal plumbing,
                                     ; like mapping status back to a job instance. We
                                     ; also want to label the workload as infrastructure
                                     ; and associate the user as the resource owner.
                                     :pod-labels {api/cook-synthetic-pod-job-uuid-label (str uuid)
                                                  (api/workload-class-label) "infrastructure"
                                                  (api/workload-id-label) "synthetic-pod"
                                                  api/resource-owner-label user}
                                     ; We need to give synthetic pods a lower priority than
                                     ; actual job pods so that the job pods can preempt them
                                     ; (https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/);
                                     ; if we don't do this, we run the risk of job pods
                                     ; encountering failures when they lose scheduling races
                                     ; against pending synthetic pods
                                     :pod-priority-class api/cook-synthetic-pod-priority-class
                                     ; We don't want to add in the cook-init cruft or the cook sidecar, because we
                                     ; don't need them for synthetic pods and all they will do is slow things down.
                                     :pod-supports-cook-init? false
                                     :pod-supports-cook-sidecar? false
                                     :task-id (str api/cook-synthetic-pod-name-prefix "-" pool-name "-" uuid)
                                     :task-request {:scalar-requests (walk/stringify-keys pool-specific-resources)
                                                    :job {:job/pool {:pool/name synthetic-task-pool-name}
                                                          :job/environment environment
                                                          :job/name name
                                                          :job/user user
                                                          :job/uuid uuid}
                                                    ; Need to pass in resources to task-metadata->pod for gpu count
                                                    :resources pool-specific-resources}}))))
                      num-synthetic-pods-to-launch (count task-metadata-seq)]
                  (prom/set prom/synthetic-pods-submitted {:compute-cluster name :pool pool-name} num-synthetic-pods-to-launch)
                  (meters/mark! (metrics/meter "cc-synthetic-pod-submit-rate" name) num-synthetic-pods-to-launch)
                  (log-structured/info "Launching synthetic pod(s) in pool"
                                       {:compute-cluster name
                                        :pool synthetic-task-pool-name
                                        :number-synthetic-pods-to-launch num-synthetic-pods-to-launch})
                  (let [timer-context-launch-tasks (timers/start (metrics/timer "cc-synthetic-pod-launch-tasks" name))
                        prom-stop-fn (prom/start-timer prom/launch-synthetic-tasks-duration {:compute-cluster name :pool pool-name})]
                    (cc/launch-tasks this
                                     synthetic-task-pool-name
                                     [{:task-metadata-seq task-metadata-seq}]
                                     (fn [_]))
                    (.stop timer-context-launch-tasks)
                    (prom-stop-fn)))))
            (.stop timer-context-autoscale)
            (prom-stop-fn))
          (catch Throwable e
            (log-structured/error "Encountered error launching synthetic pod(s)" {:compute-cluster name :pool pool-name} e))))))

  (use-cook-executor? [_] false)

  (container-defaults [_]
    ; We don't currently support specifying
    ; container defaults for k8s compute clusters
    {})

  (max-tasks-per-host [_] max-pods-per-node)

  (num-tasks-on-host [this hostname]
    (->> (get @node-name->pod-name->pod hostname {})
         (add-starting-pods-reverse this)
         (api/num-pods-on-node hostname)))

  (retrieve-sandbox-url-path
    ;; Constructs a URL to query the sandbox directory of the task.
    ;; Users will need to add the file path & offset to their query.
    ;; Refer to the 'Using the output_url' section in docs/scheduler-rest-api.adoc for further details.
    [_ {:keys [instance/sandbox-url]}]
    sandbox-url)

  (launch-rate-limiter
    [_] compute-cluster-launch-rate-limiter)

  (kill-lock-object [_] kill-lock-object))

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

(defn get-bearer-token
  "Takes a GoogleCredentials object and refreshes the credentials, and returns a bearer token suitable for use
   in an Authorization header."
  [^GoogleCredentials scoped-credentials]
  (.refresh scoped-credentials)
  (str "Bearer " (.getTokenValue (.getAccessToken scoped-credentials))))

(def ^ScheduledExecutorService bearer-token-executor (Executors/newSingleThreadScheduledExecutor
                                                       (-> (ThreadFactoryBuilder.)
                                                           (.setNameFormat "Cook's bearer-token-executor %d")
                                                           .build)))

(defn make-bearer-token-refresh-task
  "Returns a Runnable which uses scoped-credentials to generate a bearer token and sets it on the api-client"
  [api-client scoped-credentials]
  (reify
    Runnable
    (run [_]
      (try
        (let [bearer-token (get-bearer-token scoped-credentials)]
          (.setApiKey api-client bearer-token))
        (catch Exception ex
          (log/error ex "Error refreshing bearer token"))))))

(defn- set-credentials
  "Set credentials for api client"
  [api-client credentials bearer-token-refresh-seconds]
  (let [scoped-credentials (.createScoped credentials ["https://www.googleapis.com/auth/cloud-platform"
                                                       "https://www.googleapis.com/auth/userinfo.email"])
        bearer-token (get-bearer-token scoped-credentials)]
    (.scheduleAtFixedRate bearer-token-executor
                          (make-bearer-token-refresh-task api-client scoped-credentials)
                          bearer-token-refresh-seconds
                          bearer-token-refresh-seconds
                          TimeUnit/SECONDS)
    (.setApiKey api-client bearer-token)))

(defn make-api-client
  "Builds an ApiClient from the given configuration parameters:
    - If config-file is specified, initializes the api client from the file at config-file
    - If base-path is specified, sets the cluster base path
    - If verifying-ssl is specified, sets verifying ssl
    - If use-google-service-account? is true, gets google application default credentials and generates
      a bearer token for authenticating with kubernetes
    - bearer-token-refresh-seconds: interval to refresh the bearer token
    - If we have a configuration file set, then we can select the context out of that kubeconfig file with kubeconfig-context"
  [^String config-file
   base-path
   ^String use-google-service-account?
   bearer-token-refresh-seconds
   verifying-ssl
   ^String ca-cert
   ^String ca-cert-path
   kubeconfig-context
   read-timeout-seconds
   use-token-refreshing-authenticator?]
  {:pre [(not (and ca-cert ca-cert-path))]}
  (log/info "API Client config file" config-file)
  (let [^ApiClient api-client (if (some? config-file)
                                (let [^KubeConfig kubeconfig
                                      (-> config-file
                                          (FileInputStream.)
                                          (InputStreamReader. (.name (StandardCharsets/UTF_8)))
                                          (KubeConfig/loadKubeConfig))
                                      ; Workaround client library bug. The library attempts to resolve paths
                                      ; against the Kubeconfig filename. We are using an absolute path so we
                                      ; don't need the functionality. But we need to set the file anyways
                                      ; to avoid a NPE.
                                      _ (.setFile kubeconfig (File. config-file))
                                      _ (when kubeconfig-context
                                          (.setContext kubeconfig kubeconfig-context))
                                      ^ClientBuilder clientbuilder (ClientBuilder/kubeconfig kubeconfig)
                                      ; There's an issue where we don't refresh our authenticator. (See comments
                                      ; under TokenRefreshingAuthenticator.) This workaround works when gcloud
                                      ; didn't make the authenticator, so should be disabled in open source
                                      ; and enabled with e.g., iam accounts or others scenarios.
                                      _ (when use-token-refreshing-authenticator?
                                          (.setAuthentication clientbuilder
                                                              ; Refresh after 600 seconds.
                                                              (TokenRefreshingAuthenticator/fromKubeConfig kubeconfig 600)))]
                                  (.build clientbuilder))
                                (ApiClient.))
        customized-http-client (-> api-client
                                   .getHttpClient
                                   .newBuilder
                                   ;; Make watches timeout slower when idle so we lose the watch less
                                   ;; often when the watch is idle.
                                   (.readTimeout
                                     read-timeout-seconds
                                     TimeUnit/SECONDS)
                                   ;; In JDK11, we're getting SocketTimeouts.
                                   ;; https://stackoverflow.com/questions/62031298/sockettimeout-on-java-11-but-not-on-java-8
                                   ;; suggests forcing HTTP_1_1 because HTTP_2 can be flaky.
                                   ;; We can try to remove this when the client is more mature, but as
                                   ;; of okhttp 4.9.1, still buggy.
                                   (.protocols (list Protocol/HTTP_1_1))
                                   .build)
        _ (.setHttpClient api-client customized-http-client)]
    (when base-path
      (.setBasePath api-client base-path))
    (when (some? verifying-ssl)
      (.setVerifyingSsl api-client verifying-ssl))
    ; Loading ssl-cert-path must be last SSL operation we do in setting up API Client. API bug.
    ; See explanation in comments in https://github.com/kubernetes-client/java/pull/200
    (when (some? ca-cert)
      (.setSslCaCert api-client (-> (Base64/getDecoder) (.decode ca-cert) (ByteArrayInputStream.))))
    (when (some? ca-cert-path)
      (.setSslCaCert api-client (FileInputStream. (File. ca-cert-path))))
    (when use-google-service-account?
      (set-credentials api-client (GoogleCredentials/getApplicationDefault) bearer-token-refresh-seconds))
    api-client))

(defn guard-invalid-synthetic-pods-config
  "If the synthetic pods configuration section is present, validates it (throws if invalid)"
  [compute-cluster-name synthetic-pods-config]
  (when synthetic-pods-config
    (when-not (and
               (-> synthetic-pods-config :image count pos?)
               (-> synthetic-pods-config :max-pods-outstanding pos?)
               (-> synthetic-pods-config :max-total-pods pos?)
               (-> synthetic-pods-config :max-total-nodes pos?)
               (-> synthetic-pods-config :pools set?)
               (-> synthetic-pods-config :pools count pos?))
      (throw (ex-info (str "In " compute-cluster-name " compute cluster, invalid synthetic pods config")
                      synthetic-pods-config)))))

(defn factory-fn
  [{:keys [base-path
           bearer-token-refresh-seconds
           ca-cert
           ca-cert-path
           cook-pool-taint-name
           cook-pool-taint-prefix
           cook-pool-taint2-name
           cook-pool-taint2-value
           cook-pool-label-name
           cook-pool-label-prefix
           ^String config-file
           dynamic-cluster-config?
           compute-cluster-launch-rate-limits
           kubeconfig-context
           controller-num-threads
           max-pods-per-node
           name
           namespace
           node-blocklist-labels
           parallel-watch-max-outstanding
           parallel-watch-shards
           read-timeout-seconds
           scan-frequency-seconds
           state
           state-locked?
           synthetic-pods
           use-google-service-account?
           verifying-ssl
           use-token-refreshing-authenticator?]
    :or {bearer-token-refresh-seconds 300
         dynamic-cluster-config? false
         controller-num-threads 8
         max-pods-per-node 32
         namespace {:kind :static
                    :namespace "cook"}
         node-blocklist-labels (list)
         read-timeout-seconds 120
         scan-frequency-seconds 120
         state :running
         state-locked? false
         use-google-service-account? true
         parallel-watch-max-outstanding 1000
         parallel-watch-shards 200
         cook-pool-taint-prefix ""
         cook-pool-label-prefix ""
         use-token-refreshing-authenticator? false}
    :as compute-cluster-config}
   {:keys [exit-code-syncer-state]}]
  (guard-invalid-synthetic-pods-config name synthetic-pods)
  (when (not (< 0 controller-num-threads 512))
    (throw
      (ex-info
        "Please configure :controller-num-threads to > 0 and < 512 in your config."
        compute-cluster-config)))
  (let [conn cook.datomic/conn
        cluster-entity-id (get-or-create-cluster-entity-id conn name)
        api-client (make-api-client config-file
                                    base-path
                                    use-google-service-account?
                                    bearer-token-refresh-seconds
                                    verifying-ssl
                                    ca-cert
                                    ca-cert-path
                                    kubeconfig-context
                                    read-timeout-seconds
                                    use-token-refreshing-authenticator?)
        controller-executor-service (Executors/newFixedThreadPool controller-num-threads
                                                                  (-> (ThreadFactoryBuilder.)
                                                                      (.setNameFormat (str "Cook's controller-executor-for " name " %d"))
                                                                      .build))
        compute-cluster-launch-rate-limiter (cook.rate-limit/create-compute-cluster-launch-rate-limiter name compute-cluster-launch-rate-limits)
        lock-shard-count (:controller-lock-num-shards (config/kubernetes))
        compute-cluster (->KubernetesComputeCluster api-client
                                                    name
                                                    cluster-entity-id
                                                    exit-code-syncer-state
                                                    (atom {})
                                                    (atom {})
                                                    (atom {})
                                                    (atom {})
                                                    (atom {})
                                                    (atom {})
                                                    (atom {})
                                                    (atom nil)
                                                    namespace
                                                    scan-frequency-seconds
                                                    max-pods-per-node
                                                    synthetic-pods
                                                    node-blocklist-labels
                                                    controller-executor-service
                                                    {:factory-fn 'cook.kubernetes.compute-cluster/factory-fn
                                                     :config compute-cluster-config}
                                                    (atom state)
                                                    (atom state-locked?)
                                                    dynamic-cluster-config?
                                                    compute-cluster-launch-rate-limiter
                                                    cook-pool-taint-name cook-pool-taint-prefix
                                                    cook-pool-taint2-name cook-pool-taint2-value
                                                    cook-pool-label-name cook-pool-label-prefix
                                                    ; Vector instead of seq for O(1) access.
                                                    (with-meta (vec (repeatedly lock-shard-count #(ReentrantLock.)))
                                                               {:json-value (str "<count of " lock-shard-count " ReentrantLocks>")})
                                                    ; cluster-level kill-lock. See cc/kill-lock-object
                                                    (ReentrantReadWriteLock. true)
                                                    (ParallelWatchQueue. controller-executor-service parallel-watch-max-outstanding parallel-watch-shards))]
    (cc/register-compute-cluster! compute-cluster)
    compute-cluster))
