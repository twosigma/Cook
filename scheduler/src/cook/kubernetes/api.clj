(ns cook.kubernetes.api
  (:require [clj-time.core :as t]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cook.config :as config]
            [cook.kubernetes.metrics :as metrics]
            [cook.scheduler.constraints :as constraints]
            [cook.task :as task]
            [cook.tools :as util]
            [datomic.api :as d]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [plumbing.core :as pc])
  (:import (com.google.gson JsonSyntaxException)
           (com.twosigma.cook.kubernetes WatchHelper)
           (io.kubernetes.client.custom IntOrString Quantity Quantity$Format)
           (io.kubernetes.client.openapi ApiClient ApiException JSON)
           (io.kubernetes.client.openapi.apis CoreV1Api)
           (io.kubernetes.client.openapi.models
             V1Affinity V1Container V1ContainerPort V1ContainerState V1ContainerStatus V1DeleteOptions V1DeleteOptionsBuilder
             V1EmptyDirVolumeSource V1EnvVar V1Event V1HostPathVolumeSource V1HTTPGetAction V1Node V1NodeAffinity V1NodeSelector
             V1NodeSelectorRequirement V1NodeSelectorTerm V1ObjectMeta V1ObjectReference V1Pod V1PodCondition V1PodSecurityContext
             V1PodSpec V1PodStatus V1Probe V1ResourceRequirements V1Toleration V1Volume V1VolumeBuilder V1VolumeMount)
           (io.kubernetes.client.util Watch)
           (java.net SocketTimeoutException)
           (java.util.concurrent Executors ExecutorService)))


(def cook-pod-label "twosigma.com/cook-scheduler-job")
(def cook-synthetic-pod-job-uuid-label "twosigma.com/cook-scheduler-synthetic-pod-job-uuid")
(def cook-pool-label "cook-pool")
(def cook-pool-taint "cook-pool")
(def cook-sandbox-volume-name "cook-sandbox-volume")
(def cook-job-pod-priority-class "cook-workload")
(def cook-synthetic-pod-priority-class "synthetic-pod")
(def cook-synthetic-pod-name-prefix "synthetic")
(def gpu-node-taint "nvidia.com/gpu")
(def k8s-hostname-label "kubernetes.io/hostname")
; This pod annotation signals to the cluster autoscaler that
; it's safe to remove the node on which the pod is running
(def k8s-safe-to-evict-annotation "cluster-autoscaler.kubernetes.io/safe-to-evict")

(def default-shell
  "Default shell command used by our k8s scheduler to wrap and launch a job command
   when no custom shell is provided in the kubernetes compute cluster config."
  ["/bin/sh" "-c"
   (str "exec 1>$COOK_SANDBOX/stdout; "
        "exec 2>$COOK_SANDBOX/stderr; "
        "exec /bin/sh -c \"$1\"")
   "--"])

; DeletionCandidateTaint is a soft taint that k8s uses to mark unneeded
; nodes as preferably unschedulable. This taint is added as soon as the
; autoscaler detects that nodes are under-utilized and all pods could be
; scheduled even with fewer nodes in the node pool. This taint is
; subsequently "cleaned" if the node stops being unneeded. We need to
; tolerate this taint so that we can use nodes even if the autoscaler
; finds them to be temporarily unneeded. Note that there is a "harder"
; taint, ToBeDeletedByClusterAutoscaler, which is added right before a
; node is deleted, which we don't tolerate.
(def k8s-deletion-candidate-taint "DeletionCandidateOfClusterAutoscaler")

(def ^ExecutorService kubernetes-executor (Executors/newCachedThreadPool))

; Cook, Fenzo, and Mesos use mebibytes (MiB) for memory.
; Convert bytes from k8s to MiB when passing to Fenzo,
; and MiB back to bytes when submitting to k8s.
(def memory-multiplier (* 1024 1024))

(defn is-cook-scheduler-pod
  "Is this a cook pod? Uses some-> so is null-safe."
  [^V1Pod pod compute-cluster-name]
  (= compute-cluster-name (some-> pod .getMetadata .getLabels (.get cook-pod-label))))

(defn pod->node-name
  "Given a pod, returns the node name on the pod spec"
  [^V1Pod pod]
  (or (some-> pod .getSpec .getNodeName)
      (some-> pod .getSpec .getNodeSelector (get k8s-hostname-label))))

(defn cook-pod-callback-wrap
  "A special wrapping function that, given a callback, key, prev-item, and item, will invoke the callback only
  if the item is a pod that is a cook scheduler pod. (THe idea is that (partial cook-pod-callback-wrap othercallback)
  returns a new callback that only invokes othercallback on cook pods."
  [callback compute-cluster-name key prev-item item]
  (when (or (is-cook-scheduler-pod prev-item compute-cluster-name) (is-cook-scheduler-pod item compute-cluster-name))
    (callback key prev-item item)))

(defn handle-watch-updates
  "When a watch update occurs (for pods or nodes) update both the state atom as well as
  invoke the callbacks on the previous and new values for the key."
  [state-atom ^Watch watch key-fn callbacks]
  (while (.hasNext watch)
    (let [watch-response (.next watch)
          item (.-object watch-response) ;is always non-nil, even for deleted items.
          key (key-fn item)
          prev-item (get @state-atom key)
          ; Now we want to re-bind prev-item and item to the real previous and current,
          ; embedding ADDED/MODIFIED/DELETED based on the location of nils.
          [prev-item item]
          (case (.-type watch-response)
            "ADDED" [nil item]
            "MODIFIED" [prev-item item]
            "DELETED" [prev-item nil])]

      (doseq [callback callbacks]
        (try
          (callback key prev-item item)
          (catch Exception e
            (log/error e "Error while processing callback")))))))

(defn get-pod-namespaced-key
  [^V1Pod pod]
  (if-let [^V1ObjectMeta pod-metadata (some-> pod .getMetadata)]
    {:namespace (-> pod-metadata .getNamespace)
     :name (-> pod-metadata .getName)}
    (throw (ex-info
             (str "Unable to get namespaced key for pod: " pod)
             {:pod pod}))))

(defn get-all-pods-in-kubernetes
  "Get all pods in kubernetes."
  [api-client compute-cluster-name]
  (timers/time! (metrics/timer "get-all-pods" compute-cluster-name)
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
          namespaced-pod-name->pod (pc/map-from-vals get-pod-namespaced-key
                                                     (.getItems current-pods))]
      [current-pods namespaced-pod-name->pod])))

(defn try-forever-get-all-pods-in-kubernetes
  "Try forever to get all pods in kubernetes. Used when starting a cluster up."
  [api-client compute-cluster-name]
  (loop []
    (let [{:keys [reconnect-delay-ms]} (config/kubernetes)
          out (try
                (get-all-pods-in-kubernetes api-client compute-cluster-name)
                (catch Throwable e
                  (log/error e "Error during cluster startup getting all pods for" compute-cluster-name
                             "and sleeping" reconnect-delay-ms "milliseconds before reconnect")
                  (Thread/sleep reconnect-delay-ms)
                  nil))]
      (if out
        out
        (recur)))))

(defn create-pod-watch
  "Wrapper for WatchHelper/createPodWatch"
  [^ApiClient api-client resource-version]
  (WatchHelper/createPodWatch api-client resource-version))

(declare initialize-pod-watch)
(defn ^Callable initialize-pod-watch-helper
  "Help creating pod watch. Returns a new watch Callable"
  [{:keys [^ApiClient api-client all-pods-atom node-name->pod-name->pod] compute-cluster-name :name :as compute-cluster} cook-pod-callback]
  (let [[current-pods namespaced-pod-name->pod] (get-all-pods-in-kubernetes api-client compute-cluster-name)
        callbacks
        [(util/make-atom-updater all-pods-atom) ; Update the set of all pods.
         (util/make-nested-atom-updater node-name->pod-name->pod pod->node-name get-pod-namespaced-key)
         (partial cook-pod-callback-wrap cook-pod-callback compute-cluster-name)] ; Invoke the cook-pod-callback if its a cook pod.
        old-all-pods @all-pods-atom]
    (log/info "In" compute-cluster-name "compute cluster, pod watch processing pods:" (keys namespaced-pod-name->pod))
    ; We want to process all changes through the callback process.
    ; So compute the delta between the old and new and process those via the callbacks.
    ; Note as a side effect, the callbacks mutate all-pods-atom
    (doseq [task (set/union (set (keys namespaced-pod-name->pod)) (set (keys old-all-pods)))]
      (log/info "In" compute-cluster-name "compute cluster, pod watch doing (startup) callback for" task)
      (doseq [callback callbacks]
        (try
          (callback task (get old-all-pods task) (get namespaced-pod-name->pod task))
          (catch Exception e
            (log/error e "In" compute-cluster-name
                       "compute cluster, pod watch error while processing callback for" task)))))

    (let [watch (create-pod-watch api-client (-> current-pods
                                                 .getMetadata
                                                 .getResourceVersion))]
      (fn []
        (try
          (log/info "In" compute-cluster-name "compute cluster, handling pod watch updates")
          (handle-watch-updates all-pods-atom watch get-pod-namespaced-key
                                callbacks)
          (catch Exception e
            (let [cause (.getCause e)]
              (if (and cause (instance? SocketTimeoutException cause))
                (log/info e "In" compute-cluster-name "compute cluster, pod watch timed out")
                (log/error e "In" compute-cluster-name "compute cluster, error during pod watch"))))
          (finally
            (.close watch)
            (initialize-pod-watch compute-cluster cook-pod-callback)))))))

(defn initialize-pod-watch
  "Initialize the pod watch. This fills all-pods-atom with data and invokes the callback on pod changes."
  [{compute-cluster-name :name :as compute-cluster} cook-pod-callback]
  (log/info "In" compute-cluster-name "compute cluster, initializing pod watch")
  ; We'll iterate trying to connect to k8s until the initialize-pod-watch-helper returns a watch function.
  (let [{:keys [reconnect-delay-ms]} (config/kubernetes)
        tmpfn (fn []
                (try
                  (log/info "In" compute-cluster-name "compute cluster, initializing pod watch helper")
                  (initialize-pod-watch-helper compute-cluster cook-pod-callback)
                  (catch Exception e
                    (log/error e "Error during pod watch initial setup of looking at pods for" compute-cluster-name
                               "and sleeping" reconnect-delay-ms "milliseconds before reconnect")
                    (Thread/sleep reconnect-delay-ms)
                    nil)))
        ^Callable first-success (->> tmpfn repeatedly (some identity))]
    (.submit kubernetes-executor ^Callable first-success)))

(defn node->node-name
  "Given a V1Node, return the name of the node"
  [^V1Node node]
  (-> node .getMetadata .getName))

(defn get-node-pool
  "Get the pool for a node. In the case of no pool, return 'no-pool"
  [^V1Node node]
  ; In the case of nil, we have taints-on-node == [], and we'll map to no-pool.
  (let [taints-on-node (or (some-> node .getSpec .getTaints) [])
        found-cook-pool-taint (filter #(= cook-pool-taint (.getKey %)) taints-on-node)]
    (if (= 1 (count found-cook-pool-taint))
      (-> found-cook-pool-taint first .getValue)
      "no-pool")))

(declare initialize-node-watch)
(defn initialize-node-watch-helper
  "Help creating node watch. Returns a new watch Callable"
  [{:keys [^ApiClient api-client current-nodes-atom pool->node-name->node] compute-cluster-name :name :as compute-cluster}]
  (let [api (CoreV1Api. api-client)
        current-nodes-raw
        (timers/time! (metrics/timer "get-all-nodes" compute-cluster-name)
          (.listNode api
                     nil ; includeUninitialized
                     nil ; pretty
                     nil ; continue
                     nil ; fieldSelector
                     nil ; labelSelector
                     nil ; limit
                     nil ; resourceVersion
                     nil ; timeoutSeconds
                     nil ; watch
                     ))
        current-nodes (pc/map-from-vals node->node-name (.getItems current-nodes-raw))
        callbacks
        [(util/make-atom-updater current-nodes-atom) ; Update the set of all pods.
         (util/make-nested-atom-updater pool->node-name->node get-node-pool node->node-name)]
         old-current-nodes @current-nodes-atom]
    (log/info "In" compute-cluster-name "compute cluster, node watch processing nodes:" (keys @current-nodes-atom))
    ; We want to process all changes through the callback process.
    ; So compute the delta between the old and new and process those via the callbacks.
    ; Note as a side effect, the callbacks mutate current-nodes-atom
    (doseq [node-name (set/union (set (keys current-nodes)) (set (keys old-current-nodes)))]
      (log/info "In" compute-cluster-name "compute cluster, node watch doing (startup) callback for" node-name)
      (doseq [callback callbacks]
        (try
          (callback node-name (get old-current-nodes node-name) (get current-nodes node-name))
          (catch Exception e
            (log/error e "In" compute-cluster-name
                       "compute cluster, node watch error while processing callback for" node-name)))))

    (let [watch (WatchHelper/createNodeWatch api-client (-> current-nodes-raw .getMetadata .getResourceVersion))]
      (fn []
        (try
          (log/info "In" compute-cluster-name "compute cluster, handling node watch updates")
          (handle-watch-updates current-nodes-atom watch node->node-name
                                callbacks) ; Update the set of all nodes.
          (catch Exception e
            (log/warn e "Error during node watch for compute cluster" compute-cluster-name))
          (finally
            (.close watch)
            (initialize-node-watch compute-cluster)))))))

(defn initialize-node-watch
  "Initialize the node watch. This fills current-nodes-atom with data and invokes the callback on pod changes."
  [{:keys [^ApiClient api-client current-nodes-atom] compute-cluster-name :name :as compute-cluster}]
  (log/info "In" compute-cluster-name "compute cluster, initializing node watch")
  ; We'll iterate trying to connect to k8s until the initialize-node-watch-helper returns a watch function.
  (let [{:keys [reconnect-delay-ms]} (config/kubernetes)
        tmpfn (fn []
                (try
                  (log/info "In" compute-cluster-name "compute cluster, initializing node watch helper")
                  (initialize-node-watch-helper compute-cluster)
                  (catch Exception e
                    (log/error e "Error during node watch initial setup of looking at nodes for" compute-cluster-name
                               "and sleeping" reconnect-delay-ms "milliseconds before reconnect")
                    (Thread/sleep reconnect-delay-ms)
                    nil)))
        ^Callable first-success (->> tmpfn repeatedly (some identity))]
    (.submit kubernetes-executor ^Callable first-success)))

(declare initialize-event-watch)
(let [json (JSON.)]
  (defn ^Callable initialize-event-watch-helper
    "Returns a new event watch Callable"
    [^ApiClient api-client compute-cluster-name all-pods-atom]
    (let [watch (WatchHelper/createEventWatch api-client nil)]
      (fn []
        (try
          (log/info "In" compute-cluster-name "compute cluster, handling event watch updates")
          (while (.hasNext watch)
            (let [watch-response (.next watch)
                  ^V1Event event (.-object watch-response)]
              (when event
                (let [^V1ObjectReference involved-object (.getInvolvedObject event)]
                  (when (and involved-object (= (.getKind involved-object) "Pod"))
                    (let [namespaced-pod-name {:namespace (.getNamespace involved-object)
                                               :name (.getName involved-object)}]
                      (when (some-> @all-pods-atom
                                    (get namespaced-pod-name)
                                    (is-cook-scheduler-pod compute-cluster-name))
                        (log/info "In" compute-cluster-name
                                  "compute cluster, received pod event"
                                  {:event-reason (.getReason event)
                                   :event (.serialize json event)
                                   :watch-response-type (.-type watch-response)}))))))))
          (catch Exception e
            (let [cause (.getCause e)]
              (if (and cause (instance? SocketTimeoutException cause))
                (log/info e "In" compute-cluster-name "compute cluster, event watch timed out")
                (log/error e "In" compute-cluster-name "compute cluster, error during event watch"))))
          (finally
            (.close watch)
            (initialize-event-watch api-client compute-cluster-name all-pods-atom)))))))

(defn initialize-event-watch
  "Initializes the event watch"
  [^ApiClient api-client compute-cluster-name all-pods-atom]
  (log/info "In" compute-cluster-name "compute cluster, initializing event watch")
  (let [{:keys [reconnect-delay-ms]} (config/kubernetes)
        tmpfn (fn []
                (try
                  (log/info "In" compute-cluster-name "compute cluster, initializing event watch helper")
                  (initialize-event-watch-helper api-client compute-cluster-name all-pods-atom)
                  (catch Exception e
                    (log/error e "Error during event watch initial setup of looking at events for" compute-cluster-name
                               "and sleeping" reconnect-delay-ms "milliseconds before reconnect")
                    (Thread/sleep reconnect-delay-ms)
                    nil)))
        ^Callable first-success (->> tmpfn repeatedly (some identity))]
    (.submit kubernetes-executor ^Callable first-success)))

(defn to-double
  "Map a quantity to a double, whether integer, double, or float."
  [^Quantity q]
  (-> q .getNumber .doubleValue))

(defn to-int
  "Map a quantity to an int, whether integer, double, or float."
  [^Quantity q]
  (-> q .getNumber .intValue))

(defn convert-resource-map
  "Converts a map of Kubernetes resources to a cook resource map {:mem double, :cpus double, :gpus double}"
  [m]
  {:mem (if (get m "memory")
          (-> m (get "memory") to-double (/ memory-multiplier))
          0.0)
   :cpus (if (get m "cpu")
           (-> m (get "cpu") to-double)
           0.0)
   ; Assumes that each Kubernetes node and each pod only contains one type of GPU model
   :gpus (if-let [gpu-count (get m "nvidia.com/gpu")]
           (to-int gpu-count)
           0)})

(defn pods->node-name->pods
  "Given a seq of pods, create a map of node names to pods"
  [pods]
  (group-by pod->node-name pods))

(defn num-pods-on-node
  "Returns the number of pods assigned to the given node"
  [node-name pods]
  (let [node-name->pods (group-by pod->node-name pods)]
    (-> node-name->pods (get node-name []) count)))

(defn node-schedulable?
  "Can we schedule on a node. For now, yes, unless there are other taints on it or it contains any label in the
  node-blocklist-labels list.
  TODO: Incorporate other node-health measures here."
  [^V1Node node pod-count-capacity node-name->pods node-blocklist-labels]
  (if (nil? node)
    false
    (let [taints-on-node (or (some-> node .getSpec .getTaints) [])
          other-taints (remove #(contains?
                                  #{cook-pool-taint k8s-deletion-candidate-taint gpu-node-taint}
                                  (.getKey %))
                               taints-on-node)
          node-name (some-> node .getMetadata .getName)
          num-pods-on-node (-> node-name->pods (get node-name []) count)
          labels-on-node (or (some-> node .getMetadata .getLabels) {})
          matching-node-blocklist-keyvals (select-keys labels-on-node node-blocklist-labels)]
      (cond  (seq other-taints) (do
                                  (log/info "Filtering out" node-name "because it has taints" other-taints)
                                  false)
             (>= num-pods-on-node pod-count-capacity) (do
                                                        (log/info "Filtering out" node-name "because it is at or above its pod count capacity of"
                                                                  pod-count-capacity "(" num-pods-on-node ")")
                                                        false)
             (seq matching-node-blocklist-keyvals) (do
                                                     (log/info "Filtering out" node-name "because it has node blocklist labels" matching-node-blocklist-keyvals)
                                                     false)
             :else true))))

(defn add-gpu-model-to-resource-map
  "Given a map from node-name->resource-type->capacity, perform the following operation:
  - if the amount of gpus on the node is positive, set the gpus capacity to model->count
  - if the amount of gpus on the node is 0, set the gpus capacity to an empty map"
  [gpu-model {:keys [gpus] :as resource-map}]
  (let [gpu-model->count (if (and gpu-model (pos? gpus))
                           {gpu-model gpus}
                           {})]
    (assoc resource-map :gpus gpu-model->count)))

(defn get-capacity
  "Given a map from node-name to node, generate a map from node-name->resource-type-><capacity>"
  [node-name->node]
  (pc/map-vals (fn [^V1Node node]
                 (let [resource-map (some-> node .getStatus .getAllocatable convert-resource-map)
                       gpu-model (some-> node .getMetadata .getLabels (get "gpu-type"))]
                   (add-gpu-model-to-resource-map gpu-model resource-map)))
               node-name->node))

(defn get-consumption
  "Given a map from pod-name to pod, generate a map from node-name->resource-type->capacity.
  Ignores pods that do not have an assigned node.
  When accounting for resources, we use resource requests to determine how much is used, not limits.
  See https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#resource-requests-and-limits-of-pod-and-container"
  [node-name->pods]
  (->> node-name->pods
       (filter first) ; Keep those with non-nil node names.
       (pc/map-vals (fn [pods]
                      (->> pods
                           (map (fn [^V1Pod pod]
                                  (let [containers (some-> pod .getSpec .getContainers)
                                        container-requests (map (fn [^V1Container c]
                                                                  (some-> c
                                                                          .getResources
                                                                          .getRequests
                                                                          convert-resource-map))
                                                                containers)
                                        resource-map (apply merge-with + container-requests)
                                        gpu-model (some-> pod .getSpec .getNodeSelector (get "cloud.google.com/gke-accelerator"))]
                                    (add-gpu-model-to-resource-map gpu-model resource-map))))
                           (apply util/deep-merge-with +))))))

; see pod->synthesized-pod-state comment for container naming conventions
(def cook-container-name-for-job
  "required-cook-job-container")
; see pod->synthesized-pod-state comment for container naming conventions
(def cook-container-name-for-file-server
  "aux-cook-sidecar-container")
; see pod->synthesized-pod-state comment for container naming conventions
(def cook-init-container-name
  "aux-cook-init-container")

(defn double->quantity
  "Converts a double to a Quantity"
  [^double value]
  (Quantity. (BigDecimal/valueOf value)
             Quantity$Format/DECIMAL_SI))

(defn- make-empty-volume
  "Make a kubernetes volume"
  [name]
  (.. (V1VolumeBuilder.)
      (withName name)
      (withEmptyDir (V1EmptyDirVolumeSource.))
      (build)))

(defn- make-volume-mount
  "Make a kubernetes volume mount"
  ([^V1Volume volume path read-only]
   (make-volume-mount volume path nil read-only))
  ([^V1Volume volume path sub-path read-only]
   (when volume
     (doto (V1VolumeMount.)
       (.setName (.getName volume))
       (.setMountPath path)
       (.setReadOnly read-only)
       (cond-> sub-path (.setSubPath sub-path))))))

(defn make-volumes
  "Converts a list of cook volumes to kubernetes volumes and volume mounts."
  [cook-volumes sandbox-dir]
  (let [{:keys [disallowed-container-paths]} (config/kubernetes)
        allowed-cook-volumes (remove (fn [{:keys [container-path host-path mode]}]
                                       (contains? disallowed-container-paths
                                                  ;; container-path defaults to host-path when omitted
                                                  (or container-path host-path)))
                                     cook-volumes)
        host-paths (->> allowed-cook-volumes (map :host-path) distinct)
        volumes (vec (for [host-path host-paths]
                       (doto (V1Volume.)
                         (.setName (str "syn-" (d/squuid)))
                         (.setHostPath (doto (V1HostPathVolumeSource.)
                                         (.setPath host-path)
                                         (.setType "DirectoryOrCreate"))))))
        host-path->volume (zipmap host-paths volumes)
        volume-mounts (vec (for [{:keys [container-path host-path mode]} allowed-cook-volumes]
                             (make-volume-mount
                               (host-path->volume host-path)
                               ;; container-path defaults to host-path when omitted
                               (or container-path host-path)
                               (not= "RW" mode))))
        sandbox-volume (make-empty-volume cook-sandbox-volume-name)
        sandbox-volume-mount-fn #(make-volume-mount sandbox-volume sandbox-dir %)
        sandbox-volume-mount (sandbox-volume-mount-fn false)
        ; mesos-sandbox-volume-mount added for Mesos backward compatibility
        mesos-sandbox-volume-mount (make-volume-mount sandbox-volume "/mnt/mesos/sandbox" false)]
    {:sandbox-volume-mount-fn sandbox-volume-mount-fn
     :volumes (conj volumes sandbox-volume)
     :volume-mounts (conj volume-mounts sandbox-volume-mount mesos-sandbox-volume-mount)}))

(defn toleration-for-pool
  "For a given cook pool name, create the right V1Toleration so that Cook will ignore that cook-pool taint."
  [pool-name]
  (let [^V1Toleration toleration (V1Toleration.)]
    (.setKey toleration cook-pool-taint)
    (.setValue toleration pool-name)
    (.setOperator toleration "Equal")
    (.setEffect toleration "NoSchedule")
    toleration))

(def toleration-for-deletion-candidate-of-autoscaler
  (doto (V1Toleration.)
    (.setKey k8s-deletion-candidate-taint)
    (.setOperator "Exists")
    (.setEffect "PreferNoSchedule")))

(defn build-params-env
  "Make environment vars map from docker parameters"
  [params]
  (pc/for-map [{:keys [key value]} params
               :when (= "env" key)
               :let [[var-key var-val] (str/split value #"=")]]
    var-key var-val))

(defn make-security-context
  [params user]
  (let [user-param (->> params
                        (filter (fn [{:keys [key]}] (= key "user")))
                        first)
        [uid gid] (if user-param
                    ;; TODO new jobs should always have the user with correct credentials;
                    ;; TODO validate and fail when the user parameter is missing.
                    (let [[uid gid] (str/split (:value user-param) #":")]
                      [(Long/parseLong uid) (Long/parseLong gid)])
                    [(util/user->user-id user) (util/user->group-id user)])
        security-context (V1PodSecurityContext.)]
    (.setRunAsUser security-context uid)
    (.setRunAsGroup security-context gid)
    security-context))

(defn get-workdir
  [params default-workdir]
  (let [workdir-param (->> params
                           (filter (fn [{:keys [key]}] (= key "workdir")))
                           first)]
    (if workdir-param
      (:value workdir-param)
      default-workdir)))

(defn- make-env
  "Make a kubernetes environment variable"
  [var-name var-value]
  (doto (V1EnvVar.)
    (.setName (str var-name))
    (.setValue (str var-value))))

(defn make-filtered-env-vars
  "Create a Kubernetes API compatible var list from an environment vars map,
   with variables filtered based on disallowed-var-names in the Kubernetes config."
  [env]
  (let [{:keys [disallowed-var-names]} (config/kubernetes)
        disallowed-var? #(contains? disallowed-var-names (key %))]
    (->> env
         (remove disallowed-var?)
         (mapv #(apply make-env %)))))

(defn- add-as-decimals
  "Takes two doubles and adds them as decimals to avoid floating point error. Kubernetes will not be able to launch a
   pod if the required cpu has too much precision. For example, adding 0.1 and 0.02 as doubles results in 0.12000000000000001"
  [a b]
  (double (+ (bigdec a) (bigdec b))))

(defn adjust-job-resources
  "Adjust required resources for a job, adding any additional resources. Additional resources might be needed for
   sidecars or extra processes that will be launched with the job. This is called from make-task-request in the scheduler.
   The adjusted resource value is used for bin-packing when scheduling."
  [{:keys [job/checkpoint] :as job} {:keys [cpus mem] :as resources}]
  (let [{:keys [cpu-request memory-request] :as resource-requirements}
        (-> (config/kubernetes) :sidecar :resource-requirements)
        checkpoint-memory-overhead
        (when checkpoint
          (-> (config/kubernetes) :default-checkpoint-config (merge (util/job-ent->checkpoint job)) :memory-overhead))]
    (cond-> resources
      resource-requirements
      (assoc
        :cpus (add-as-decimals cpus cpu-request)
        :mem (add-as-decimals mem memory-request))
      checkpoint-memory-overhead
      (update :mem #(add-as-decimals % checkpoint-memory-overhead)))))

(defn- add-node-selector
  "Adds a node selector with the given key and val to the given pod spec"
  [^V1PodSpec pod-spec key val]
  (.setNodeSelector pod-spec (-> pod-spec
                                 .getNodeSelector
                                 (assoc key val))))

(defn- checkpoint->volume
  "Get separate volume needed for checkpointing"
  [{:keys [mode volume-name]}]
  (when (and mode volume-name)
    (make-empty-volume volume-name)))

(defn- checkpoint->volume-mounts
  "Get custom volume mounts needed for checkpointing"
  [{:keys [mode init-container-volume-mounts main-container-volume-mounts]} checkpointing-tools-volume]
  (let [volumes-from-spec-fn #(map (fn [{:keys [path sub-path]}]
                                     (make-volume-mount checkpointing-tools-volume path sub-path false)) %)]
    (when (and mode checkpointing-tools-volume)
      {:init-container-checkpoint-volume-mounts (volumes-from-spec-fn init-container-volume-mounts)
       :main-container-checkpoint-volume-mounts (volumes-from-spec-fn main-container-volume-mounts)})))

(defn checkpoint->env
  "Get environment variables needed for checkpointing"
  [{:keys [mode options periodic-options]}]
  (cond-> {}
    mode
    (assoc "COOK_CHECKPOINT_MODE" mode)
    options
    ((fn [m]
       (let [{:keys [preserve-paths]} options]
         (cond-> m
           preserve-paths
           (#(reduce-kv (fn [map index path] (assoc map (str "COOK_CHECKPOINT_PRESERVE_PATH_" index) path))
                        % (vec (sort preserve-paths))))))))
    periodic-options
    ((fn [m]
       (let [{:keys [period-sec]} periodic-options]
         (cond-> m
           period-sec
           (assoc "COOK_CHECKPOINT_PERIOD_SEC" (str period-sec))))))))

(def default-checkpoint-failure-reasons
  "Default set of failure reasons that should be counted against checkpointing attempts"
  #{:max-runtime-exceeded
    :mesos-command-executor-failed
    :mesos-container-launch-failed
    :mesos-container-limitation-memory
    :mesos-unknown
    :straggler})

(defn calculate-effective-checkpointing-config
  "Given the job's checkpointing config, calculate the effective config. Making any adjustments such as defaults,
  overrides, or other behavior modifications."
  [{:keys [job/checkpoint job/instance] :as job} task-id]
  (when checkpoint
    (let [{:keys [default-checkpoint-config]} (config/kubernetes)
          {:keys [max-checkpoint-attempts checkpoint-failure-reasons] :as checkpoint}
          (merge default-checkpoint-config (util/job-ent->checkpoint job))]
      (if max-checkpoint-attempts
        (let [checkpoint-failure-reasons (or checkpoint-failure-reasons default-checkpoint-failure-reasons)
              checkpoint-failures (filter (fn [{:keys [instance/reason]}]
                                            (contains? checkpoint-failure-reasons (:reason/name reason)))
                                          instance)]
          (if (-> checkpoint-failures count (>= max-checkpoint-attempts))
            (log/info "Will not checkpoint task-id" task-id ", there are at least" max-checkpoint-attempts "failed instances"
                      {:job job})
            checkpoint))
        checkpoint))))

(defn ^V1Pod task-metadata->pod
  "Given a task-request and other data generate the kubernetes V1Pod to launch that task."
  [namespace compute-cluster-name
   {:keys [task-id command container task-request hostname pod-annotations pod-labels pod-hostnames-to-avoid
           pod-priority-class pod-supports-cook-init? pod-supports-cook-sidecar?]
    :or {pod-priority-class cook-job-pod-priority-class
         pod-supports-cook-init? true
         pod-supports-cook-sidecar? true}}]
  (let [{:keys [scalar-requests job resources]} task-request
        ;; NOTE: The scheduler's adjust-job-resources-for-pool-fn may modify :resources,
        ;; whereas :scalar-requests always contains the unmodified job resource values.
        {:strs [mem cpus]} scalar-requests
        {:keys [docker volumes]} container
        {:keys [image parameters port-mapping]} docker
        {:keys [environment]} command
        pool-name (some-> job :job/pool :pool/name)
        ; gpu count is not stored in scalar-requests because Fenzo does not handle gpus in binpacking
        gpus (or (:gpus resources) 0)
        gpu-model-requested (constraints/job->gpu-model-requested gpus job pool-name)
        pod (V1Pod.)
        pod-spec (V1PodSpec.)
        metadata (V1ObjectMeta.)
        container (V1Container.)
        resources (V1ResourceRequirements.)
        labels (assoc pod-labels cook-pod-label compute-cluster-name)
        security-context (make-security-context parameters (:user command))
        sandbox-dir (:default-workdir (config/kubernetes))
        workdir (get-workdir parameters sandbox-dir)
        {:keys [volumes volume-mounts sandbox-volume-mount-fn]} (make-volumes volumes sandbox-dir)
        {:keys [custom-shell init-container set-container-cpu-limit? sidecar]} (config/kubernetes)
        checkpoint (calculate-effective-checkpointing-config job task-id)
        checkpoint-memory-overhead (:memory-overhead checkpoint)
        use-cook-init? (and init-container pod-supports-cook-init?)
        use-cook-sidecar? (and sidecar pod-supports-cook-sidecar?)
        init-container-workdir "/mnt/init-container"
        init-container-workdir-volume (when use-cook-init?
                                        (make-empty-volume "cook-init-container-workdir-volume"))
        init-container-workdir-volume-mount-fn (partial make-volume-mount
                                                        init-container-workdir-volume
                                                        init-container-workdir)
        sidecar-workdir "/mnt/sidecar"
        sidecar-workdir-volume (when use-cook-sidecar? (make-empty-volume "cook-sidecar-workdir-volume"))
        sidecar-workdir-volume-mount-fn (partial make-volume-mount sidecar-workdir-volume sidecar-workdir)
        scratch-space "/mnt/scratch-space"
        scratch-space-volume (make-empty-volume "cook-scratch-space-volume")
        scratch-space-volume-mount-fn (partial make-volume-mount scratch-space-volume scratch-space)
        checkpoint-volume (checkpoint->volume checkpoint)
        {:keys [init-container-checkpoint-volume-mounts main-container-checkpoint-volume-mounts]}
        (checkpoint->volume-mounts checkpoint checkpoint-volume)
        sandbox-env {"COOK_SANDBOX" sandbox-dir
                     "HOME" sandbox-dir
                     "MESOS_DIRECTORY" sandbox-dir
                     "MESOS_SANDBOX" sandbox-dir
                     "SIDECAR_WORKDIR" sidecar-workdir}
        params-env (build-params-env parameters)
        progress-env (task/build-executor-environment job)
        checkpoint-env (checkpoint->env checkpoint)
        main-env-base (merge environment params-env progress-env sandbox-env checkpoint-env)
        progress-file-var (get main-env-base task/progress-meta-env-name task/default-progress-env-name)
        progress-file-path (get main-env-base progress-file-var)
        main-env (cond-> main-env-base
                   ;; Add a default progress file path to the environment when missing,
                   ;; preserving compatibility with Meosos + Cook Executor.
                   (not progress-file-path)
                   (assoc progress-file-var (str task-id ".progress")))
        main-env-vars (make-filtered-env-vars main-env)
        computed-mem (if checkpoint-memory-overhead (add-as-decimals mem checkpoint-memory-overhead) mem)]

    ; metadata
    (.setName metadata (str task-id))
    (.setNamespace metadata namespace)
    (.setLabels metadata labels)
    (when pod-annotations
      (.setAnnotations metadata pod-annotations))

    ; container
    (.setName container cook-container-name-for-job)
    (.setCommand container
                 (conj (or (when use-cook-init? custom-shell) default-shell)
                       (:value command)))
    (.setEnv container main-env-vars)
    (.setImage container image)
    (doseq [{:keys [container-port host-port protocol]} port-mapping]
      (when container-port
        (let [port (V1ContainerPort.)]
          (.containerPort port (int container-port))
          (when host-port
            (.hostPort port (int host-port)))
          (when protocol
            (.protocol port protocol))
          (.addPortsItem container port))))

    ; allocate a TTY to support tools that need to read from stdin
    (.setTty container true)
    (.setStdin container true)

    (.putRequestsItem resources "memory" (double->quantity (* memory-multiplier computed-mem)))
    (.putLimitsItem resources "memory" (double->quantity (* memory-multiplier computed-mem)))
    (.putRequestsItem resources "cpu" (double->quantity cpus))
    (when set-container-cpu-limit?
      ; Some environments may need pods to run in the "Guaranteed"
      ; QoS, which requires limits for both memory and cpu
      (.putLimitsItem resources "cpu" (double->quantity cpus)))
    (when (pos? gpus)
      (.putLimitsItem resources "nvidia.com/gpu" (double->quantity gpus))
      (.putRequestsItem resources "nvidia.com/gpu" (double->quantity gpus))
      (add-node-selector pod-spec "cloud.google.com/gke-accelerator" gpu-model-requested)
      ; GKE nodes with GPUs have gpu-count label, so synthetic pods need a matching node selector
      (add-node-selector pod-spec "gpu-count" (-> gpus int str)))
    (.setResources container resources)
    (.setVolumeMounts container (filterv some? (conj (concat volume-mounts main-container-checkpoint-volume-mounts)
                                                     (init-container-workdir-volume-mount-fn true)
                                                     (scratch-space-volume-mount-fn false)
                                                     (sidecar-workdir-volume-mount-fn true))))
    (.setWorkingDir container workdir)

    ; pod-spec
    (.addContainersItem pod-spec container)

    ; init container
    (when use-cook-init?
      (when-let [{:keys [command image]} init-container]
        (let [container (V1Container.)]
          ; container
          (.setName container cook-init-container-name)
          (.setImage container image)
          (.setCommand container command)
          (.setWorkingDir container init-container-workdir)
          (.setEnv container main-env-vars)
          (.setVolumeMounts container (filterv some? (concat [(init-container-workdir-volume-mount-fn false)
                                                              (scratch-space-volume-mount-fn false)]
                                                             init-container-checkpoint-volume-mounts)))
          (.addInitContainersItem pod-spec container))))

    ; sandbox file server container
    (when use-cook-sidecar?
      (when-let [{:keys [command health-check-endpoint image port resource-requirements]} sidecar]
        (let [{:keys [cpu-request cpu-limit memory-request memory-limit]} resource-requirements
              container (V1Container.)
              resources (V1ResourceRequirements.)]
          ; container
          (.setName container cook-container-name-for-file-server)
          (.setImage container image)
          (.setCommand container (conj command (str port)))
          (.setWorkingDir container sidecar-workdir)
          (.setPorts container [(.containerPort (V1ContainerPort.) (int port))])

          (.setEnv container (conj main-env-vars
                                   (make-env "COOK_SCHEDULER_REST_URL" (config/scheduler-rest-url))
                                   ;; DEPRECATED - sidecar should use COOK_SANDBOX instead.
                                   ;; Will remove this environment variable in a future release.
                                   (make-env "COOK_WORKDIR" sandbox-dir)))

          ; optionally enable http-based readiness probe
          (when health-check-endpoint
            (let [http-get-action (doto (V1HTTPGetAction.)
                                    (.setPort (-> port int IntOrString.))
                                    (.setPath health-check-endpoint))
                  readiness-probe (doto (V1Probe.)
                                    (.setHttpGet http-get-action))]
            (.setReadinessProbe container readiness-probe)))

          ; resources
          (.putRequestsItem resources "cpu" (double->quantity cpu-request))
          (when set-container-cpu-limit?
            (.putLimitsItem resources "cpu" (double->quantity cpu-limit)))
          (.putRequestsItem resources "memory" (double->quantity (* memory-multiplier memory-request)))
          (.putLimitsItem resources "memory" (double->quantity (* memory-multiplier memory-limit)))
          (.setResources container resources)

          (.setVolumeMounts container [(sandbox-volume-mount-fn true) (sidecar-workdir-volume-mount-fn false)])
          (.addContainersItem pod-spec container))))

    ; We're either using the hostname (in the case of users' job pods)
    ; or pod-hostnames-to-avoid (in the case of synthetic pods), but not both.
    (if hostname
      ; Why not just set the node name?
      ; The reason is that setting the node name prevents pod preemption
      ; (https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/)
      ; from happening. We want this pod to preempt lower priority pods
      ; (e.g. synthetic pods).
      (add-node-selector pod-spec k8s-hostname-label hostname)
      (when (seq pod-hostnames-to-avoid)
        ; Use node "anti"-affinity to disallow scheduling on nodes with particular labels
        ; (https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#node-affinity)
        (let [affinity (V1Affinity.)
              node-affinity (V1NodeAffinity.)
              node-selector (V1NodeSelector.)
              node-selector-term (V1NodeSelectorTerm.)]

          ; Disallow scheduling on hostnames we're being told to avoid (if any)
          (let [node-selector-requirement-k8s-hostname-label (V1NodeSelectorRequirement.)]
            (.setKey node-selector-requirement-k8s-hostname-label k8s-hostname-label)
            (.setOperator node-selector-requirement-k8s-hostname-label "NotIn")
            (run! #(.addValuesItem node-selector-requirement-k8s-hostname-label %) pod-hostnames-to-avoid)
            (.addMatchExpressionsItem node-selector-term node-selector-requirement-k8s-hostname-label))

          (.addNodeSelectorTermsItem node-selector node-selector-term)
          (.setRequiredDuringSchedulingIgnoredDuringExecution node-affinity node-selector)
          (.setNodeAffinity affinity node-affinity)
          (.setAffinity pod-spec affinity))))

    (.setRestartPolicy pod-spec "Never")

    (.addTolerationsItem pod-spec toleration-for-deletion-candidate-of-autoscaler)
    ; TODO:
    ; This will need to change to allow for setting the toleration
    ; for the default pool when the pool was not specified by the
    ; user. For the time being, this isn't a problem only if / when
    ; the default pool is not being fed offers from Kubernetes.
    (when pool-name
      (.addTolerationsItem pod-spec (toleration-for-pool pool-name))
      ; Add a node selector for nodes labeled with the Cook pool
      ; we're launching in. This is technically only needed for
      ; synthetic pods (which don't specify a node name), but it
      ; doesn't hurt to add it for all pods we submit.
      (add-node-selector pod-spec cook-pool-label pool-name))
    (.setVolumes pod-spec (filterv some? (conj volumes
                                               init-container-workdir-volume
                                               scratch-space-volume
                                               sidecar-workdir-volume
                                               checkpoint-volume)))
    (.setSecurityContext pod-spec security-context)
    (.setPriorityClassName pod-spec pod-priority-class)

    ; pod
    (.setMetadata pod metadata)
    (.setSpec pod pod-spec)

    pod))

(defn V1Pod->name
  "Extract the name of a pod from the pod itself"
  [^V1Pod pod]
  (-> pod .getMetadata .getName))

(defn synthetic-pod?
  "Given a pod name, returns true if it has the synthetic pod prefix"
  [pod-name]
  (str/starts-with? pod-name cook-synthetic-pod-name-prefix))

(defn pod-unschedulable?
  "Returns true if the given pod name is not a synthetic
  pod and the given pod status has a PodScheduled
  condition with status False and reason Unschedulable"
  [pod-name ^V1PodStatus pod-status]
  (let [{:keys [pod-condition-unschedulable-seconds]} (config/kubernetes)]
    (and (some->> pod-name synthetic-pod? not)
         (some->> pod-status .getConditions
                  (some
                    (fn pod-condition-unschedulable?
                      [^V1PodCondition condition]
                      (and (-> condition .getType (= "PodScheduled"))
                           (-> condition .getStatus (= "False"))
                           (-> condition .getReason (= "Unschedulable"))
                           (-> condition
                               .getLastTransitionTime
                               (t/plus (t/seconds pod-condition-unschedulable-seconds))
                               (t/before? (t/now))))))))))

(defn pod-containers-not-initialized?
  "Returns true if the given pod status has an Initialized condition with status
  False and reason ContainersNotInitialized, and the last transition was more than
  pod-condition-containers-not-initialized-seconds seconds ago"
  [pod-name ^V1PodStatus pod-status]
  (let [{:keys [pod-condition-containers-not-initialized-seconds]} (config/kubernetes)
        ^V1PodCondition pod-condition
        (some->> pod-status
                 .getConditions
                 (filter
                   (fn pod-condition-containers-not-initialized?
                     [^V1PodCondition condition]
                     (and (-> condition .getType (= "Initialized"))
                          (-> condition .getStatus (= "False"))
                          (-> condition .getReason (= "ContainersNotInitialized")))))
                 first)]
    (when pod-condition
      (let [last-transition-time-plus-threshold-seconds
            (-> pod-condition
                .getLastTransitionTime
                (t/plus (t/seconds pod-condition-containers-not-initialized-seconds)))
            now (t/now)
            threshold-passed? (t/before? last-transition-time-plus-threshold-seconds now)]
        (log/info "Pod" pod-name "has containers that are not initialized"
                  {:last-transition-time-plus-threshold-seconds last-transition-time-plus-threshold-seconds
                   :now now
                   :pod-condition pod-condition
                   :threshold-passed? threshold-passed?})
        threshold-passed?))))

(defn pod->synthesized-pod-state
  "From a V1Pod object, determine the state of the pod, waiting running, succeeded, failed or unknown.

   Kubernetes doesn't really have a good notion of 'pod state'. For one thing, that notion can't be universal across
   applications. Thus, we synthesize that state by looking at the containers within the pod and applying our own
   business logic."
  [^V1Pod pod]
  (when pod
    (let [^V1PodStatus pod-status (.getStatus pod)
          container-statuses (.getContainerStatuses pod-status)
          ; TODO: We want the generation of the pod status to reflect the container status:
          ; Right now, we only look at one container, the required-cook-job-container container in a pod.
          ; * In the future, we want support for cook sidecars. In order to determine the pod status
          ;   from the main cook job status and the sidecar statuses, we'll use a naming scheme for sidecars
          ; * A pod will be considered complete if all containers with name required-* are complete.
          ; * The main job container will always be named required-cook-job-container
          ; * We want a pod to be considered failed if any container with the name required-* fails or any container
          ;   with the name extra-* fails.
          ; * A job may have additional containers with the name aux-*
          job-status (first (filter (fn [c] (= cook-container-name-for-job (.getName c)))
                                    container-statuses))
          pod-preempted-timestamp (some-> pod .getMetadata .getLabels (get (or (-> (config/kubernetes) :node-preempted-label) "node-preempted")))
          synthesized-pod-state
          (if (some-> pod .getMetadata .getDeletionTimestamp)
            ; If a pod has been ordered deleted, treat it as if it was gone, It's being async removed.
            ; Note that we distinguish between this explicit :missing, and not being there at all when processing
            ; (:cook-expected-state/killed, :missing) in cook.kubernetes.controller/process
            {:state :missing
             :reason "Pod was explicitly deleted"
             :pod-deleted? true}
            ; If pod isn't being async removed, then look at the containers inside it.
            (if job-status
              (let [^V1ContainerState state (.getState job-status)]
                (cond
                  (.getWaiting state)
                  (if (pod-containers-not-initialized? (V1Pod->name pod) pod-status)
                    ; If the containers are not getting initialized,
                    ; then we should consider the pod failed. This
                    ; state can occur, for example, when volume
                    ; mounts fail.
                    {:state :pod/failed
                     :reason "ContainersNotInitialized"}
                    {:state :pod/waiting
                     :reason (-> state .getWaiting .getReason)})
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
              (cond
                ; https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase
                ; Failed means:
                ; All Containers in the Pod have terminated, and at least
                ; one Container has terminated in failure. That is, the
                ; Container either exited with non-zero status or was
                ; terminated by the system.
                (= (.getPhase pod-status) "Failed") {:state :pod/failed
                                                     :reason (.getReason pod-status)}

                ; If the pod is unschedulable, then we should consider it failed. Note that
                ; pod-unschedulable? will never return true for synthetic pods, because
                ; they will be unschedulable by design, in order to trigger the cluster
                ; autoscaler to scale up. For non-synthetic pods, however, this state
                ; likely means something changed about the node we matched to. For example,
                ; if the ToBeDeletedByClusterAutoscaler taint gets added between when we
                ; saw available capacity on a node and when we submitted the pod to that
                ; node, then the pod will never get scheduled.
                (pod-unschedulable? (V1Pod->name pod) pod-status) {:state :pod/failed
                                                                   :reason "Unschedulable"}

                :else {:state :pod/waiting
                       :reason "Pending"})))]
      (if pod-preempted-timestamp
        (assoc synthesized-pod-state :pod-preempted-timestamp pod-preempted-timestamp)
        synthesized-pod-state))))

(defn pod->sandbox-file-server-container-state
  "From a V1Pod object, determine the state of the sandbox file server container, running, not running, or unknown.

  We're using this as first-order approximation that the sandbox file server is ready to serve files. We can later add
  health checks to the server to be more precise."
  [^V1Pod pod]
  (when pod
    (let [^V1PodStatus pod-status (.getStatus pod)
          container-statuses (.getContainerStatuses pod-status)
          ^V1ContainerStatus file-server-status
          (first
            (filter
              (fn [c]
                (= cook-container-name-for-file-server (.getName c)))
              container-statuses))]
      (cond
        (nil? file-server-status) :unknown
        (.getReady file-server-status) :running
        :else :not-running))))

(defn delete-pod
  "Kill this kubernetes pod. This is the same as deleting it."
  [^ApiClient api-client compute-cluster-name ^V1Pod pod]
  (let [api (CoreV1Api. api-client)
        ^V1DeleteOptions deleteOptions (-> (V1DeleteOptionsBuilder.) (.withPropagationPolicy "Background") .build)
        pod-name (-> pod .getMetadata .getName)
        pod-namespace (-> pod .getMetadata .getNamespace)]
    ; TODO: This likes to noisily throw NotFound multiple times as we delete away from kubernetes.
    ; I suspect our predicate of k8s-actual-state-equivalent needs tweaking.
    (timers/time! (metrics/timer "delete-pod" compute-cluster-name)
      (try
        (.deleteNamespacedPod
          api
          pod-name
          pod-namespace
          nil ; pretty
          nil ; dryRun
          nil ; gracePeriodSeconds
          nil ; oprphanDependents
          nil ; propagationPolicy
          deleteOptions
          )
        (catch JsonSyntaxException e
          ; Silently gobble this exception.
          ;
          ; The java API can throw a a JsonSyntaxException parsing the kubernetes reply!
          ; https://github.com/kubernetes-client/java/issues/252
          ; https://github.com/kubernetes-client/java/issues/86
          ;
          ; They actually advise catching the exception and moving on!
          ;
          (log/info "In" compute-cluster-name "compute cluster, caught the https://github.com/kubernetes-client/java/issues/252 exception deleting" pod-name))
        (catch ApiException e
          (meters/mark! (metrics/meter "delete-pod-errors" compute-cluster-name))
          (let [code (.getCode e)
                already-deleted? (contains? #{404} code)]
            (if already-deleted?
              (log/info e "In" compute-cluster-name "compute cluster, pod" pod-name "was already deleted")
              (throw e))))))))

(defn create-namespaced-pod
  "Delegates to the k8s API .createNamespacedPod function"
  [api namespace pod]
  (.createNamespacedPod api namespace pod nil nil nil))

(let [json (JSON.)]
  (defn launch-pod
    "Attempts to submit the given pod to k8s. If pod submission fails, we inspect the
    response code to determine whether or not this is a bad pod spec (e.g. the
    namespace doesn't exist on the cluster or there is an invalid environment
    variable name), or whether the failure is something less offensive (like a 409
    conflict error because we've attempted to re-submit a pod that the watch has not
    yet notified us exists). The function returns false if we should consider the
    launch operation failed."
    [api-client compute-cluster-name {:keys [launch-pod]} pod-name]
    (if launch-pod
      (let [{:keys [pod]} launch-pod
            pod-name-from-pod (-> pod .getMetadata .getName)
            namespace (-> pod .getMetadata .getNamespace)
            api (CoreV1Api. api-client)]
        (assert (= pod-name-from-pod pod-name)
                (str "Pod name from pod (" pod-name-from-pod ") "
                     "does not match pod name argument (" pod-name ")"))
        (log/info "In" compute-cluster-name "compute cluster, launching pod with name" pod-name "in namespace" namespace ":" (.serialize json pod))
        (try
          (timers/time! (metrics/timer "launch-pod" compute-cluster-name)
                        (create-namespaced-pod api namespace pod))
          true
          (catch ApiException e
            (let [code (.getCode e)
                  bad-pod-spec? (contains? #{400 404 422} code)]
              (log/info e "In" compute-cluster-name "compute cluster, error submitting pod with name" pod-name "in namespace" namespace
                        ", code:" code ", response body:" (.getResponseBody e))
              (if bad-pod-spec?
                (meters/mark! (metrics/meter "launch-pod-bad-spec-errors" compute-cluster-name))
                (meters/mark! (metrics/meter "launch-pod-good-spec-errors" compute-cluster-name)))
              (not bad-pod-spec?)))))
      (do
        ; Because of the complicated nature of task-metadata-seq, we can't easily run the pod
        ; creation code for launching a pod on a server restart. Thus, if we create an instance,
        ; store into datomic, but then the Cook Scheduler exits --- before k8s creates a pod (either
        ; the message isn't sent, or there's a k8s problem) --- we will be unable to create a new
        ; pod and we can't retry this at the kubernetes level.
        (log/info "Unable to launch pod because we do not reconstruct the pod on startup:" pod-name)
        false))))


;; TODO: Need the 'stuck pod scanner' to detect stuck states and move them into killed.
