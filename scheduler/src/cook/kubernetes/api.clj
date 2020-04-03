(ns cook.kubernetes.api
  (:require [clj-time.core :as t]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cook.kubernetes.metrics :as metrics]
            [datomic.api :as d]
            [cook.config :as config]
            [cook.task :as task]
            [cook.tools :as util]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [plumbing.core :as pc])
  (:import
    (com.google.gson JsonSyntaxException)
    (com.twosigma.cook.kubernetes WatchHelper)
    (io.kubernetes.client ApiClient ApiException JSON)
    (io.kubernetes.client.apis CoreV1Api)
    (io.kubernetes.client.custom Quantity Quantity$Format IntOrString)
    (io.kubernetes.client.models V1Affinity V1Container V1ContainerPort V1ContainerState V1DeleteOptions
                                 V1DeleteOptionsBuilder V1EmptyDirVolumeSource V1EnvVar V1EnvVarBuilder
                                 V1HostPathVolumeSource V1HTTPGetAction V1Node V1NodeAffinity V1NodeSelector
                                 V1NodeSelectorRequirement V1NodeSelectorTerm V1ObjectMeta V1Pod V1PodCondition
                                 V1PodSecurityContext V1PodSpec V1PodStatus V1Probe V1ResourceRequirements
                                 V1Toleration V1VolumeBuilder V1Volume V1VolumeMount)
    (io.kubernetes.client.util Watch)
    (java.util.concurrent Executors ExecutorService)))


(def cook-pod-label "twosigma.com/cook-scheduler-job")
(def cook-synthetic-pod-job-uuid-label "twosigma.com/cook-scheduler-synthetic-pod-job-uuid")
(def cook-pool-label "cook-pool")
(def cook-sandbox-volume-name "cook-sandbox-volume")
(def cook-job-pod-priority-class "cook-workload")
(def cook-synthetic-pod-priority-class "synthetic-pod")
(def cook-synthetic-pod-name-prefix "synthetic")
(def k8s-hostname-label "kubernetes.io/hostname")

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

; Cook, Fenzo, and Mesos use MB for memory. Convert bytes from k8s to MB when passing to fenzo, and MB back to bytes
; when submitting to k8s.
(def memory-multiplier (* 1000 1000))

(defn is-cook-scheduler-pod
  "Is this a cook pod? Uses some-> so is null-safe."
  [^V1Pod pod compute-cluster-name]
  (= compute-cluster-name (some-> pod .getMetadata .getLabels (.get cook-pod-label))))

(defn make-atom-updater
  "Given a state atom, returns a callback that updates that state-atom when called with a key, prev item, and item."
  [state-atom]
  (fn
    [key prev-item item]
    (cond
      (and (nil? prev-item) (not (nil? item))) (swap! state-atom (fn [m] (assoc m key item)))
      (and (not (nil? prev-item)) (not (nil? item))) (swap! state-atom (fn [m] (assoc m key item)))
      (and (not (nil? prev-item)) (nil? item)) (swap! state-atom (fn [m] (dissoc m key))))))

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
  {:namespace (-> pod
                  .getMetadata
                  .getNamespace)
   :name (-> pod
             .getMetadata
             .getName)})

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

(declare initialize-pod-watch)
(defn ^Callable initialize-pod-watch-helper
  "Help creating pod watch. Returns a new watch Callable"
  [^ApiClient api-client compute-cluster-name all-pods-atom cook-pod-callback]
  (let [[current-pods namespaced-pod-name->pod] (get-all-pods-in-kubernetes api-client compute-cluster-name)
        ; 2 callbacks;
        callbacks
        [(make-atom-updater all-pods-atom) ; Update the set of all pods.
         (partial cook-pod-callback-wrap cook-pod-callback compute-cluster-name)] ; Invoke the cook-pod-callback if its a cook pod.
        old-all-pods @all-pods-atom]
    (log/info "In" compute-cluster-name "compute cluster, pod watch processing pods:" (keys namespaced-pod-name->pod))
    ; We want to process all changes through the callback process.
    ; So compute the delta between the old and new and process those via the callbacks.
    ; Note as a side effect, the callbacks mutate all-pods-atom
    (doseq [task (set/union (keys namespaced-pod-name->pod) (keys old-all-pods))]
      (log/info "In" compute-cluster-name "compute cluster, pod watch doing (startup) callback for" task)
      (doseq [callback callbacks]
        (try
          (callback task (get old-all-pods task) (get namespaced-pod-name->pod task))
          (catch Exception e
            (log/error e "In" compute-cluster-name
                       "compute cluster, pod watch error while processing callback for" task)))))

    (let [watch (WatchHelper/createPodWatch api-client (-> current-pods
                                                           .getMetadata
                                                           .getResourceVersion))]
      (fn []
        (try
          (log/info "In" compute-cluster-name "compute cluster, handling pod watch updates")
          (handle-watch-updates all-pods-atom watch get-pod-namespaced-key
                                callbacks)
          (catch Exception e
            (let [cause (.getCause e)]
              (if (and cause (instance? java.net.SocketTimeoutException cause))
                (log/info e "In" compute-cluster-name "compute cluster, pod watch timed out")
                (log/error e "In" compute-cluster-name "compute cluster, error during pod watch"))))
          (finally
            (.close watch)
            (initialize-pod-watch api-client compute-cluster-name all-pods-atom cook-pod-callback)))))))

(defn initialize-pod-watch
  "Initialize the pod watch. This fills all-pods-atom with data and invokes the callback on pod changes."
  [^ApiClient api-client compute-cluster-name all-pods-atom cook-pod-callback]
  (log/info "In" compute-cluster-name "compute cluster, initializing pod watch")
  ; We'll iterate trying to connect to k8s until the initialize-pod-watch-helper returns a watch function.
  (let [{:keys [reconnect-delay-ms]} (config/kubernetes)
        tmpfn (fn []
                (try
                  (log/info "In" compute-cluster-name "compute cluster, initializing pod watch helper")
                  (initialize-pod-watch-helper api-client compute-cluster-name all-pods-atom cook-pod-callback)
                  (catch Exception e
                    (log/error e "Error during pod watch initial setup of looking at pods for" compute-cluster-name
                               "and sleeping" reconnect-delay-ms "milliseconds before reconnect")
                    (Thread/sleep reconnect-delay-ms)
                    nil)))
        ^Callable first-success (->> tmpfn repeatedly (some identity))]
    (.submit kubernetes-executor ^Callable first-success)))

(declare initialize-node-watch)
(defn initialize-node-watch-helper
  "Help creating node watch. Returns a new watch Callable"
  [^ApiClient api-client compute-cluster-name current-nodes-atom]
  (let [api (CoreV1Api. api-client)
        current-nodes
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
        node-name->node (pc/map-from-vals (fn [^V1Node node]
                                            (-> node .getMetadata .getName))
                                          (.getItems current-nodes))]
    (reset! current-nodes-atom node-name->node)
    (let [watch (WatchHelper/createNodeWatch api-client (-> current-nodes .getMetadata .getResourceVersion))]
      (fn []
        (try
          (log/info "In" compute-cluster-name "compute cluster, handling node watch updates")
          (handle-watch-updates current-nodes-atom watch (fn [n] (-> n .getMetadata .getName))
                                [(make-atom-updater current-nodes-atom)]) ; Update the set of all nodes.
          (catch Exception e
            (log/warn e "Error during node watch for compute cluster" compute-cluster-name))
          (finally
            (.close watch)
            (initialize-node-watch api-client compute-cluster-name current-nodes-atom)))))))

(defn initialize-node-watch
  "Initialize the node watch. This fills current-nodes-atom with data and invokes the callback on pod changes."
  [^ApiClient api-client compute-cluster-name current-nodes-atom]
  (log/info "In" compute-cluster-name "compute cluster, initializing node watch")
  ; We'll iterate trying to connect to k8s until the initialize-node-watch-helper returns a watch function.
  (let [{:keys [reconnect-delay-ms]} (config/kubernetes)
        tmpfn (fn []
                (try
                  (log/info "In" compute-cluster-name "compute cluster, initializing node watch helper")
                  (initialize-node-watch-helper api-client compute-cluster-name current-nodes-atom)
                  (catch Exception e
                    (log/error e "Error during node watch initial setup of looking at nodes for" compute-cluster-name
                               "and sleeping" reconnect-delay-ms "milliseconds before reconnect")
                    (Thread/sleep reconnect-delay-ms)
                    nil)))
        ^Callable first-success (->> tmpfn repeatedly (some identity))]
    (.submit kubernetes-executor ^Callable first-success)))

(defn to-double
  "Map a quantity to a double, whether integer, double, or float."
  [^Quantity q]
  (-> q .getNumber .doubleValue))

(defn convert-resource-map
  "Converts a map of Kubernetes resources to a cook resource map {:mem double, :cpus double}"
  [m]
  {:mem (if (get m "memory")
          (-> m (get "memory") to-double (/ memory-multiplier))
          0.0)
   :cpus (if (get m "cpu")
           (-> m (get "cpu") to-double)
           0.0)})

(defn get-node-pool
  "Get the pool for a node. In the case of no pool, return 'no-pool"
  [^V1Node node]
  ; In the case of nil, we have taints-on-node == [], and we'll map to no-pool.
  (let [taints-on-node (or (some-> node .getSpec .getTaints) [])
        cook-pool-taint (filter #(= "cook-pool" (.getKey %)) taints-on-node)]
    (if (= 1 (count cook-pool-taint))
      (-> cook-pool-taint first .getValue)
      "no-pool")))

(defn pod->node-name
  "Given a pod, returns the node name on the pod spec"
  [^V1Pod pod]
  (or (some-> pod .getSpec .getNodeName)
      (some-> pod .getSpec .getNodeSelector (get k8s-hostname-label))))

(defn num-pods-on-node
  "Returns the number of pods assigned to the given node"
  [node-name pods]
  (let [node-name->pods (group-by pod->node-name pods)]
    (-> node-name->pods (get node-name []) count)))

(defn node-schedulable?
  "Can we schedule on a node. For now, yes, unless there are other taints on it or it contains any label in the
  node-blocklist-labels list.
  TODO: Incorporate other node-health measures here."
  [^V1Node node pod-count-capacity pods node-blocklist-labels]
  (if (nil? node)
    false
    (let [taints-on-node (or (some-> node .getSpec .getTaints) [])
          other-taints (remove #(contains?
                                  #{"cook-pool" k8s-deletion-candidate-taint}
                                  (.getKey %))
                               taints-on-node)
          node-name (some-> node .getMetadata .getName)
          pods-on-node (num-pods-on-node node-name pods)
          labels-on-node (or (some-> node .getMetadata .getLabels) {})
          matching-node-blocklist-keyvals (select-keys labels-on-node node-blocklist-labels)]
      (cond  (seq other-taints) (do
                                  (log/info "Filtering out" node-name "because it has taints" other-taints)
                                  false)
             (>= pods-on-node pod-count-capacity) (do
                                                    (log/info "Filtering out" node-name "because it is at or above its pod count capacity of"
                                                              pod-count-capacity "(" pods-on-node ")")
                                                    false)
             (seq matching-node-blocklist-keyvals) (do
                                                    (log/info "Filtering out" node-name "because it has node blocklist labels" matching-node-blocklist-keyvals)
                                                    false)
             :else true))))

(defn get-capacity
  "Given a map from node-name to node, generate a map from node-name->resource-type-><capacity>"
  [node-name->node]
  (pc/map-vals (fn [^V1Node node]
                 (-> node .getStatus .getAllocatable convert-resource-map))
               node-name->node))

(defn get-consumption
  "Given a map from pod-name to pod, generate a map from node-name->resource-type->capacity

  When accounting for resources, we use resource requests to determine how much is used, not limits.
  See https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#resource-requests-and-limits-of-pod-and-container"
  [pods]
  (let [node-name->pods (group-by pod->node-name pods)
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
    (.setKey toleration "cook-pool")
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
  "Given the required resources for a job, add to them the resources for any sidecars that will be launched with the job."
  [{:keys [cpus mem] :as resources}]
  (if-let [{:keys [cpu-request memory-request]} (-> (config/kubernetes) :sidecar :resource-requirements)]
    (assoc resources
      :cpus (add-as-decimals cpus cpu-request)
      :mem (add-as-decimals mem memory-request))
    resources))

(defn- add-node-selector
  "Adds a node selector with the given key and val to the given pod spec"
  [^V1PodSpec pod-spec key val]
  (.setNodeSelector pod-spec (-> pod-spec
                                 .getNodeSelector
                                 (assoc key val))))

(defn- checkpoint->volume-mounts
  "Get custom volume mounts needed for checkpointing"
  [{:keys [mode volume-mounts]} checkpointing-tools-volume]
  (when mode
    (map (fn [{:keys [path sub-path]}] (make-volume-mount checkpointing-tools-volume path sub-path true)) volume-mounts)))

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

(defn ^V1Pod task-metadata->pod
  "Given a task-request and other data generate the kubernetes V1Pod to launch that task."
  [namespace compute-cluster-name compute-cluster-node-blocklist-labels
   {:keys [task-id command container task-request hostname pod-labels pod-hostnames-to-avoid
           pod-priority-class pod-supports-cook-init? pod-supports-cook-sidecar?]
    :or {pod-priority-class cook-job-pod-priority-class
         pod-supports-cook-init? true
         pod-supports-cook-sidecar? true}}]
  (let [{:keys [scalar-requests job]} task-request
        ;; NOTE: The scheduler's adjust-job-resources-for-pool-fn may modify :resources,
        ;; whereas :scalar-requests always contains the unmodified job resource values.
        {:strs [mem cpus]} scalar-requests
        {:keys [docker volumes]} container
        {:keys [image parameters]} docker
        {:keys [job/progress-output-file job/progress-regex-string job/checkpoint]} job
        {:keys [environment]} command
        pod (V1Pod.)
        pod-spec (V1PodSpec.)
        metadata (V1ObjectMeta.)
        container (V1Container.)
        resources (V1ResourceRequirements.)
        labels (assoc pod-labels cook-pod-label compute-cluster-name)
        pool-name (some-> job :job/pool :pool/name)
        security-context (make-security-context parameters (:user command))
        sandbox-dir (:default-workdir (config/kubernetes))
        workdir (get-workdir parameters sandbox-dir)
        {:keys [volumes volume-mounts sandbox-volume-mount-fn]} (make-volumes volumes sandbox-dir)
        {:keys [custom-shell init-container sidecar default-checkpoint-config]} (config/kubernetes)
        checkpoint (when checkpoint (merge default-checkpoint-config (util/job-ent->checkpoint job)))
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
        checkpoint-volume-mounts (checkpoint->volume-mounts checkpoint init-container-workdir-volume)
        sandbox-env {"COOK_SANDBOX" sandbox-dir
                     "HOME" sandbox-dir
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
        main-env-vars (make-filtered-env-vars main-env)]

    ; metadata
    (.setName metadata (str task-id))
    (.setNamespace metadata namespace)
    (.setLabels metadata labels)

    ; container
    (.setName container cook-container-name-for-job)
    (.setCommand container
                 (conj (or (when use-cook-init? custom-shell) default-shell)
                       (:value command)))
    (.setEnv container main-env-vars)
    (.setImage container image)

    ; allocate a TTY to support tools that need to read from stdin
    (.setTty container true)
    (.setStdin container true)

    (.putRequestsItem resources "memory" (double->quantity (* memory-multiplier mem)))
    (.putLimitsItem resources "memory" (double->quantity (* memory-multiplier mem)))
    (.putRequestsItem resources "cpu" (double->quantity cpus))
    ; We would like to make this burstable, but for various reasons currently need pods to run in the
    ; "Guaranteed" QoS which requires limits for both memory and cpu.
    (.putLimitsItem resources "cpu" (double->quantity cpus))
    (.setResources container resources)
    (.setVolumeMounts container (filterv some? (conj (concat volume-mounts checkpoint-volume-mounts)
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

          (.setVolumeMounts container [(init-container-workdir-volume-mount-fn false)
                                       (scratch-space-volume-mount-fn false)])
          (.addInitContainersItem pod-spec container))))

    ; sandbox file server container
    (when use-cook-sidecar?
      (when-let [{:keys [command image port resource-requirements]} sidecar]
        (let [{:keys [cpu-request cpu-limit memory-request memory-limit]} resource-requirements
              container (V1Container.)
              resources (V1ResourceRequirements.)
              readiness-probe (V1Probe.)
              http-get-action (V1HTTPGetAction.)]
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

          (.setPort http-get-action (-> port int IntOrString.))
          (.setPath http-get-action "readiness-probe")
          (.setHttpGet readiness-probe http-get-action)
          (.setReadinessProbe container readiness-probe)

          ; resources
          (.putRequestsItem resources "cpu" (double->quantity cpu-request))
          (.putLimitsItem resources "cpu" (double->quantity cpu-limit))
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
      (when (or (seq pod-hostnames-to-avoid)
                (seq compute-cluster-node-blocklist-labels))
        ; Use node "anti"-affinity to disallow scheduling on nodes with particular labels
        ; (https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#node-affinity)
        (let [affinity (V1Affinity.)
              node-affinity (V1NodeAffinity.)
              node-selector (V1NodeSelector.)
              node-selector-term (V1NodeSelectorTerm.)]

          ; Disallow scheduling on hostnames we're being told to avoid (if any)
          (when (seq pod-hostnames-to-avoid)
            (let [node-selector-requirement-k8s-hostname-label (V1NodeSelectorRequirement.)]
              (.setKey node-selector-requirement-k8s-hostname-label k8s-hostname-label)
              (.setOperator node-selector-requirement-k8s-hostname-label "NotIn")
              (run! #(.addValuesItem node-selector-requirement-k8s-hostname-label %) pod-hostnames-to-avoid)
              (.addMatchExpressionsItem node-selector-term node-selector-requirement-k8s-hostname-label)))

          ; Disallow scheduling on nodes with blocklist labels (if any)
          (when (seq compute-cluster-node-blocklist-labels)
            (run!
              (fn add-node-selector-term-for-blocklist-label
                [node-blocklist-label]
                (let [node-selector-requirement-blocklist-label (V1NodeSelectorRequirement.)]
                  (.setKey node-selector-requirement-blocklist-label node-blocklist-label)
                  (.setOperator node-selector-requirement-blocklist-label "DoesNotExist")
                  (.addMatchExpressionsItem node-selector-term node-selector-requirement-blocklist-label)))
              compute-cluster-node-blocklist-labels))

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
                                               sidecar-workdir-volume)))
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
                                    container-statuses))]
      ; TODO: When we add logic here that supports detecting pods that have a i-am-being-preempted label, we need to modify
      ; the controller to set the failure reason to unknown for pods in  :running,:missing.
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
                   :reason "Pending"}))))))

(defn pod->sandbox-file-server-container-state
  "From a V1Pod object, determine the state of the sandbox file server container, running, not running, or unknown.

  We're using this as first-order approximation that the sandbox file server is ready to serve files. We can later add
  health checks to the server to be more precise."
  [^V1Pod pod]
  (when pod
    (let [^V1PodStatus pod-status (.getStatus pod)
          container-statuses (.getContainerStatuses pod-status)
          file-server-status (first (filter (fn [c] (= cook-container-name-for-file-server (.getName c)))
                                            container-statuses))]
      (cond
        (nil? file-server-status) :unknown
        (.isReady file-server-status) :running
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
          deleteOptions
          nil                                               ; pretty
          nil                                               ; dryRun
          nil                                               ; gracePeriodSeconds
          nil                                               ; oprphanDependents
          nil                                               ; propagationPolicy
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
