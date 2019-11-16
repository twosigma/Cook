(ns cook.kubernetes.api
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cook.config :as config]
            [me.raynes.conch :as sh]
            [plumbing.core :as pc])
  (:import
    (com.google.gson JsonSyntaxException)
    (com.twosigma.cook.kubernetes WatchHelper)
    (io.kubernetes.client ApiClient ApiException)
    (io.kubernetes.client.apis CoreV1Api)
    (io.kubernetes.client.custom Quantity Quantity$Format)
    (io.kubernetes.client.models V1Pod V1Container V1Node V1Pod V1Container V1ResourceRequirements V1EnvVar
                                 V1ObjectMeta V1PodSpec V1PodStatus V1ContainerState V1DeleteOptionsBuilder
                                 V1DeleteOptions V1HostPathVolumeSource V1VolumeMount V1VolumeBuilder V1Taint
                                 V1Toleration V1PodSecurityContext V1EmptyDirVolumeSource V1EnvVarBuilder)
    (io.kubernetes.client.util Watch)
    (java.util.concurrent Executors ExecutorService)
    (java.util UUID)))


(def cook-pod-label "twosigma.com/cook-scheduler-job")

(def ^ExecutorService kubernetes-executor (Executors/newCachedThreadPool))

; Cook, Fenzo, and Mesos use MB for memory. Convert bytes from k8s to MB when passing to fenzo, and MB back to bytes
; when submitting to k8s.
(def memory-multiplier (* 1000 1000))

(sh/let-programs
  [_id "/usr/bin/id"]
  (defn uid [user-name]
    (Long/parseLong (str/trim (_id "-u" user-name))))
  (defn gid [user-name]
    (Long/parseLong (str/trim (_id "-g" user-name)))))

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
  [api-client]
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
    [current-pods namespaced-pod-name->pod]))

(defn initialize-pod-watch
  "Initialize the pod watch. This fills all-pods-atom with data and invokes the callback on pod changes."
  [^ApiClient api-client compute-cluster-name all-pods-atom cook-pod-callback]
  (let [[current-pods namespaced-pod-name->pod] (get-all-pods-in-kubernetes api-client)
        ; 2 callbacks;
        callbacks
        [(make-atom-updater all-pods-atom) ; Update the set of all pods.
         (partial cook-pod-callback-wrap cook-pod-callback compute-cluster-name)] ; Invoke the cook-pod-callback if its a cook pod.
        old-all-pods @all-pods-atom]
    (log/info "Processing pods for compute cluster" compute-cluster-name ":" (keys namespaced-pod-name->pod))
    ; We want to process all changes through the callback process.
    ; So compute the delta between the old and new and process those via the callbacks.
    ; Note as a side effect, the callbacks mutate all-pods-atom
    (doseq [task (set/union (keys namespaced-pod-name->pod) (keys old-all-pods))]
      (doseq [callback callbacks]
        (try
          (log/info "In" compute-cluster-name "compute cluster, doing (startup) callback for" task)
          (callback task (get old-all-pods task) (get namespaced-pod-name->pod task))
          (catch Exception e
            (log/error e "Error while processing callback for" task)))))

    (let [watch (WatchHelper/createPodWatch api-client (-> current-pods
                                                           .getMetadata
                                                           .getResourceVersion))]
      (.submit kubernetes-executor ^Callable
      (fn []
        (try
          (handle-watch-updates all-pods-atom watch get-pod-namespaced-key
                                callbacks)
          (catch Exception e
            (log/error e "Error during watch"))
          (finally
            (.close watch)
            (initialize-pod-watch api-client compute-cluster-name all-pods-atom cook-pod-callback))))))))

(defn initialize-node-watch
  "Initialize the node watch. This fills current-nodes-atom with data and invokes the callback on pod changes."
  [^ApiClient api-client current-nodes-atom]
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
          (handle-watch-updates current-nodes-atom watch (fn [n] (-> n .getMetadata .getName))
                                [(make-atom-updater current-nodes-atom)]) ; Update the set of all nodes.
          (catch Exception e
            (log/warn e "Error during node watch"))
          (finally
            (.close watch)
            (initialize-node-watch api-client current-nodes-atom))))))))

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

(defn node-schedulable?
  "Can we schedule on a node. For now, yes, unless there are other taints on it. TODO: Incorporate other node-health measures here."
  [^V1Node node]
  (if (nil? node)
    false
    (let [taints-on-node (or (some-> node .getSpec .getTaints) [])
          other-taints (remove #(= "cook-pool" (.getKey %)) taints-on-node)
          schedulable (zero? (count other-taints))]
      (when-not schedulable
        (log/info "Filtering out" (some-> node .getMetadata .getName) "because it has taints" other-taints))
      schedulable)))

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
  [namespaced-pod-name->pod]
  (let [node-name->pods (group-by (fn [^V1Pod p] (-> p .getSpec .getNodeName))
                                  (vals namespaced-pod-name->pod))
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

(def cook-container-name-for-job
  "required-cook-job-container")

(defn double->quantity
  "Converts a double to a Quantity"
  [^double value]
  (Quantity. (BigDecimal/valueOf value)
             Quantity$Format/DECIMAL_SI))

(defn make-volumes
  "Converts a list of cook volumes to kubernetes volumes and volume mounts."
  [cook-volumes]
  (let [{:keys [disallowed-container-paths]} (config/kubernetes)
        filtered-cook-volumes (filter (fn [{:keys [host-path container-path]}]
                                        (let [path (or container-path host-path)]
                                          (if disallowed-container-paths
                                            (not (contains? disallowed-container-paths
                                                            path))
                                            true)))
                                      cook-volumes)
        host-path->name (pc/map-from-keys (fn [_] (str (UUID/randomUUID)))
                                          (map :host-path filtered-cook-volumes))
        volumes (map (fn [{:keys [host-path]}]
                       (let [host-path-source (V1HostPathVolumeSource.)]
                         ; host path
                         (.setPath host-path-source host-path)
                         (.setType host-path-source "DirectoryOrCreate")

                         ; volume
                         (-> (V1VolumeBuilder.)
                             (.withName (host-path->name host-path))
                             (.withHostPath host-path-source)
                             (.build))))
                     filtered-cook-volumes)
        volume-mounts (map (fn [{:keys [container-path host-path mode]
                                 :or {mode "RO"}}]
                             (let [container-path (or container-path host-path)
                                   volume-mount (V1VolumeMount.)
                                   read-only (not (= mode "RW"))]
                               (.setName volume-mount (host-path->name host-path))
                               (.setMountPath volume-mount container-path)
                               (.setReadOnly volume-mount read-only)
                               volume-mount))
                           filtered-cook-volumes)]
    {:volumes volumes
     :volume-mounts volume-mounts}))

(defn toleration-for-pool
  "For a given cook pool name, create the right V1Toleration so that Cook will ignore that cook-pool taint."
  [pool-name]
  (let [^V1Toleration toleration (V1Toleration.)]
    (.setKey toleration "cook-pool")
    (.setValue toleration pool-name)
    (.setOperator toleration "Equal")
    (.setEffect toleration "NoSchedule")
    toleration))

(defn param-env-vars
  [params]
  (->> params
       (filter (fn [{:keys [key]}]
                 (= key "env")))
       (map (fn [{:keys [value]}]
              (let [[env-var-name env-var-value] (str/split value #"=")
                    env (V1EnvVar.)]
                (.setName env (str env-var-name))
                (.setValue env (str env-var-value))
                env)))))

(defn make-security-context
  [params user]
  (let [user-param (->> params
                        (filter (fn [{:keys [key]}] (= key "user")))
                        first)
        [uid gid] (if user-param
                    (let [[uid gid] (str/split (:value user-param) #":")]
                      [(Long/parseLong uid) (Long/parseLong gid)])
                    [(uid user) (gid user)])
        security-context (V1PodSecurityContext.)]
    (.setRunAsUser security-context uid)
    (.setRunAsGroup security-context gid)
    security-context))

(defn get-workdir
  [params]
  (let [workdir-param (->> params
                           (filter (fn [{:keys [key]}] (= key "workdir")))
                           first)]
    (if workdir-param
      (:value workdir-param)
      (:default-workdir (config/kubernetes)))))

(defn filter-env-vars
  [env-vars]
  (let [{:keys [disallowed-var-names]} (config/kubernetes)]
    (->> env-vars
         (filter (fn [^V1EnvVar var]
                   (not (contains? disallowed-var-names (.getName var)))))
         (into []))))

(defn ^V1Pod task-metadata->pod
  "Given a task-request and other data generate the kubernetes V1Pod to launch that task."
  [namespace compute-cluster-name {:keys [task-id command container task-request hostname]}]
  (let [{:keys [resources job]} task-request
        {:keys [mem cpus]} resources
        {:keys [docker volumes]} container
        {:keys [image parameters]} docker
        pod (V1Pod.)
        pod-spec (V1PodSpec.)
        metadata (V1ObjectMeta.)
        container (V1Container.)
        env (map (fn [[k v]]
                   (let [env (V1EnvVar.)]
                     (.setName env (str k))
                     (.setValue env (str v))
                     env))
                 (:environment command))
        resources (V1ResourceRequirements.)
        labels {cook-pod-label compute-cluster-name}
        pool-name (some-> job :job/pool :pool/name)
        {:keys [volumes volume-mounts]} (make-volumes volumes)
        security-context (make-security-context parameters (:user command))
        workdir (get-workdir parameters)
        workdir-volume (-> (V1VolumeBuilder.)
                           (.withName "cook-workdir")
                           (.withEmptyDir (V1EmptyDirVolumeSource.))
                           (.build))
        workdir-volume-mount (V1VolumeMount.)
        workdir-env-vars [(-> (V1EnvVarBuilder.)
                              (.withName "HOME")
                              (.withValue workdir)
                              (.build))
                          (-> (V1EnvVarBuilder.)
                              (.withName "MESOS_SANDBOX")
                              (.withValue workdir)
                              (.build))]]
    ; workdir mount
    (.setName workdir-volume-mount "cook-workdir")
    (.setMountPath workdir-volume-mount workdir)
    (.setReadOnly workdir-volume-mount false)

    ; metadata
    (.setName metadata (str task-id))
    (.setNamespace metadata namespace)
    (.setLabels metadata labels)

    ; container
    (.setName container cook-container-name-for-job)
    (.setCommand container
                 ["/bin/sh" "-c" (:value command)])

    (.setEnv container (-> []
                           (into env)
                           (into (param-env-vars parameters))
                           (into workdir-env-vars)
                           filter-env-vars))
    (.setImage container image)

    (.putRequestsItem resources "memory" (double->quantity (* memory-multiplier mem)))
    (.putLimitsItem resources "memory" (double->quantity (* memory-multiplier mem)))
    (.putRequestsItem resources "cpu" (double->quantity cpus))
    ; We would like to make this burstable, but for various reasons currently need pods to run in the
    ; "Guaranteed" QoS which requires limits for both memory and cpu.
    (.putLimitsItem resources "cpu" (double->quantity cpus))
    (.setResources container resources)
    (.setVolumeMounts container (into [] (conj volume-mounts workdir-volume-mount)))
    (.setWorkingDir container workdir)

    ; pod-spec
    (.addContainersItem pod-spec container)
    (.setNodeName pod-spec hostname)
    (.setRestartPolicy pod-spec "Never")
    (when pool-name
      (.addTolerationsItem pod-spec (toleration-for-pool pool-name)))
    (.setVolumes pod-spec (into [] (conj volumes workdir-volume)))
    (.setSecurityContext pod-spec security-context)

    ; pod
    (.setMetadata pod metadata)
    (.setSpec pod pod-spec)

    pod))

(defn V1Pod->name
  "Extract the name of a pod from the pod itself"
  [^V1Pod pod]
  (-> pod .getMetadata .getName))


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
          ; * The main job container will always be named required-cookJobContainer
          ; * We want a pod to be considered failed if any container with the name required-* fails or any container
          ;   with the name extra-* fails.
          ; * A job may have additional containers with the name aux-*
          job-status (first (filter (fn [c] (= cook-container-name-for-job (.getName c)))
                                    container-statuses))]
      (if (some-> pod .getMetadata .getDeletionTimestamp)
        ; If a pod has been ordered deleted, treat it as if it was gone, It's being async removed.
        {:state :missing :reason "Pod was explicitly deleted"}
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

          {:state :pod/waiting
           :reason "Pending"})))))

(defn kill-task
  "Kill this kubernetes pod"
  [^ApiClient api-client ^V1Pod pod]
  (let [api (CoreV1Api. api-client)
        ^V1DeleteOptions deleteOptions (-> (V1DeleteOptionsBuilder.) (.withPropagationPolicy "Background") .build)]
    (try
      (.deleteNamespacedPod
        api
        (-> pod .getMetadata .getName)
        (-> pod .getMetadata .getNamespace)
        deleteOptions
        nil ; pretty
        nil ; dryRun
        nil ; gracePeriodSeconds
        nil ; oprphanDependents
        nil ; propagationPolicy
        )
      (catch JsonSyntaxException e
        (log/info "Caught the https://github.com/kubernetes-client/java/issues/252 exception.")
        ; Silently gobble this exception.
        ;
        ; The java API can throw a a JsonSyntaxException parsing the kubernetes reply!
        ; https://github.com/kubernetes-client/java/issues/252
        ; https://github.com/kubernetes-client/java/issues/86
        ;
        ; They actually advise catching the exception and moving on!
        ;
        ))))


(defn remove-finalization-if-set-and-delete
  "Remove finalization for a pod if it's there. No-op if it's not there.
  We always delete unconditionally, so finalization doesn't matter."
  [^ApiClient api-client ^V1Pod pod]
  ; TODO: This likes to noisily throw NotFound multiple times as we delete away from kubernetes.
  ; I suspect our predicate of existing-states-equivalent needs tweaking.
  (kill-task api-client pod))

(defn launch-task
  "Given a V1Pod, launch it."
  [api-client {:keys [launch-pod] :as expected-state-dict}]
  ;; TODO: make namespace configurable
  (if launch-pod
    (let [{:keys [pod]} launch-pod
          pod-name (-> pod .getMetadata .getName)
          namespace (-> pod .getMetadata .getNamespace)
          ;; TODO: IF there's an error, log it and move on. We'll try again later.
          api (CoreV1Api. api-client)]
      (log/info "Launching pod with name" pod-name "in namespace" namespace ":" pod)
      (try
        (-> api
            (.createNamespacedPod namespace pod nil nil nil))
        (catch ApiException e
          (log/error e "Error submitting pod with name" pod-name "in namespace" namespace ":" (.getResponseBody e)))))
    ; Because of the complicated nature of task-metadata-seq, we can't easily run the V1Pod creation code for a
    ; launching pod on a server restart. Thus, if we create a task, store into datomic, but then the cook scheduler
    ; fails --- before kubernetes creates a pod (either the message isn't sent, or there's a kubernetes problem) ---
    ; we will be unable to create a new V1Pod and we can't retry this at the kubernetes level.
    ;
    ; Eventually, the stuck pod detector will recognize the stuck pod, kill the task, and a cook-level retry will make
    ; a new task.
    ;
    ; Because the issue is relatively rare and auto-recoverable, we're going to punt on the task-metadata-seq refactor need
    ; to handle this situation better.
    (log/warn "Unimplemented Operation to launch a pod because we do not reconstruct the V1Pod on startup.")))


;; TODO: Need the 'stuck pod scanner' to detect stuck states and move them into killed.
