(ns cook.kubernetes.api
  (:require [clojure.tools.logging :as log]
            [plumbing.core :as pc])
  (:import
    (com.google.gson JsonSyntaxException)
    (com.twosigma.cook.kubernetes WatchHelper)
    (io.kubernetes.client ApiClient ApiException)
    (io.kubernetes.client.apis CoreV1Api)
    (io.kubernetes.client.custom Quantity Quantity$Format)
    (io.kubernetes.client.models V1Pod V1Container V1Node V1Pod V1Container V1ResourceRequirements V1EnvVar
                                 V1ObjectMeta V1PodSpec V1PodStatus V1ContainerState V1DeleteOptionsBuilder
                                 V1DeleteOptions)
    (io.kubernetes.client.util Watch)
    (java.util.concurrent Executors ExecutorService)))


(def ^ExecutorService kubernetes-executor (Executors/newFixedThreadPool 2))

(defn handle-watch-updates
  "When a watch update occurs (for pods or nodes) update both the state atom as well as
  invoke the callback on the previous and new values for the key."
  [state-atom ^Watch watch key-fn callback]
  (while (.hasNext watch)
    (let [update (.next watch)
          item (.-object update)
          prev-item (get @state-atom (key-fn item))]
      (case (.-type update)
        "ADDED" (swap! state-atom (fn [m] (assoc m (key-fn item) item)))
        "MODIFIED" (swap! state-atom (fn [m] (assoc m (key-fn item) item)))
        "DELETED" (swap! state-atom (fn [m] (dissoc m (key-fn item)))))
      (when callback
        (try
          (callback prev-item item)
          (catch Exception e
            (log/error e "Error while processing callback")))))))

(defn initialize-pod-watch
  "Initialize the pod watch. This fills current-pods-atom with data and invokes the callback on pod changes.

  Note that we require that this function set the current-pods-atom before it finishes so that
  cook.kubernetes.compute-cluster/initialize-cluster works correctly."
  [^ApiClient api-client current-pods-atom pod-callback]
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
    (log/error "TODO: We should have two atoms, one for cook pods (that feeds into the controller) and one for
    all pods (that feeds into machine utilization then into offer generation)")
    (reset! current-pods-atom pod-name->pod)
    (let [watch (WatchHelper/createPodWatch api-client (-> current-pods
                                                           .getMetadata
                                                           .getResourceVersion))]
      (.submit kubernetes-executor ^Callable
      (fn []
        (try
          (handle-watch-updates current-pods-atom watch (fn [p] (-> p .getMetadata .getName)) pod-callback)
          (catch Exception e
            (log/error e "Error during watch"))
          (finally
            (.close watch)
            (initialize-pod-watch api-client current-pods-atom pod-callback))))))))

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
          (handle-watch-updates current-nodes-atom watch (fn [n] (-> n .getMetadata .getName)) nil)
          (catch Exception e
            (log/warn e "Error during node watch")
            (initialize-node-watch api-client current-nodes-atom))
          (finally
            (.close watch))))))))

(defn to-double
  "Map a quantity to a double, whether integer, double, or float."
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
  "Given a map from node-name to node, generate a map from node-name->resource-type-><capacity>"
  [node-name->node]
  (pc/map-vals (fn [^V1Node node]
                 (-> node .getStatus .getCapacity convert-resource-map))
               node-name->node))

(defn get-consumption
  "Given a map from pod-name to pod, generate a map from .....

  When accounting for resources, we use resource requests to determine how much is used, not limits.
  See https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#resource-requests-and-limits-of-pod-and-container"
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

(def cook-container-name-for-job
  "required-cook-job-container")

(defn ^V1Pod task-metadata->pod
  "Given a task-request and other data generate the kubernetes V1Pod to launch that task."
  [{:keys [task-id command container task-request hostname]}]
  (let [{:keys [resources]} task-request
        {:keys [mem cpus]} resources
        {:keys [docker]} container
        {:keys [image]} docker
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
        resources (V1ResourceRequirements.)]
    ; metadata
    (.setName metadata (str task-id))

    ; container
    (.setName container cook-container-name-for-job)
    (.setCommand container
                 ["/bin/sh" "-c" (:value command)])

    (.setEnv container (into [] env))
    (.setImage container image)

    (.putRequestsItem resources "memory"
                      (Quantity. (BigDecimal/valueOf ^double (* 1024 1024 mem))
                                 Quantity$Format/DECIMAL_SI))
    (.putLimitsItem resources "memory"
                    (Quantity. (BigDecimal/valueOf ^double (* 1024 1024 mem))
                               Quantity$Format/DECIMAL_SI))
    (.putRequestsItem resources "cpu"
                      (Quantity. (BigDecimal/valueOf ^double cpus)
                                 Quantity$Format/DECIMAL_SI))
    (.setResources container resources)

    ; pod-spec
    (.addContainersItem pod-spec container)
    (.setNodeName pod-spec hostname)
    (.setRestartPolicy pod-spec "Never")

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
  (let [namespace "cook"]
    ;; TODO: IF there's an error, log it and move on. We'll try again later.
    (if launch-pod
      (let [api (CoreV1Api. api-client)]
        (log/info "Launching pod" api launch-pod)
        (try
          (-> api
              (.createNamespacedPod namespace launch-pod nil nil nil))
          (catch ApiException e
            (log/error e "Error submitting pod:" (.getResponseBody e)))))
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
      (log/warn "Unimplemented Operation to launch a pod because we do not reconstruct the V1Pod on startup."))))


;; TODO: Need the 'stuck pod scanner' to detect stuck states and move them into killed.
