(ns cook.kubernetes.api
  (:require [clojure.tools.logging :as log]
            [plumbing.core :as pc])
  (:import
    (com.twosigma.cook.kubernetes WatchHelper)
    (io.kubernetes.client.models V1Pod V1PodBuilder V1Container)
           (io.kubernetes.client.custom Quantity)
           (io.kubernetes.client ApiClient Configuration ApiException)
           (io.kubernetes.client.util Config Watch)
           (io.kubernetes.client.apis CoreV1Api)
           (io.kubernetes.client.models V1Node V1Pod V1Container V1ResourceRequirements V1PodBuilder V1EnvVar V1ObjectMeta V1PodSpec)
           (io.kubernetes.client.custom Quantity Quantity$Format)
           (java.util UUID)
           (java.util.concurrent Executors ExecutorService)
           ))


(def ^ExecutorService kubernetes-executor (Executors/newFixedThreadPool 2))

(defn handle-watch-updates
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
    (reset! current-pods-atom pod-name->pod)
    (let [watch (WatchHelper/createPodWatch api-client (-> current-pods
                                                           .getMetadata
                                                           .getResourceVersion))]
      (.submit kubernetes-executor ^Callable
      (fn []
        (try
          (handle-watch-updates current-pods-atom watch (fn [p] (-> p .getMetadata .getName)) pod-callback)
          (catch Exception e
            (log/error "Error during watch: " (-> e .toString))
            (.close watch)
            (initialize-pod-watch api-client current-pods-atom pod-callback))))))))

(defn initialize-node-watch [^ApiClient api-client current-nodes-atom]
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



(defn task-metadata->pod
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
                     (.setName env k)
                     (.setValue env v)
                     env))
                 (:environment command))
        resources (V1ResourceRequirements.)]
    ; metadata
    (.setName metadata (str task-id))

    ; container
    (.setName container "job")
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
;;
;;  This file contains accessor functions for all kubernetes API. That way we can conveniently mock these functions in clojure.
;;

(defn TODO
  []
  (throw (UnsupportedOperationException. "TODO")))

(defn V1Pod->name
  "Extract the name of a pod from the pod itself"
  [^V1Pod pod]
  (-> pod .getMetadata .getName))

(defn synthesize-pod-state
  "Given the current pod metadata/information, synthesize the pod state (running, pending, doomed etc.)"
  [^V1Pod pod]
  :synthesized/TODO-pod) ; TOOD

(defn synthesize-node-state
  "Given the current node metadata/information, synthesize the node state -- whether or not we can/should launch things on it."
  [^V1Node node]
  :synthesized/TODO-node) ; TOOD

(defn ^UUID pod-name->task-uuid
  "Handles decoding the UUID from the name cook_uuid_43222432_......"
  [pod-name]
  (TODO)) ; TODO

(defn ^UUID task-uuid->pod-name
  "Handles encoding the UUID to the name cook_uuid_43222432_......"
  [task-uuid]
  "TODO: Implement task-uuid->pod-name") ; TODO

(defn remove-finalization-if-set-and-delete
  "Remove finalization for a pod if its there. No-op if its not there."
  [api-client ^V1Pod pod]
  (TODO))

(defn kill-task
  "Kill this kubernetes pod"
  [^ApiClient api-client ^V1Pod pod]
  (let [api (CoreV1Api. api-client)]
    (.deleteNamespacedPod ; TODO: Double check arguments.
      api
      (-> pod .getMetadata .getName)
      (-> pod .getMetadata .getNamespace)
      nil
      nil
      nil
      nil
      nil
      nil)))

(defn launch-task
  "Given a pod-name use lookup the associated task, extract the parts needed to syntehsize the kubenretes object and go"
  [api-client {:keys [launch-pod] :as expected-state-dict}]
  ;; TODO: IF there's an error, log it and move on. We'll try again later.
  (let [api (CoreV1Api. api-client)]
    (log/info "Launching pod" api launch-pod)
    (try
      (-> api
          (.createNamespacedPod "cook" launch-pod nil nil nil))
      (catch ApiException e
        (log/error e "Error submitting pod:" (.getResponseBody e))))))

;; TODO: Need the 'stuck pod scanner' to detect stuck states and move them into killed.


(defn rebuild-watch-pods
  ;;   We treat 'finalization removed' on a failed/succeeded task the same as if it didn't exist in kubernetes at all.
  [] ; TODO.
  )