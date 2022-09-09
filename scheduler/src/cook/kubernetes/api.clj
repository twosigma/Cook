(ns cook.kubernetes.api
  (:require [better-cond.core :as b]
            [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [cook.cached-queries :as cached-queries]
            [cook.config :as config]
            [cook.config-incremental :as config-incremental]
            [cook.kubernetes.metrics :as metrics]
            [cook.log-structured :as log-structured]
            [cook.monitor :as monitor]
            [cook.passport :as passport]
            [cook.prometheus-metrics :as prom]
            [cook.regexp-tools :as regexp-tools]
            [cook.rest.api :as api]
            [cook.scheduler.constraints :as constraints]
            [cook.task :as task]
            [cook.tools :as tools]
            [cook.util :as util]
            [datomic.api :as d]
            [metrics.counters :as counters]
            [metrics.histograms :as histograms]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [plumbing.core :as pc])
  (:import (com.google.common.util.concurrent ThreadFactoryBuilder)
           (com.google.gson JsonSyntaxException)
           (com.twosigma.cook.kubernetes FinalizerHelper WatchHelper)
           (io.kubernetes.client.custom IntOrString Quantity Quantity$Format)
           (io.kubernetes.client.openapi ApiClient ApiException JSON)
           (io.kubernetes.client.openapi.apis CoreV1Api)
           (io.kubernetes.client.openapi.models
             CoreV1Event V1Affinity V1Container V1ContainerPort V1ContainerState V1ContainerStatus V1DeleteOptions
             V1DeleteOptionsBuilder V1EmptyDirVolumeSource V1EnvVar V1EnvVarSource V1HostPathVolumeSource
             V1HTTPGetAction V1LabelSelector V1ObjectFieldSelector V1Node V1NodeList V1NodeAffinity V1NodeSelector
             V1NodeSelectorRequirement V1NodeSelectorTerm V1ObjectMeta V1ObjectReference V1Pod V1PodAffinityTerm
             V1PodAntiAffinity V1PodCondition V1PodSecurityContext V1PodSpec V1PodStatus V1Probe V1ResourceRequirements
             V1Toleration V1Volume V1VolumeBuilder V1VolumeMount V1Taint)
           (io.kubernetes.client.util Watch)
           (java.net SocketTimeoutException)
           (java.util List)
           (java.util.concurrent Executors ExecutorService)))


(def cook-pod-label "twosigma.com/cook-scheduler-job")
(def cook-synthetic-pod-job-uuid-label "twosigma.com/cook-scheduler-synthetic-pod-job-uuid")
(def resource-owner-label "resource-owner")
(def cook-sandbox-volume-name "cook-sandbox-volume")
(def cook-job-pod-priority-class "cook-workload")
(def cook-synthetic-pod-priority-class "synthetic-pod")
(def cook-synthetic-pod-name-prefix "synthetic")
(def gpu-node-taint "nvidia.com/gpu")
(def k8s-hostname-label "kubernetes.io/hostname")
; This pod annotation signals to the cluster autoscaler that
; it's safe to remove the node on which the pod is running
(def k8s-safe-to-evict-annotation "cluster-autoscaler.kubernetes.io/safe-to-evict")
; We want nodes older than 5 minutes to get this taint. A separate process taints those old nodes
; with this taint. Cook ignores it for scheduling purposes, but synthetic pods are configured so they
; don't ignore it. Cook creates synthetic pods as a scaling signal. When they run on existing nodes,
; the signal we intend to send to the autoscaler is attenuated and we autoscale much less than intended.
;
; We prefix the taint with the ignore-taint string so that the autoscaler won't use this when constructing exemplar new nodes.
(def tenured-node-taint "ignore-taint.cluster-autoscaler.kubernetes.io/cook-node-tenured")

(def default-shell
  "Default shell command used by our k8s scheduler to wrap and launch a job command
   when no custom shell is provided in the kubernetes compute cluster config."
  ["/bin/sh" "-c"
   (str "exec 1>$COOK_SANDBOX/stdout; "
        "exec 2>$COOK_SANDBOX/stderr; "
        "exec /bin/sh -c \"$1\"")
   "--"])

(defn synthetic-pod?
  "Given a pod name, returns true if it has the synthetic pod prefix"
  [pod-name]
  (str/starts-with? pod-name cook-synthetic-pod-name-prefix))

(defn kubernetes-scheduler-pod?
  "Given a pod, returns true if its scheduling is handled by Kubernetes."
  [^V1Pod pod]
  (= "kubernetes" (some-> pod .getMetadata .getLabels (.get "twosigma.com/cook-scheduler"))))

(defn pod-name->job-uuid
  "If a pod is synthetic, return the uuid of the job it was created for"
  [pod-name]
  (when (synthetic-pod? pod-name)
    (re-find #"[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}" pod-name)))

(defn pod-name->instance-uuid
  "If a pod is not synthetic, return the instance id associated with its pod-name"
  [pod-name]
  (when-not (synthetic-pod? pod-name)
    pod-name))

(defn pod-name->job-map
  "Returns event-map with added fields :instance-uuid (if pod is not synthetic), job-name, job-uuid, pool, and user."
  [pod-name]
  (let [instance-uuid (pod-name->instance-uuid pod-name)
        job-uuid (or (pod-name->job-uuid pod-name)
                     (cached-queries/instance-uuid->job-uuid-cache-lookup instance-uuid))
        {:keys [job/name job/user] :as job} (cached-queries/job-uuid->job-map-cache-lookup job-uuid)]
    (cond->
      {:job-name name
       :job-uuid job-uuid
       :pod-name pod-name
       :pool (cached-queries/job->pool-name job)
       :user user}
      instance-uuid (assoc :instance-uuid instance-uuid))))

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

(def ^ExecutorService kubernetes-executor (Executors/newCachedThreadPool
                                            (-> (ThreadFactoryBuilder.)
                                                (.setNameFormat "Cook's kubernetes-executor %d")
                                                .build)))

; Cook, Fenzo, and Mesos use mebibytes (MiB) for memory.
; Convert bytes from k8s to MiB when passing to Fenzo,
; and MiB back to bytes when submitting to k8s.
(def memory-multiplier (* 1024 1024))

(defn pod-label-prefix
  "Returns the prefix to use for application-related pod labels"
  []
  (:add-job-label-to-pod-prefix (config/kubernetes)))

(defn workload-class-label
  "Returns the full pod label for workload-class"
  []
  (str (pod-label-prefix) "application.workload-class"))

(defn workload-id-label
  "Returns the full pod label for workload-id"
  []
  (str (pod-label-prefix) "application.workload-id"))

(defn pod-labels-defaults
  "Returns a map with default pod labels"
  []
  (let [prefix (pod-label-prefix)]
    {(str prefix "application.name") "undefined"
     (str prefix "application.version") "undefined"
     (str prefix "application.workload-class") "undefined"
     (str prefix "application.workload-id") "undefined"
     (str prefix "application.workload-details") "undefined"}))

(defn is-cook-scheduler-pod
  "Is this a cook pod? Uses some-> so is null-safe."
  [^V1Pod pod compute-cluster-name]
  (= compute-cluster-name (some-> pod .getMetadata .getLabels (.get cook-pod-label))))

(defn pod->node-name
  "Given a pod, returns the node name on the pod spec"
  [^V1Pod pod]
  (or (some-> pod .getSpec .getNodeName)
      (some-> pod .getSpec .getNodeSelector (.get k8s-hostname-label))))

(defn cook-pod-callback-wrap
  "A special wrapping function that, given a callback, key, prev-item, and item, will invoke the callback only
  if the item is a pod that is a cook scheduler pod. (THe idea is that (partial cook-pod-callback-wrap othercallback)
  returns a new callback that only invokes othercallback on cook pods."
  [callback compute-cluster-name key prev-item item]
  (when (or (is-cook-scheduler-pod prev-item compute-cluster-name) (is-cook-scheduler-pod item compute-cluster-name))
    (callback key prev-item item)))

(defn process-watch-response!
  "Given a watch response object (for pods or nodes), updates the state atom and
  invokes the provided callbacks on the previous and new values for the key."
  [state-atom watch-object watch-type key-fn callbacks]
  (let [key (key-fn watch-object)
        prev-item (get @state-atom key)
        ; Now we want to re-bind prev-item and item to the real previous and current,
        ; embedding ADDED/MODIFIED/DELETED based on the location of nils.
        [prev-item item]
        (case watch-type
          "ADDED" [nil watch-object]
          "MODIFIED" [prev-item watch-object]
          "DELETED" [prev-item nil])]
    (doseq [callback callbacks]
      (try
        (callback key prev-item item)
        (catch Exception e
          (log/error e "Error while processing callback"))))))

(let [last-watch-response-or-connect-millis-atom (atom {})]
  (defn update-watch-gap-metric!
    "Given a compute cluster name and a watch object type (either :pod or :node), updates a
    metric with the gap in milliseconds between the last watch response or connection and the
    current watch response or connection. The goal here is to measure 1) the gap between events
    when we process watches, and 2) the gap when we're not processing anything because we're
    reconnecting the watch. This function is called at the start of watch processing (with one
    metric name) and between watch events (with a different metric name). In both cases, we
    update the last-watch-response-or-connect timestamp."
    [compute-cluster-name watch-object-type metric-name prom-metric-name log?]
    (when-let [last-watch-response-or-connect-millis
               (get-in
                 @last-watch-response-or-connect-millis-atom
                 [compute-cluster-name watch-object-type])]
      (let [millis (- (System/currentTimeMillis) last-watch-response-or-connect-millis)
            metric-name (str (name watch-object-type) "-" metric-name)]
        (histograms/update!
          (metrics/histogram metric-name compute-cluster-name) millis)
        (prom/observe prom-metric-name {:compute-cluster compute-cluster-name :object (name watch-object-type)} millis)
        (when log?
          (log-structured/info
            "Marked watch gap"
            {:compute-cluster compute-cluster-name
             :metric-name metric-name
             :watch-gap-millis millis
             :watch-last-event (-> last-watch-response-or-connect-millis tc/from-long str)}))))
    (swap!
      last-watch-response-or-connect-millis-atom
      assoc-in
      [compute-cluster-name watch-object-type]
      (System/currentTimeMillis))))

(defn mark-watch-gap!
  "Updates the watch-gap-millis metric."
  [compute-cluster-name watch-object-type]
  (update-watch-gap-metric! compute-cluster-name watch-object-type "watch-gap-millis" prom/watch-gap false))

(defn mark-disconnected-watch-gap!
  "Updates the disconnected-watch-gap-millis metric."
  [compute-cluster-name watch-object-type]
  (update-watch-gap-metric! compute-cluster-name watch-object-type "disconnected-watch-gap-millis" prom/disconnected-watch-gap true))

(defn handle-watch-updates
  "When a watch update occurs (for pods or nodes) update both the state atom as well as
  invoke the callbacks on the previous and new values for the key."
  [state-atom ^Watch watch key-fn callbacks set-metric-counters-fn compute-cluster-name watch-object-type]
  (mark-disconnected-watch-gap! compute-cluster-name watch-object-type)
  (while (.hasNext watch)
    (let [watch-response (.next watch)
          item (.-object watch-response)
          type (.-type watch-response)]
      (if (some? item)
        (do
          (mark-watch-gap! compute-cluster-name watch-object-type)
          (process-watch-response! state-atom item type key-fn callbacks)
          (set-metric-counters-fn))
        (throw (ex-info
                 "Encountered nil object on watch response"
                 {:compute-cluster-name compute-cluster-name
                  :watch-object item
                  :watch-object-type watch-object-type
                  :watch-status (.-status watch-response)
                  :watch-type type})))))
  (log-structured/info (print-str "There are no more" (name watch-object-type) "watch updates")
                       {:compute-cluster compute-cluster-name}))

(defn get-pod-namespaced-key
  [^V1Pod pod]
  (if-let [^V1ObjectMeta pod-metadata (some-> pod .getMetadata)]
    {:namespace (-> pod-metadata .getNamespace)
     :name (-> pod-metadata .getName)}
    (throw (ex-info
             (str "Unable to get namespaced key for pod: " pod)
             {:pod pod}))))

(defn list-pods-for-all-namespaces
  "Thin wrapper around .listPodForAllNamespaces"
  [api continue limit]
  (let [response
        (.listPodForAllNamespaces api
                                  nil         ; allowWatchBookmarks
                                  continue    ; continue
                                  nil         ; fieldSelector
                                  nil         ; labelSelector
                                  (int limit) ; limit
                                  nil         ; pretty
                                  nil         ; resourceVersion
                                  nil         ; resourceVersionMatch
                                  nil         ; timeoutSeconds
                                  nil         ; watch
                                  )]
    {:continue (-> response .getMetadata .getContinue)
     :pods (.getItems response)
     :resource-version (-> response .getMetadata .getResourceVersion)}))

(defn list-pods
  "Calls .listPodForAllNamespaces with chunks of size limit"
  [api-client compute-cluster-name limit]
  (let [api (CoreV1Api. api-client)]
    (loop [all-pods []
           continue-string nil
           resource-version-string nil]
      (log-structured/info "Listing pods for all namespaces"
                {:compute-cluster compute-cluster-name
                 :continue continue-string
                 :limit limit
                 :number-pods-so-far (count all-pods)
                 :resource-version resource-version-string})
      (let [{:keys [continue pods resource-version]}
            (prom/with-duration
              prom/list-pods-chunk-duration {:compute-cluster compute-cluster-name}
              (timers/time!
                (metrics/timer "get-all-pods-single-chunk" compute-cluster-name)
                (list-pods-for-all-namespaces api continue-string limit)))
            new-all-pods (concat all-pods pods)]

        ; Note that the resourceVersion of the list remains constant across each request,
        ; indicating the server is showing us a consistent snapshot of the pods.
        ; (https://kubernetes.io/docs/reference/using-api/api-concepts/#retrieving-large-results-sets-in-chunks)
        (when (or (and (some? resource-version-string)
                       (not= resource-version resource-version-string))
                  (str/blank? resource-version))
          (throw
            (ex-info (str "In " compute-cluster-name
                          " compute cluster, unexpected resource version on chunk")
                     {:continue continue-string
                      :limit limit
                      :number-pods-so-far (count new-all-pods)
                      :number-pods-this-chunk (count pods)
                      :resource-version-new resource-version
                      :resource-version-previous resource-version-string})))

        (if (str/blank? continue)
          (do
            (log-structured/info "Done listing pods for all namespaces"
                                 {:compute-cluster compute-cluster-name
                                  :limit limit
                                  :number-pods (count new-all-pods)
                                  :number-pods-this-chunk (count pods)
                                  :resource-version resource-version})
            {:pods new-all-pods
             :resource-version resource-version})
          (recur new-all-pods
                 continue
                 resource-version))))))

(defn get-all-pods-in-kubernetes
  "Get all pods in kubernetes."
  [api-client compute-cluster-name limit]
  (prom/with-duration
    prom/list-pods-duration {:compute-cluster compute-cluster-name}
    (timers/time! (metrics/timer "get-all-pods" compute-cluster-name)
                  (let [{:keys [pods resource-version]} (list-pods api-client compute-cluster-name limit)
                        namespaced-pod-name->pod (pc/map-from-vals get-pod-namespaced-key pods)]
                    [resource-version namespaced-pod-name->pod]))))

(defn try-forever-get-all-pods-in-kubernetes
  "Try forever to get all pods in kubernetes. Used when starting a cluster up."
  [api-client compute-cluster-name]
  (loop []
    (let [{:keys [list-pods-limit reconnect-delay-ms]} (config/kubernetes)
          out (try
                (get-all-pods-in-kubernetes api-client compute-cluster-name list-pods-limit)
                (catch Throwable e
                  (log-structured/error "Error during cluster startup getting all pods for cluster; sleeping before reconnect"
                                        {:compute-cluster compute-cluster-name
                                         :reconnect-delay-ms reconnect-delay-ms}
                                        e)
                  (Thread/sleep reconnect-delay-ms)
                  nil))]
      (if out
        out
        (recur)))))

(defn create-pod-watch
  "Wrapper for WatchHelper/createPodWatch"
  [^ApiClient api-client resource-version]
  (WatchHelper/createPodWatch api-client resource-version))

(defn- set-metric-counter
  [counter-name counter-value compute-cluster-name]
  (monitor/set-counter!
    (counters/counter ["cook-k8s" counter-name (str "compute-cluster-" compute-cluster-name)])
    counter-value))

(declare initialize-pod-watch)
(defn ^Callable initialize-pod-watch-helper
  "Help creating pod watch. Returns a new watch Callable"
  [{:keys [^ApiClient api-client all-pods-atom ^ExecutorService controller-executor-service node-name->pod-name->pod]
    compute-cluster-name :name {:keys [max-total-pods]} :synthetic-pods-config :as compute-cluster} cook-pod-callback]
  (let [{:keys [list-pods-limit]} (config/kubernetes)
        [^String resource-version namespaced-pod-name->pod]
        (get-all-pods-in-kubernetes api-client compute-cluster-name list-pods-limit)
        callbacks
        [(tools/make-atom-updater all-pods-atom) ; Update the set of all pods.
         (tools/make-nested-atom-updater node-name->pod-name->pod pod->node-name get-pod-namespaced-key)
         (partial cook-pod-callback-wrap cook-pod-callback compute-cluster-name)] ; Invoke the cook-pod-callback if its a cook pod.
        old-all-pods @all-pods-atom
        new-pod-names (set (keys namespaced-pod-name->pod))
        old-pod-names (set (keys old-all-pods))]
    (log-structured/info "Pod watch processing pods"
                         {:compute-cluster compute-cluster-name
                          :number-pods (count namespaced-pod-name->pod)
                          :resource-version (print-str resource-version)})
    ; We want to process all changes through the callback process.
    ; So compute the delta between the old and new and process those via the callbacks.
    ; Note as a side effect, the callbacks mutate all-pods-atom
    (let [tasks (set/union new-pod-names old-pod-names)]
      (doseq [task tasks]
        (log-structured/info "Pod watch doing callback for task"
                             {:compute-cluster compute-cluster-name
                              :pod-name (:name task)
                              :pod-namespace (:namespace task)
                              :new? (contains? new-pod-names task)
                              :old? (contains? old-pod-names task)})
        (doseq [callback callbacks]
          (try
            (callback task (get old-all-pods task) (get namespaced-pod-name->pod task))
            (catch Exception e
              (log-structured/error (print-str "Pod watch error while processing callback for" task)
                                    {:compute-cluster compute-cluster-name}
                                    e))))))
    (log-structured/info "Pod watch done doing callbacks for tasks, starting handling pod watch updates"
                         {:compute-cluster compute-cluster-name})
    (let [watch (create-pod-watch api-client resource-version)]
      (fn []
        (try
          (log-structured/info "Handling pod watch updates" {:compute-cluster compute-cluster-name})
          (handle-watch-updates
            all-pods-atom
            watch
            get-pod-namespaced-key
            callbacks
            (fn []
              (let [total-pods (count @all-pods-atom)]
                (prom/set prom/total-pods {:compute-cluster compute-cluster-name} total-pods)
                (set-metric-counter "total-pods" total-pods compute-cluster-name))
              (when max-total-pods
                  (prom/set prom/max-pods {:compute-cluster compute-cluster-name} max-total-pods)
                  (set-metric-counter "max-total-pods" max-total-pods compute-cluster-name)))
            compute-cluster-name
            :pod)
          (catch Exception e
            (let [cause (.getCause e)]
              (if (and cause (instance? SocketTimeoutException cause))
                (log-structured/info "Pod watch timed out" {:compute-cluster compute-cluster-name} e)
                (log-structured/error "Error during pod watch" {:compute-cluster compute-cluster-name} e))))
          (finally
            (.close watch)
            (initialize-pod-watch compute-cluster cook-pod-callback)))))))

(defn initialize-pod-watch
  "Initialize the pod watch. This fills all-pods-atom with data and invokes the callback on pod changes."
  [{:keys [state-atom] compute-cluster-name :name :as compute-cluster} cook-pod-callback]
  (log-structured/info "Initializing pod watch" {:compute-cluster compute-cluster-name})
  ; We'll iterate trying to connect to k8s until the initialize-pod-watch-helper returns a watch function.
  (let [{:keys [reconnect-delay-ms]} (config/kubernetes)
        tmpfn (fn []
                (try
                  (log-structured/info "Initializing pod watch helper" {:compute-cluster compute-cluster-name})
                  (initialize-pod-watch-helper compute-cluster cook-pod-callback)
                  (catch Exception e
                    (let [state @state-atom]
                      (if (= state :deleted)
                        (log-structured/info
                          "Cannot create pod watch (cluster is deleted); sleeping before next attempt"
                          {:compute-cluster compute-cluster-name :reconnect-delay-ms reconnect-delay-ms :state state}
                          e)
                        (log-structured/error
                          "Error during pod watch initial setup; sleeping before next attempt"
                          {:compute-cluster compute-cluster-name :reconnect-delay-ms reconnect-delay-ms :state state}
                          e)))
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
  [cook-pool-label-name cook-pool-label-prefix ^V1Node node]
  ; In the case of nil, we have taints-on-node == [], and we'll map to no-pool.
  (let [^String label-value (or (some-> node .getMetadata .getLabels (get cook-pool-label-name "")) "")]
        (if (str/starts-with? label-value cook-pool-label-prefix)
          (subs label-value (count cook-pool-label-prefix))
          "no-pool")))

(declare initialize-node-watch)
(defn initialize-node-watch-helper
  "Help creating node watch. Returns a new watch Callable"
  [{:keys [^ApiClient api-client current-nodes-atom pool->node-name->node cook-pool-label-name cook-pool-label-prefix]
    compute-cluster-name :name {:keys [max-total-nodes]} :synthetic-pods-config :as compute-cluster}]
  (let [api (CoreV1Api. api-client)
        ^V1NodeList current-nodes-raw
        (prom/with-duration
          prom/list-nodes-duration {:compute-cluster compute-cluster-name}
          (timers/time! (metrics/timer "get-all-nodes" compute-cluster-name)
                        (.listNode api
                                   nil ; pretty
                                   nil ; allowWatchBookmarks
                                   nil ; continue
                                   nil ; fieldSelector
                                   nil ; labelSelector
                                   nil ; limit
                                   nil ; resourceVersion
                                   nil ; resourceVersionMatch
                                   nil ; timeoutSeconds
                                   nil ; watch
                                   )))
        current-nodes (pc/map-from-vals node->node-name (.getItems current-nodes-raw))
        callbacks
        [(tools/make-atom-updater current-nodes-atom) ; Update the set of all pods.
         (tools/make-nested-atom-updater pool->node-name->node (partial get-node-pool cook-pool-label-name cook-pool-label-prefix) node->node-name)]
        old-current-nodes @current-nodes-atom
        new-node-names (set (keys current-nodes))
        old-node-names (set (keys old-current-nodes))]
    (log-structured/info "Node watch processing nodes"
                         {:compute-cluster compute-cluster-name :number-nodes (count current-nodes)})
    ; We want to process all changes through the callback process.
    ; So compute the delta between the old and new and process those via the callbacks.
    ; Note as a side effect, the callbacks mutate current-nodes-atom
    (doseq [node-name (set/union new-node-names old-node-names)]
      (log-structured/info "Node watch doing callback"
                           {:compute-cluster compute-cluster-name
                            :new? (contains? new-node-names node-name)
                            :old? (contains? old-node-names node-name)
                            :node node-name})
      (doseq [callback callbacks]
        (try
          (callback node-name (get old-current-nodes node-name) (get current-nodes node-name))
          (catch Exception e
            (log-structured/error "Node watch error while processing callback for node"
                                  {:compute-cluster compute-cluster-name
                                   :node node-name}
                                  e)))))

    (let [watch (WatchHelper/createNodeWatch api-client (-> current-nodes-raw .getMetadata .getResourceVersion))]
      (fn []
        (try
          (log-structured/info "Handling node watch updates"
                               {:compute-cluster compute-cluster-name})
          (handle-watch-updates
            current-nodes-atom
            watch
            node->node-name
            callbacks ; Update the set of all nodes.
            (fn []
              (let [current-nodes (count @current-nodes-atom)]
                (prom/set prom/total-nodes {:compute-cluster compute-cluster-name} current-nodes)
                (set-metric-counter "total-nodes"  current-nodes compute-cluster-name))
              (when max-total-nodes
                  (prom/set prom/max-nodes {:compute-cluster compute-cluster-name} max-total-nodes)
                  (set-metric-counter "max-total-nodes" max-total-nodes compute-cluster-name)))
            compute-cluster-name
            :node)
          (catch Exception e
            (let [cause (-> e Throwable->map :cause)]
              (if (= cause "Socket closed")
                ; We expect timeouts to happen on the node watch when there is
                ; no node-related activity, so we log with INFO instead of WARN
                (log-structured/info "Node watch timed out"
                                     {:compute-cluster compute-cluster-name}
                                     e)
                (log-structured/warn "Error during node watch"
                                     {:compute-cluster compute-cluster-name :cause cause}
                                     e))))
          (finally
            (.close watch)
            (initialize-node-watch compute-cluster)))))))

(defn initialize-node-watch
  "Initialize the node watch. This fills current-nodes-atom with data and invokes the callback on pod changes."
  [{:keys [state-atom] compute-cluster-name :name :as compute-cluster}]
  (log-structured/info "Initializing node watch" {:compute-cluster compute-cluster-name})
  ; We'll iterate trying to connect to k8s until the initialize-node-watch-helper returns a watch function.
  (let [{:keys [reconnect-delay-ms]} (config/kubernetes)
        tmpfn (fn []
                (try
                  (log-structured/info "Initializing node watch helper" {:compute-cluster compute-cluster-name})
                  (initialize-node-watch-helper compute-cluster)
                  (catch Exception e
                    (let [state @state-atom]
                      (if (= state :deleted)
                        (log-structured/info
                          "Cannot create node watch (cluster is deleted); sleeping before next attempt"
                          {:compute-cluster compute-cluster-name :reconnect-delay-ms reconnect-delay-ms :state state}
                          e)
                        (log-structured/error
                          "Error during node watch initial setup; sleeping before next attempt"
                          {:compute-cluster compute-cluster-name :reconnect-delay-ms reconnect-delay-ms :state state}
                          e)))
                    (Thread/sleep reconnect-delay-ms)
                    nil)))
        ^Callable first-success (->> tmpfn repeatedly (some identity))]
    (.submit kubernetes-executor ^Callable first-success)))

(defn prepare-object-metadata-for-logging
  "Generates a pruned version of object metadata that is suitable for logging.

  Logging the full metadata object is too much, in particular the
  managedFields. This function generates a pruned version of metadata
  that's not as verbose. This cuts logging output by about 2x compared to a
  simple toString on V1ObjectMeta. As of version 11.0.0 of the Kubernetes
  client library, the following fields are present in V1ObjectMeta,
  separated by whether or not we're choosing to log them.

  We do log the following fields because we either use them to determine
  state in the state machine (e.g. labels) or because we know they are
  useful for debugging purposes (e.g. resourceVersion):

  - annotations
  - creationTimestamp
  - deletionTimestamp
  - finalizers
  - labels
  - name
  - namespace
  - resourceVersion

  We do not log the following fields:

  - clusterName
  - deletionGracePeriodSeconds
  - generateName
  - generation
  - managedFields
  - ownerReferences
  - selfLink
  - uid"
  [^V1ObjectMeta metadata]
  (let [annotations (.getAnnotations metadata)
        creation-timestamp (.getCreationTimestamp metadata)
        deletion-timestamp (.getDeletionTimestamp metadata)
        finalizers (.getFinalizers metadata)
        labels (.getLabels metadata)]
    (cond->
      {:name (.getName metadata)
       :namespace (.getNamespace metadata)
       :resource-version (.getResourceVersion metadata)}
      (seq annotations) (assoc :annotations (.toString annotations))
      creation-timestamp (assoc :creation-timestamp (.toString creation-timestamp))
      deletion-timestamp (assoc :deletion-timestamp (.toString deletion-timestamp))
      (seq finalizers) (assoc :finalizers (.toString finalizers))
      (seq labels) (assoc :labels (.toString labels)))))

(declare initialize-event-watch)
(defn ^Callable initialize-event-watch-helper
  "Returns a new event watch Callable"
  [{:keys [^ApiClient api-client all-pods-atom] compute-cluster-name :name :as compute-cluster}]
  (let [watch (WatchHelper/createEventWatch api-client nil)]
    (fn []
      (try
        (log-structured/info "Handling event watch updates" {:compute-cluster compute-cluster-name})
        (while (.hasNext watch)
          (let [watch-response (.next watch)
                ^CoreV1Event event (.-object watch-response)]
            (when event
              (let [^V1ObjectReference involved-object (.getInvolvedObject event)]
                (when (and involved-object (= (.getKind involved-object) "Pod"))
                  (let [namespaced-pod-name {:namespace (.getNamespace involved-object)
                                             :name (.getName involved-object)}
                        host (some-> event .getSource .getHost)
                        field-path (.getFieldPath involved-object)
                        event-type (.getType event)
                        tracked-pod?
                        (some-> @all-pods-atom
                          (get namespaced-pod-name)
                          (is-cook-scheduler-pod compute-cluster-name))]
                    (when (or
                            tracked-pod?
                            (and
                              ; The event type can be either "Normal" or "Warning", and we're
                              ; only interested in "Warning" events for non-tracked pods
                              (= event-type "Warning")
                              ; The fieldPath is something like:
                              ; "spec.containers{aux-cook-sidecar-container}"
                              (some-> field-path str/lower-case (str/includes? "cook"))))
                      (log-structured/info
                                "Received pod event"
                                {:compute-cluster compute-cluster-name
                                 :event-reason (.getReason event)
                                 :event {:first-timestamp (some-> event .getFirstTimestamp .toString)
                                         :involved-object
                                         (cond->
                                           {:name (.getName involved-object)
                                            :namespace (.getNamespace involved-object)
                                            :resource-version (.getResourceVersion involved-object)}
                                           field-path (assoc :field-path field-path))
                                         :kind (.getKind event)
                                         :last-timestamp (some-> event .getLastTimestamp .toString)
                                         :message (.getMessage event)
                                         :metadata (some-> event .getMetadata prepare-object-metadata-for-logging)
                                         :source
                                         (cond->
                                           {:component (some-> event .getSource .getComponent)}
                                           host (assoc :host host))
                                         :type event-type}
                                 :tracked-pod? tracked-pod?
                                 :watch-response-type (.-type watch-response)}))))))))
        (catch Exception e
          (let [cause (.getCause e)]
            (if (and cause (instance? SocketTimeoutException cause))
              (log-structured/info "Event watch timed out" {:compute-cluster compute-cluster-name} e)
              (log-structured/error "Error during event watch" {:compute-cluster compute-cluster-name} e))))
        (finally
          (.close watch)
          (initialize-event-watch compute-cluster))))))

(defn initialize-event-watch
  "Initializes the event watch"
  [{:keys [state-atom] compute-cluster-name :name :as compute-cluster}]
  (log-structured/info "Initializing event watch" {:compute-cluster compute-cluster-name})
  (let [{:keys [reconnect-delay-ms]} (config/kubernetes)
        tmpfn (fn []
                (try
                  (log-structured/info "Initializing event watch helper"
                            {:compute-cluster compute-cluster-name})
                  (initialize-event-watch-helper compute-cluster)
                  (catch Exception e
                    (let [state @state-atom]
                      (if (= state :deleted)
                        (log-structured/info
                          "Cannot create event watch (cluster is deleted); sleeping before next attempt"
                          {:compute-cluster compute-cluster-name :reconnect-delay-ms reconnect-delay-ms :state state}
                          e)
                        (log-structured/error
                          "Error during event watch initial setup; sleeping before next attempt"
                          {:compute-cluster compute-cluster-name :reconnect-delay-ms reconnect-delay-ms :state state}
                          e)))
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
  "Converts a map of Kubernetes resources to a cook resource map {:mem double, :cpus double, :gpus double, :disk double}"
  [m]
  (let [res-map
        {:mem (if (get m "memory")
                (-> m (get "memory") to-double (/ memory-multiplier))
                0.0)
         :cpus (if (get m "cpu")
                 (-> m (get "cpu") to-double)
                 0.0)
         ; Assumes that each Kubernetes node and each pod only contains one type of GPU model
         :gpus (if-let [gpu-count (get m "nvidia.com/gpu")]
                 (to-int gpu-count)
                 0)}]
    ; Only add disk to the resource map if pods are using disk
    (if (get m "ephemeral-storage")
      (assoc res-map :disk (-> m (get "ephemeral-storage") to-double (/ constraints/disk-multiplier)))
      res-map)))

(defn pods->node-name->pods
  "Given a seq of pods, create a map of node names to pods"
  [pods]
  (group-by pod->node-name pods))

(defn num-pods-on-node
  "Returns the number of pods assigned to the given node"
  [node-name pods]
  (->> pods
       (filter #(= (pod->node-name %) node-name))
       count))

(defn node->resource-map
  "Given a node, returns the resource map used internally by Cook"
  [^V1Node node]
  (some-> node .getStatus .getAllocatable convert-resource-map))

(defn node-schedulable?
  "Can we schedule on a node. For now, yes, unless there are other taints on it or it contains any label in the
  node-blocklist-labels list.
  TODO: Incorporate other node-health measures here."
  [{:keys [name node-blocklist-labels cook-pool-taint-name cook-pool-taint2-name]} ^V1Node node pod-count-capacity node-name->pods]
  (b/cond
    (nil? node)
    false

    :let [node-name (some-> node .getMetadata .getName)]

    ;; Note that node-unschedulable may be nil or false or true.
    :let [node-unschedulable (some-> node .getSpec .getUnschedulable)]
    node-unschedulable (do
                         (log-structured/info "Filtering out node because it is unschedulable"
                                              {:node node-name
                                               :reason "unschedulable"})
                         false)

    :let [taints-on-node (or (some-> node .getSpec .getTaints) [])
          other-taints
          (remove #(contains?
                     (set [cook-pool-taint-name k8s-deletion-candidate-taint gpu-node-taint tenured-node-taint cook-pool-taint2-name])
                     (.getKey ^V1Taint %))
                  taints-on-node)]
    (seq other-taints) (do
                         (log-structured/info "Filtering out node because it has taints"
                                              {:node node-name
                                               :reason "taints"
                                               :filtered-taints (print-str other-taints)})
                         false)

    :let [num-pods-on-node (-> node-name->pods (get node-name []) count)]
    (>= num-pods-on-node pod-count-capacity) (do
                                               (log-structured/info "Filtering out node because it is at or above its pod count capacity"
                                                                    {:node node-name
                                                                     :reason "pod_limit"
                                                                     :pod-count-capacity pod-count-capacity
                                                                     :pod-count-current num-pods-on-node})
                                               false)

    :let [labels-on-node (or (some-> node .getMetadata .getLabels) {})
          matching-node-blocklist-keyvals (select-keys labels-on-node node-blocklist-labels)]
    (seq matching-node-blocklist-keyvals) (do
                                            (log-structured/info "Filtering out node because it has node blocklist labels"
                                                                 {:node node-name
                                                                  :reason "blocklist_labels"
                                                                  :blocklist-labels (print-str matching-node-blocklist-keyvals)})
                                            false)

    ; If a node is tainted as having GPUs but has no allocatable GPUs, log and filter it
    ; out so that we don't risk matching any jobs to it (something is wrong with the node)
    :let [has-gpu-node-taint? (->> taints-on-node (map #(.getKey ^V1Taint %)) (some #{gpu-node-taint}))
          has-allocatable-gpus? (some-> node node->resource-map :gpus pos?)]
    (and has-gpu-node-taint? (not has-allocatable-gpus?))
    (let [filter-out? (:filter-out-unsound-gpu-nodes? (config/kubernetes))]
      (log-structured/error
        "Encountered node with GPU taint but no allocatable GPUs"
        {:allocatable (some->> node .getStatus .getAllocatable (pc/map-vals #(.toSuffixedString %)))
         :filter-out? filter-out?
         :gpu-node-taint (print-str gpu-node-taint)
         :compute-cluster name
         :node node-name})
      (not filter-out?))

    :else true))

(defn force-gpu-model-in-resource-map
  "Given a map from node-name->resource-type->capacity, set the gpus capacity to model->amount
  or remove the gpus resource type from the map if gpu-model is not provided"
  [gpu-model {:keys [gpus] :as resource-map}]
  (if (and gpu-model (pos? gpus))
    (assoc resource-map :gpus {gpu-model gpus})
    (dissoc resource-map :gpus)))

(defn pool->disk-type-label-name
  "Given a pool name, get the disk-type-label-name that will be used as a nodeSelector label"
  [pool-name]
  (regexp-tools/match-based-on-pool-name
    (config/disk)
    pool-name
    :disk-node-label
    :default-value "cloud.google.com/gke-boot-disk"))

(defn force-disk-type-in-resource-map
  "Given a map from node-name->resource-type->capacity, set the disk capacity to type->amount
  or remove the disk resource type from the map if disk-type is not provided"
  [disk-type {:keys [disk] :as resource-map}]
  (if (and disk disk-type)
    (assoc resource-map :disk {disk-type disk})
    (dissoc resource-map :disk)))

(defn get-capacity
  "Given a map from node-name to node, generate a map from node-name->resource-type-><capacity>"
  [node-name->node pool-name]
  (pc/map-vals (fn [^V1Node node]
                 (let [resource-map (node->resource-map node)
                       gpu-model (some-> node .getMetadata .getLabels (get "gpu-type"))
                       disk-type (some-> node .getMetadata .getLabels (get (pool->disk-type-label-name pool-name)))]
                   (->> resource-map
                        (force-gpu-model-in-resource-map gpu-model)
                        (force-disk-type-in-resource-map disk-type))))
               node-name->node))

(defn get-consumption
  "Given a map from pod-name to pod, generate a map from node-name->resource-type->consumption.
  Ignores pods that do not have an assigned node.
  When accounting for resources, we use resource requests to determine how much is used, not limits.
  See https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#resource-requests-and-limits-of-pod-and-container"
  [clobber-synthetic-pods node-name->pods pool-name]
  (->> node-name->pods
       ; Keep those with non-nil node names
       (filter first)
       ; Keep those with non-nil and non-empty pods (in the wild, we occasionally see nodes at the
       ; beginning of their lifetime come through this code path with no pods associated to them, and
       ; when this happens, an exception is thrown in the calling code and no offers are generated)
       (filter #(-> % second seq))
       (pc/map-vals (fn [pods]
                      (->> pods
                           (remove #(and clobber-synthetic-pods (some-> ^V1Pod % .getMetadata .getName synthetic-pod?)))
                           (map (fn [^V1Pod pod]
                                  (let [containers (some-> pod .getSpec .getContainers)
                                        container-requests (map (fn [^V1Container c]
                                                                  (some-> c
                                                                          .getResources
                                                                          .getRequests
                                                                          convert-resource-map))
                                                                containers)
                                        resource-map (apply merge-with + container-requests)
                                        ^V1NodeSelector nodeSelector (some-> pod .getSpec .getNodeSelector)
                                        gpu-model (some-> nodeSelector (get "cloud.google.com/gke-accelerator"))
                                        disk-type (some-> nodeSelector (get (pool->disk-type-label-name pool-name)))]
                                    (->> resource-map
                                         (force-gpu-model-in-resource-map gpu-model)
                                         (force-disk-type-in-resource-map disk-type)))))
                           ; remove nil resource-maps from collection before deep-merge-with because some pods do not
                           ; have container resource requests, and deep-merge-with does not gracefully handle nil values
                           (remove nil?)
                           (apply util/deep-merge-with +))))
       ; Keep entries with non-nil resource-type -> consumption maps
       ; (the resource-type -> consumption map can be nil, for
       ; example, if all pods on the node have no resource request)
       (filter #(-> % second some?))
       (into {})))

; see pod->synthesized-pod-state comment for container naming conventions
(def cook-container-name-for-job
  "required-cook-job-container")
; see pod->synthesized-pod-state comment for container naming conventions
(def cook-container-name-for-file-server
  "aux-cook-sidecar-container")
; see pod->synthesized-pod-state comment for container naming conventions
(def checkpoint-cook-init-container-name
  "aux-cook-init-container-for-checkpoint")
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
  [^String name]
  (let [^V1VolumeBuilder builder (-> ^V1VolumeBuilder (V1VolumeBuilder.)
                                     (.withName name)
                                     (.withEmptyDir (V1EmptyDirVolumeSource.)))]
    (.build builder)))

(defn make-shm-volume
  "Make a volume for /dev/shm and add it to the volume mount for the given image"
  []
  (let [^V1EmptyDirVolumeSource volume-source (V1EmptyDirVolumeSource.)
        _ (.medium volume-source "Memory")
        ; Randomize the name to reduce the changes of name collision with any other volume name.
        ^String name (str "shmvolume-" (+ (rand-int 899999) 100000))
        ^V1VolumeBuilder builder (-> ^V1VolumeBuilder (V1VolumeBuilder.)
                                     (.withName name)
                                     (.withEmptyDir volume-source))]
    (.build builder)))

(defn- make-shm-volume-mount
  "Make a volume mount for a shm volume mount."
  ([^V1Volume volume]
   (doto (V1VolumeMount.)
     (.setName (.getName volume))
     (.setMountPath "/dev/shm"))))

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

(defn maybe-conj
  "Conj the item onto the accum only if not nil"
  [accum item]
  (cond-> accum
    (some? item) (conj item)))


(defn make-volumes
  "Converts a list of cook volumes to kubernetes volumes and volume mounts."
  [cook-volumes sandbox-dir job-labels]
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
        mesos-sandbox-volume-mount (make-volume-mount sandbox-volume "/mnt/mesos/sandbox" false)
        label-prefix (:add-job-label-to-pod-prefix (config/kubernetes))
        make-shm-volume? (= "true" (get job-labels (str label-prefix "shared-memory")))
        shm-volume (when make-shm-volume? (make-shm-volume))
        shm-volume-mount (when make-shm-volume? (make-shm-volume-mount shm-volume))]
    {:sandbox-volume-mount-fn sandbox-volume-mount-fn
     :volumes (maybe-conj (conj volumes sandbox-volume) shm-volume)
     :volume-mounts (maybe-conj (conj volume-mounts sandbox-volume-mount mesos-sandbox-volume-mount) shm-volume-mount)}))

(defn toleration-for-pool
  "For a given cook pool name, create a pool-specific toleration so that Cook will ignore that cook-pool taint."
  [cook-pool-taint-name cook-pool-taint-prefix pool-name]
  (let [^V1Toleration toleration (V1Toleration.)]
    (.setKey toleration cook-pool-taint-name)
    (.setValue toleration (str cook-pool-taint-prefix pool-name))
    (.setOperator toleration "Equal")
    (.setEffect toleration "NoSchedule")
    toleration))

(defn toleration-for-pool2
  "Create a static toleration that cook will always use."
  [cook-pool-taint2-name cook-pool-taint2-value]
  (let [^V1Toleration toleration (V1Toleration.)]
    (.setKey toleration cook-pool-taint2-name)
    (.setValue toleration cook-pool-taint2-value) ; Bug: Not using tint name.
    (.setOperator toleration "Equal")
    (.setEffect toleration "NoSchedule")
    toleration))

(def toleration-tenured-node
  (doto (V1Toleration.)
    (.setKey tenured-node-taint)
    (.setOperator "Exists")
    (.setEffect "NoSchedule")))

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
                    [(tools/user->user-id user) (tools/user->group-id user)])
        security-context (V1PodSecurityContext.)]
    (.setRunAsUser security-context uid)
    (.setRunAsGroup security-context gid)
    (when-let [group-ids (tools/user->all-group-ids user)]
      (.setSupplementalGroups security-context group-ids))
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

(def hostIpEnvVarSource
  (doto
    (V1EnvVarSource.)
    (.fieldRef
      (doto
        (V1ObjectFieldSelector.)
        (.fieldPath "status.hostIP")))))

(def hostIpEnvVar
  (doto
    (V1EnvVar.)
    (.setName "HOST_IP")
    (.valueFrom hostIpEnvVarSource)))

(defn make-filtered-env-vars
  "Create a Kubernetes API compatible var list from an environment vars map,
   with variables filtered based on disallowed-var-names in the Kubernetes config."
  [env]
  (let [{:keys [disallowed-var-names]} (config/kubernetes)
        disallowed-var? #(contains? disallowed-var-names (key %))]
    (->> env
         (remove disallowed-var?)
         (mapv #(apply make-env %))
         (cons hostIpEnvVar))))

(defn get-default-env-for-pool
  "Given a pool name, determine the default environment for containers in that pool"
  [pool-name]
  (let [effective-pool-name (or pool-name config/default-pool "")
        default-envs (get-in config/config [:settings :pools :default-env])]
    (regexp-tools/match-based-on-pool-name default-envs effective-pool-name :env :default-value {})))

(defn get-default-pod-labels-for-pool
  "Given a pool name, determine the default labels for pods in that pool"
  [pool-name]
  (let [effective-pool-name (or pool-name config/default-pool "")
        default-pod-labels (get-in config/config [:settings :pools :default-pod-labels])]
    (regexp-tools/match-based-on-pool-name default-pod-labels effective-pool-name :pod-labels :default-value {})))

(defn- add-as-decimals
  "Takes two doubles and adds them as decimals to avoid floating point error. Kubernetes will not be able to launch a
   pod if the required cpu has too much precision. For example, adding 0.1 and 0.02 as doubles results in 0.12000000000000001"
  ; If you get an error like IllegalArgumentException "No suffix for exponent-18" in JSON serialization. Use this function.
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
          (-> (config/kubernetes) :default-checkpoint-config (merge (tools/job-ent->checkpoint job)) :memory-overhead))]
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

(defn resolve-incremental-config-volume-mounts
  "Helper method to resolve a container's volume mounts from an incremental configuration.
  We want to be able to dynamically set the container's volume mounts so that we can incrementally migrate to a
  new version of the software that checkpoints the application."
  [{:keys [incremental-config-volume-mounts-key incremental-config-volume-mounts] :as checkpoint} {:keys [job/uuid]} passport-event-base]
  (if (and incremental-config-volume-mounts-key incremental-config-volume-mounts)
    (let [resolved-key (if (string? incremental-config-volume-mounts-key)
                         incremental-config-volume-mounts-key
                         (config-incremental/resolve-incremental-config uuid incremental-config-volume-mounts-key))]
      (passport/log-event (merge passport-event-base
                                 {:event-type passport/checkpoint-volume-mounts-key-selected
                                  :volume-mounts-key resolved-key}))
      (get incremental-config-volume-mounts resolved-key checkpoint))
    checkpoint))

(defn- checkpoint->volume-mounts
  "Get custom volume mounts needed for checkpointing"
  [{:keys [mode] :as checkpoint} checkpointing-tools-volume job passport-event-base]
  (when (and mode checkpointing-tools-volume)
    (let [volume-mounts-config (resolve-incremental-config-volume-mounts checkpoint job passport-event-base)
          volumes-from-spec-fn #(map (fn [{:keys [path sub-path]}]
                                       (make-volume-mount checkpointing-tools-volume path sub-path false)) %)
          make-volume-mounts-fn (fn [{:keys [init-container-volume-mounts main-container-volume-mounts]}]
                                  {:init-container-checkpoint-volume-mounts (volumes-from-spec-fn init-container-volume-mounts)
                                   :main-container-checkpoint-volume-mounts (volumes-from-spec-fn main-container-volume-mounts)})]
      (make-volume-mounts-fn volume-mounts-config))))

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
  [{:keys [job/checkpoint job/instance job/uuid] :as job} task-id]
  (when checkpoint
    (let [{:keys [default-checkpoint-config]} (config/kubernetes)
          {:keys [max-checkpoint-attempts checkpoint-failure-reasons disable-checkpointing supported-pools] :as checkpoint}
          (merge default-checkpoint-config (tools/job-ent->checkpoint job))]
      (when-not disable-checkpointing
        (and
          (if max-checkpoint-attempts
            (let [checkpoint-failure-reasons (or checkpoint-failure-reasons default-checkpoint-failure-reasons)
                  checkpoint-failures (filter (fn [{:keys [instance/reason]}]
                                                (contains? checkpoint-failure-reasons (:reason/name reason)))
                                              instance)]
              (if (-> checkpoint-failures count (>= max-checkpoint-attempts))
                (log-structured/info "Will not checkpoint, has reached the max checkout failure level"
                                     {:job-id uuid
                                      :max-checkpoint-attempts max-checkpoint-attempts
                                      :number-checkpoint-failures (count checkpoint-failures)
                                      :task-id task-id})
                checkpoint))
            checkpoint)
          (if supported-pools
            (let [pool-name (cached-queries/job->pool-name job)]
              (if (contains? supported-pools pool-name)
                checkpoint
                (log-structured/info "Will not checkpoint, pool is not supported"
                                     {:job-id uuid
                                      :pool pool-name
                                      :task-id task-id})))
            checkpoint))))))

(defn calculate-effective-image
  "Transform the supplied job's image as necessary. e.g. do special transformation if checkpointing is enabled
  and an image transformation function is supplied."
  [{:keys [calculate-effective-image-fn] :as kubernetes-config} job-submit-time image {:keys [mode]} task-id]
  (if (and mode calculate-effective-image-fn)
    (try
      ((util/lazy-load-var-memo calculate-effective-image-fn) kubernetes-config job-submit-time image)
      (catch Exception e
        (log-structured/error "Error calculating effective image for checkpointing"
                              {:calculate-effective-image-fn calculate-effective-image-fn
                               :image image
                               :task-id task-id}
                              e)
        image))
    image))

(defn job->pod-labels
  "Returns the dictionary of labels that should be
  added to the job's pod based on the job's labels
  and/or the job's application fields."
  [job]
  (let [prefix (:add-job-label-to-pod-prefix (config/kubernetes))
        pod-labels-from-job-labels
        (if prefix
          (->> job
               tools/job-ent->label
               (filter (fn [[k _]] (str/starts-with? k prefix)))
               (into {}))
          {})
        add-pod-label-prefix
        (fn [m]
          (pc/map-keys (fn [k] (str prefix "application." k)) m))
        force-valid-pod-label-values
        (fn [m]
          (->> m
               (map
                 (fn [[key val]]
                   ; The workload-* field values are already checked for
                   ; pod-label-value validity at job submission time, but
                   ; the name and version fields existed prior to the k8s
                   ; support in Cook, so we need to coerce names and
                   ; versions that are too long and/or start/end in a
                   ; non-alphanumeric character.
                   (if (or (#{:application/workload-class
                              :application/workload-details
                              :application/workload-id} key)
                           (api/valid-k8s-pod-label-value? val))
                     [key val]
                     (let [shortened-val (if (-> val count (> 61))
                                           (subs val 0 61)
                                           val)
                           prefixed-val (if (Character/isLetterOrDigit ^char (nth shortened-val 0))
                                          shortened-val
                                          (str "0" shortened-val))
                           last-index (-> prefixed-val count dec)
                           suffixed-val (if (Character/isLetterOrDigit ^char (nth prefixed-val last-index))
                                          prefixed-val
                                          (str prefixed-val "0"))
                           final-val (if (api/valid-k8s-pod-label-value? suffixed-val)
                                       suffixed-val
                                       "invalid")]
                       [key final-val]))))
               (into {})))
        pod-labels-from-job-application
        (some-> job
          :job/application
          walk/stringify-keys
          add-pod-label-prefix
          force-valid-pod-label-values)]
    (merge (pod-labels-defaults)
           pod-labels-from-job-labels
           pod-labels-from-job-application)))

(defn set-mem-cpu-resources
  "Given a resources object and a CPU and memory request and limit, update the resources object to reflect the
  desired requests and limits."
  [^V1ResourceRequirements resources memory-request memory-limit cpu-request cpu-limit]
  (let [{:keys [set-container-cpu-limit?]} (config/kubernetes)]
    (.putRequestsItem resources "memory" (double->quantity (* memory-multiplier memory-request)))
    ; set memory-limit if the memory-limit is set
    (when memory-limit
      (.putLimitsItem resources "memory" (double->quantity (* memory-multiplier memory-limit))))
    (.putRequestsItem resources "cpu" (double->quantity cpu-request))
    (when set-container-cpu-limit?
      ; Some environments may need pods to run in the "Guaranteed"
      ; QoS, which requires limits for both memory and cpu
      (.putLimitsItem resources "cpu" (double->quantity cpu-limit)))))

(defn resolve-image-from-incremental-config
  "Helper method to resolve a container's image from an incremental configuration"
  [job passport-event-base passport-event-type image-config image-fallback]
  (let [job-uuid (:job/uuid job)]
    (if (string? image-config)
      image-config
      (let [[resolved-config reason] (config-incremental/resolve-incremental-config job-uuid image-config image-fallback)]
        (passport/log-event (merge passport-event-base
                                   {:event-type passport-event-type
                                    :image-config image-config
                                    :reason reason
                                    :resolved-config resolved-config}))
        resolved-config))))

(defn ^V1Pod task-metadata->pod
  "Given a task-request and other data generate the kubernetes V1Pod to launch that task."
  [namespace {:keys [cook-pool-taint-name cook-pool-taint-prefix cook-pool-taint2-name cook-pool-taint2-value
                     cook-pool-label-name cook-pool-label-prefix] compute-cluster-name :name}
   {:keys [task-id command container task-request hostname pod-annotations pod-constraints pod-hostnames-to-avoid
           pod-labels pod-priority-class pod-supports-cook-init? pod-supports-cook-sidecar?]
    :or {pod-priority-class cook-job-pod-priority-class
         pod-supports-cook-init? true
         pod-supports-cook-sidecar? true}}]
  (let [{:keys [scalar-requests job]} task-request
        ;; NOTE: The scheduler's adjust-job-resources-for-pool-fn may modify :resources,
        ;; whereas :scalar-requests always contains the unmodified job resource values.
        {:strs [mem cpus]} scalar-requests
        {:keys [docker volumes]} container
        {:keys [parameters]} docker
        {:keys [environment]} command
        job-name (:job/name job)
        user (:job/user job)
        pool-name (cached-queries/job->pool-name job)
        pod (V1Pod.)
        pod-spec (V1PodSpec.)
        metadata (V1ObjectMeta.)
        pod-labels (merge (get-default-pod-labels-for-pool pool-name) (job->pod-labels job) pod-labels)
        labels (assoc pod-labels cook-pod-label compute-cluster-name)
        security-context (make-security-context parameters (:user command))
        sandbox-dir (:default-workdir (config/kubernetes))
        workdir (get-workdir parameters sandbox-dir)
        {:keys [volumes volume-mounts sandbox-volume-mount-fn]} (make-volumes volumes sandbox-dir pod-labels)
        {:keys [custom-shell init-container checkpoint-container sidecar telemetry-pool-regex telemetry-agent-host-var-name telemetry-env-var-name
                telemetry-env-value telemetry-service-var-name telemetry-tags-entry-separator
                telemetry-tags-key-invalid-char-pattern telemetry-tags-key-invalid-char-replacement
                telemetry-tags-key-value-separator telemetry-tags-var-name telemetry-version-var-name]}
        (config/kubernetes)
        checkpoint (calculate-effective-checkpointing-config job task-id)
        pod-name (str task-id)
        checkpoint-memory-overhead (:memory-overhead checkpoint)
        use-cook-init? (and init-container pod-supports-cook-init?)
        use-checkpoint-injection? (and checkpoint-container pod-supports-cook-init?)
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
        passport-event-base {:job-name job-name
                             :job-uuid (str (:job/uuid job))
                             :pool pool-name
                             :user user}
        {:keys [init-container-checkpoint-volume-mounts main-container-checkpoint-volume-mounts]}
        (checkpoint->volume-mounts checkpoint checkpoint-volume job passport-event-base)
        sandbox-env {"COOK_SANDBOX" sandbox-dir
                     "HOME" sandbox-dir
                     "MESOS_DIRECTORY" sandbox-dir
                     "MESOS_SANDBOX" sandbox-dir
                     "SIDECAR_WORKDIR" sidecar-workdir}
        params-env (build-params-env parameters)
        progress-env (task/build-executor-environment job)
        checkpoint-env (checkpoint->env checkpoint)
        metadata-env {"COOK_COMPUTE_CLUSTER_NAME" compute-cluster-name
                      "COOK_JOB_NAME" job-name
                      "COOK_JOB_USER" user
                      "COOK_POOL" pool-name
                      "COOK_SCHEDULER_REST_URL" (config/scheduler-rest-url)
                      "USER" user}
        main-env-base (merge environment params-env progress-env sandbox-env checkpoint-env metadata-env)
        progress-file-var (get main-env-base task/progress-meta-env-name task/default-progress-env-name)
        progress-file-path (get main-env-base progress-file-var)
        computed-mem (if checkpoint-memory-overhead (add-as-decimals mem checkpoint-memory-overhead) mem)
        application (:job/application job)
        include-telemetry (some-> telemetry-pool-regex re-pattern (re-matches pool-name))
        main-env (cond-> main-env-base
                   ;; Add a default progress file path to the environment when missing,
                   ;; preserving compatibility with Meosos + Cook Executor.
                   (not progress-file-path)
                   (assoc progress-file-var (str workdir "/" (str (:job/uuid job)) ".progress"))

                   mem
                   (assoc "COOK_USER_MEMORY_REQUEST_BYTES" (* memory-multiplier mem))

                   computed-mem
                   (assoc "COOK_MEMORY_REQUEST_BYTES" (* memory-multiplier computed-mem))

                   (and include-telemetry telemetry-env-var-name telemetry-env-value)
                   (assoc telemetry-env-var-name
                          telemetry-env-value)

                   (and include-telemetry telemetry-service-var-name)
                   (assoc telemetry-service-var-name
                          (or (:application/name application) "cook-job"))

                   (and include-telemetry
                        telemetry-tags-entry-separator
                        telemetry-tags-key-value-separator
                        telemetry-tags-var-name)
                   (assoc telemetry-tags-var-name
                          (->> pod-labels
                               (map (fn [[k v]]
                                      (str (if (and telemetry-tags-key-invalid-char-pattern
                                                    telemetry-tags-key-invalid-char-replacement)
                                             (str/replace
                                               k
                                               telemetry-tags-key-invalid-char-pattern
                                               telemetry-tags-key-invalid-char-replacement)
                                             k)
                                           telemetry-tags-key-value-separator
                                           v)))
                               (str/join telemetry-tags-entry-separator)))

                   (and include-telemetry telemetry-version-var-name)
                   (assoc telemetry-version-var-name
                          (or (:application/version application) "undefined")))
        main-env-vars (cond->> (-> main-env
                                 (merge (get-default-env-for-pool pool-name))
                                 make-filtered-env-vars)
                        (and include-telemetry telemetry-agent-host-var-name)
                        (cons (doto
                                (V1EnvVar.)
                                (.setName telemetry-agent-host-var-name)
                                (.valueFrom hostIpEnvVarSource))))]

    ; metadata
    (.setName metadata pod-name)
    (.setNamespace metadata namespace)
    (.setLabels metadata labels)
    ; Only include finalizers with real pods.
    (when-not (synthetic-pod? pod-name)
      (let [[resolved-config reason] (config-incremental/resolve-incremental-config task-id :add-finalizer "false")]
        (if (= "true" resolved-config)
          (.setFinalizers metadata (list FinalizerHelper/collectResultsFinalizer)))))
    (let [pod-annotations'
          (if (synthetic-pod? pod-name)
            pod-annotations
            ; For non-synthetic (real job) pods, when configured to do so,
            ; we add a pod annotation indicating that Kubernetes should
            ; use all of the group IDs the user is a member of, in order
            ; to avoid contradictions between which group Cook thinks a
            ; user belongs to and which group Kubernetes thinks the user
            ; belongs to.
            (let [[resolved-config _]
                  (config-incremental/resolve-incremental-config
                    task-id :add-use-all-gids-annotation "false")
                  use-all-gids-annotation-name
                  (:use-all-gids-annotation-name (config/kubernetes))]
              (cond-> pod-annotations
                (and
                  (= "true" resolved-config)
                  use-all-gids-annotation-name)
                (assoc use-all-gids-annotation-name "true"))))]
      (when (seq pod-annotations')
        (.setAnnotations metadata pod-annotations')))

    (.setHostnameAsFQDN pod-spec false)

    (let [{:keys [image]} docker
          checkpoint (calculate-effective-checkpointing-config job task-id)
          job-submit-time (tools/job->submit-time job)
          image (if (synthetic-pod? pod-name)
                  image
                  (calculate-effective-image (config/kubernetes) job-submit-time image checkpoint task-id))]

      ; main container
      (let [{:keys [resources]} task-request
            ;; NOTE: The scheduler's adjust-job-resources-for-pool-fn may modify :resources,
            ;; whereas :scalar-requests always contains the unmodified job resource values.
            {:keys [port-mapping]} docker
            ; gpu count is not stored in scalar-requests because Fenzo does not handle gpus in binpacking
            gpus (or (:gpus resources) 0)
            gpu-model-requested (constraints/job->gpu-model-requested gpus job pool-name)
            ;; if disk config for pool has enable-constraint? set to true, add disk info to the job
            enable-disk-constraint? (regexp-tools/match-based-on-pool-name (config/disk) pool-name :enable-constraint? :default-value false)
            ;; if user did not specify disk request, use default on pool
            disk-request (when enable-disk-constraint?
                           (constraints/job-resources->disk-request resources pool-name))
            disk-limit (when enable-disk-constraint? (-> resources :disk :limit))
            ;; if user did not specify disk type, use default on pool
            disk-type (when enable-disk-constraint? (constraints/job-resources->disk-type resources pool-name))
            container (V1Container.)
            resources (V1ResourceRequirements.)]
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

        ; Don't add memory limit if user sets job label to allow memory usage above request to "true"
        (let [allow-memory-usage-above-request (some->> (:memory-limit-job-label-name (config/kubernetes))
                                                        (get (tools/job-ent->label job))
                                                        clojure.string/lower-case
                                                        (= "true"))]
          (set-mem-cpu-resources resources computed-mem (when-not allow-memory-usage-above-request computed-mem) cpus cpus))

        (when (pos? gpus)
          (.putLimitsItem resources "nvidia.com/gpu" (double->quantity gpus))
          (.putRequestsItem resources "nvidia.com/gpu" (double->quantity gpus))
          (add-node-selector pod-spec "cloud.google.com/gke-accelerator" gpu-model-requested)
          ; GKE nodes with GPUs have gpu-count label, so synthetic pods need a matching node selector
          (add-node-selector pod-spec "gpu-count" (-> gpus int str)))
        (when disk-request
          ; do not add disk request/limit to synthetic pod yaml because GKE CA will not scale up from 0 if unschedulable pods
          ; require ephemeral-storage: https://github.com/kubernetes/autoscaler/issues/1869
          (when-not (synthetic-pod? pod-name)
            (.putRequestsItem resources "ephemeral-storage" (double->quantity (* constraints/disk-multiplier disk-request)))
            ; by default, do not set disk-limit
            (when disk-limit
              (.putLimitsItem resources "ephemeral-storage" (double->quantity (* constraints/disk-multiplier disk-limit)))))
          (add-node-selector pod-spec (pool->disk-type-label-name pool-name) disk-type))
        (.setResources container resources)
        (.setVolumeMounts container (filterv some? (conj (concat volume-mounts main-container-checkpoint-volume-mounts)
                                                         (init-container-workdir-volume-mount-fn true)
                                                         (scratch-space-volume-mount-fn false)
                                                         (sidecar-workdir-volume-mount-fn true))))
        (.setWorkingDir container workdir)

        (.addContainersItem pod-spec container))


      (let [get-resource-requirements-fn (fn [fieldname] (if use-cook-sidecar?
                                                           (get-in sidecar [:resource-requirements fieldname])
                                                           0))
            total-memory-request (add-as-decimals computed-mem (get-resource-requirements-fn :memory-request))
            total-memory-limit (add-as-decimals computed-mem (get-resource-requirements-fn :memory-limit))
            total-cpu-request (add-as-decimals cpus (get-resource-requirements-fn :cpu-request))
            total-cpu-limit (add-as-decimals cpus (get-resource-requirements-fn :cpu-limit))]
        ;init container
        (when use-cook-init?
          (let [{:keys [command image image-fallback]} init-container
                container (V1Container.)
                resources (V1ResourceRequirements.)]
            ; container
            (.setName container cook-init-container-name)
            (.setImage container (resolve-image-from-incremental-config
                                   job passport-event-base passport/init-container-image-selected
                                   image image-fallback))
            (.setCommand container command)
            ; Shares the same workdir as the checkpoint container.
            (.setWorkingDir container init-container-workdir)
            (.setEnv container main-env-vars)
            (.setVolumeMounts container (filterv some? (concat [(init-container-workdir-volume-mount-fn false)
                                                                (scratch-space-volume-mount-fn false)]
                                                               init-container-checkpoint-volume-mounts)))
            (set-mem-cpu-resources resources total-memory-request total-memory-limit total-cpu-request total-cpu-limit)
            (.setResources container resources)
            (.addInitContainersItem pod-spec container)))

        ; checkpoint injection container
        ; This container is used for injecting checkpoint/restore functionality into the user's main container.
        ;
        ; This is run in the same image as the user's container, but run with an alternate command line. The reason is that
        ; the init container may not use the same platform image as the user's container, so certain binaries or other
        ; images can't be copied between those container.
        ;
        ; The idea is the init container injects some scripts into a shared volume used by the checkpoint container.
        ; The checkpoint container then runs. As it runs with the same image as the user container, those scripts can
        ; inspect the platform the user container is on and install the right checkpoint or other code.
        (when use-checkpoint-injection?
          (let [{:keys [command]} checkpoint-container
                container (V1Container.)
                resources (V1ResourceRequirements.)]
            ; container
            (.setName container checkpoint-cook-init-container-name)
            (.setImage container image)
            (.setCommand container command)
            ; Shares the same workdir as the init container.
            (.setWorkingDir container init-container-workdir)
            (.setEnv container main-env-vars)
            (.setVolumeMounts container (filterv some? (concat [(init-container-workdir-volume-mount-fn false)
                                                                (scratch-space-volume-mount-fn false)]
                                                               init-container-checkpoint-volume-mounts)))
            (set-mem-cpu-resources resources total-memory-request total-memory-limit total-cpu-request total-cpu-limit)
            (.setResources container resources)
            (.addInitContainersItem pod-spec container))))

      ; sandbox file server container
      (when use-cook-sidecar?
        (let [{:keys [command health-check-endpoint image image-fallback port resource-requirements]} sidecar
              {:keys [cpu-request cpu-limit memory-request memory-limit]} resource-requirements
              container (V1Container.)
              resources (V1ResourceRequirements.)]
          ; container
          (.setName container cook-container-name-for-file-server)
          (.setImage container (resolve-image-from-incremental-config
                                 job passport-event-base passport/sidecar-image-selected
                                 image image-fallback))
          (.setCommand container (conj command (str port)))
          (.setWorkingDir container sidecar-workdir)
          (.setPorts container [(.containerPort (V1ContainerPort.) (int port))])

          (.setEnv container (conj main-env-vars
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
          (set-mem-cpu-resources resources memory-request memory-limit cpu-request cpu-limit)
          (.setResources container resources)

          (.setVolumeMounts container [(sandbox-volume-mount-fn true) (sidecar-workdir-volume-mount-fn false) (scratch-space-volume-mount-fn false)])
          (.addContainersItem pod-spec container))))

    ; We're either using the hostname (in the case of users' job pods)
    ; or pod-hostnames-to-avoid (in the case of synthetic pods), but not both.
    (if hostname
      ; Why not just set the node name?
      ; The reason is that setting the node name prevents pod preemption
      ; (https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/)
      ; from happening. We want this pod to preempt lower priority pods
      ; (e.g. synthetic pods).
      (do
        (add-node-selector pod-spec k8s-hostname-label hostname)
        ; Allow real pods to run on tenured nodes.
        (.addTolerationsItem pod-spec toleration-tenured-node))
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
    ; Cook supports two kinds of tolerations on nodes.
    ; 1. It can tolerate a node, where the value of that toleration depends on the pool name.
    ; I.e.,  <KEY1>=<PREFIX><POOLNAME>
    (when cook-pool-taint-name
      (.addTolerationsItem pod-spec (toleration-for-pool cook-pool-taint-name cook-pool-taint-prefix pool-name)))
    ; 2. Or it can statically tolerate a particular KEY2 and VAL2.
    (when cook-pool-taint2-name
      (.addTolerationsItem pod-spec (toleration-for-pool2 cook-pool-taint2-name cook-pool-taint2-value)))
    ; We need to make sure synthetic pods --- which don't have a hostname set --- have a node selector
    ; to run only in nodes labelled with the appropriate cook pool
    (when-not hostname (add-node-selector pod-spec cook-pool-label-name (str cook-pool-label-prefix pool-name)))

    (when pod-constraints
      (doseq [{:keys [constraint/attribute
                      constraint/operator
                      constraint/pattern]}
              pod-constraints]
        (condp = operator
          :constraint.operator/equals
          ; Add a node selector for any EQUALS constraints
          ; either defined by the user on their job spec
          ; or defaulted on the Cook pool via configuration
          (add-node-selector pod-spec attribute pattern)
          :else
          ; A bad constraint operator should have
          ; been blocked at job submission time
          (throw (ex-info "Encountered unexpected constraint operator"
                          {:operator operator
                           :task-id task-id})))))

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

    (when (synthetic-pod? pod-name)
      ; We want to allow synthetic pods to have a non-default (typically 0) termination grace period,
      ; so that they can be deleted quickly to free up space on nodes for real job pods. The default
      ; grace period of 30 seconds can cause real job pods to be deemed unschedulable and fail.
      (when-let [{:keys [synthetic-pod-termination-grace-period-seconds]} (config/kubernetes)]
        (.setTerminationGracePeriodSeconds pod-spec synthetic-pod-termination-grace-period-seconds))

      ; We want to allow synthetic pods to be repelled from certain nodes via inter-pod (anti-)affinity
      ; (https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#inter-pod-affinity-and-anti-affinity)
      ; in order to repel them from "tenured" nodes, e.g. by running a "repeller" pod on nodes that
      ; have been up and running for a certain amount of time. Without this, synthetic pods will often
      ; run on nodes that have been alive for a while (tenured nodes) when job pods complete and free
      ; up space, causing those synthetic pods to not serve their purpose of triggering scale-up.
      (let [{:keys [synthetic-pod-anti-affinity-namespace
                    synthetic-pod-anti-affinity-pod-label-key
                    synthetic-pod-anti-affinity-pod-label-value]}
            (config/kubernetes)]
        (when (and synthetic-pod-anti-affinity-pod-label-key
                   synthetic-pod-anti-affinity-pod-label-value)
          ; If the synthetic pod spec already has an affinity defined (see the code for
          ; pod-hostnames-to-avoid above), we add to it; otherwise, we create a new one
          (let [affinity (or (.getAffinity pod-spec) (V1Affinity.))
                pod-anti-affinity (V1PodAntiAffinity.)
                pod-affinity-term (V1PodAffinityTerm.)
                label-selector (V1LabelSelector.)]
            (.setMatchLabels label-selector
                             {synthetic-pod-anti-affinity-pod-label-key
                              synthetic-pod-anti-affinity-pod-label-value})
            (.setLabelSelector pod-affinity-term label-selector)
            (.setTopologyKey pod-affinity-term k8s-hostname-label)
            (when synthetic-pod-anti-affinity-namespace
              (.addNamespacesItem pod-affinity-term synthetic-pod-anti-affinity-namespace))
            (.setRequiredDuringSchedulingIgnoredDuringExecution pod-anti-affinity [pod-affinity-term])
            (.setPodAntiAffinity affinity pod-anti-affinity)
            (.setAffinity pod-spec affinity)))))

    pod))

(defn V1Pod->name
  "Extract the name of a pod from the pod itself"
  [^V1Pod pod]
  (-> pod .getMetadata .getName))

(defn pod-unschedulable?
  "Returns true if the given pod status has a PodScheduled
  condition with status False and reason Unschedulable"
  [^V1Pod pod]
  (let [pod-status (.getStatus pod)
        {:keys [pod-condition-unschedulable-seconds
                synthetic-pod-condition-unschedulable-seconds]}
        (config/kubernetes)
        unschedulable-seconds
        (if (or (some-> (V1Pod->name pod) synthetic-pod?)
                (some-> pod kubernetes-scheduler-pod?))
          synthetic-pod-condition-unschedulable-seconds
          pod-condition-unschedulable-seconds)]
    (some->> pod-status
             .getConditions
             (some
              (fn pod-condition-unschedulable?
                [^V1PodCondition condition]
                (and (-> condition .getType (= "PodScheduled"))
                     (-> condition .getStatus (= "False"))
                     (-> condition .getReason (= "Unschedulable"))
                     (-> condition
                         .getLastTransitionTime
                         (t/plus (t/seconds unschedulable-seconds))
                         (t/before? (t/now)))))))))

(defn pod-has-stuck-condition?
  "Returns true if the given pod status has a pod condition that is deemed stuck,
  meaning that it's been in some unready state for long enough that we assume it
  will not recover, and we should fail the job instance altogether."
  [pod-name ^V1PodStatus pod-status pod-condition-type pod-condition-reason pod-condition-seconds]
  (let [^V1PodCondition pod-condition
        (some->> pod-status
                 .getConditions
                 (filter
                   (fn pod-condition-of-interest?
                     [^V1PodCondition condition]
                     (and (-> condition .getType (= pod-condition-type))
                          (-> condition .getStatus (= "False"))
                          (-> condition .getReason (= pod-condition-reason)))))
                 first)]
    (when pod-condition
      (let [last-transition-time-plus-threshold-seconds
            (-> pod-condition
                .getLastTransitionTime
                (t/plus (t/seconds pod-condition-seconds)))
            now (t/now)
            threshold-passed? (t/before? last-transition-time-plus-threshold-seconds now)]
        (log-structured/info "Pod has unready pod condition"
                             {:pod-name pod-name
                              :last-transition-time-plus-threshold-seconds (print-str last-transition-time-plus-threshold-seconds)
                              :now (print-str now)
                              :pod-condition (print-str pod-condition)
                              :pod-condition-reason pod-condition-reason
                              :pod-condition-seconds pod-condition-seconds
                              :pod-condition-type pod-condition-type
                              :threshold-passed? threshold-passed?})
        threshold-passed?))))

(defn pod-containers-not-initialized?
  "Returns true if the given pod status has an Initialized condition with status
  False and reason ContainersNotInitialized, and the last transition was more than
  pod-condition-containers-not-initialized-seconds seconds ago"
  [pod-name ^V1PodStatus pod-status]
  (pod-has-stuck-condition?
    pod-name
    pod-status
    "Initialized"
    "ContainersNotInitialized"
    (:pod-condition-containers-not-initialized-seconds (config/kubernetes))))

(defn pod-containers-not-ready?
  "Returns true if the given pod status has a ContainersReady condition with status
  False and reason ContainersNotReady, and the last transition was more than
  pod-condition-containers-not-ready-seconds seconds ago"
  [pod-name ^V1PodStatus pod-status]
  (pod-has-stuck-condition?
    pod-name
    pod-status
    "ContainersReady"
    "ContainersNotReady"
    (:pod-condition-containers-not-ready-seconds (config/kubernetes))))

(defn has-old-deletion-timestamp?
  "Returns true if the given pod has a deletion timestamp that is older
  than the configurable timeout, meaning we should hard-kill the pod"
  [^V1Pod pod]
  (let [{:keys [pod-deletion-timeout-seconds]} (config/kubernetes)
        pod-metadata (some-> pod .getMetadata)
        pod-deletion-timestamp (some-> pod-metadata .getDeletionTimestamp)]
    (and pod-deletion-timestamp
         pod-deletion-timeout-seconds
         (-> pod-deletion-timestamp
             (t/plus (t/seconds pod-deletion-timeout-seconds))
             (t/before? (t/now))))))

(defn pod->synthesized-pod-state
  "From a V1Pod object, determine the state of the pod, waiting running, succeeded, failed or unknown.

   Kubernetes doesn't really have a good notion of 'pod state'. For one thing, that notion can't be universal across
   applications. Thus, we synthesize that state by looking at the containers within the pod and applying our own
   business logic."
  [pod-name ^V1Pod pod]
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
          ^V1ContainerStatus job-status (first (filter (fn [^V1ContainerStatus c] (= cook-container-name-for-job (.getName c)))
                                                       container-statuses))
          {:keys [node-preempted-label]} (config/kubernetes)
          ^V1ObjectMeta pod-metadata (some-> pod .getMetadata)
          pod-preempted-timestamp
          (some-> pod-metadata
                  .getLabels
                  (get (or node-preempted-label "node-preempted")))
          pod-deletion-timestamp (some-> pod-metadata .getDeletionTimestamp)
          synthesized-pod-state
          (if pod-deletion-timestamp
            {:state :pod/deleting
             :reason "Pod was explicitly deleted"}
            ; If pod isn't being async removed, then look at the containers inside it.
            (if job-status
              (let [^V1ContainerState state (.getState job-status)]
                (cond
                  (.getWaiting state)
                  (cond
                    (pod-containers-not-initialized? pod-name pod-status)
                    ; If the containers are not getting initialized,
                    ; then we should consider the pod failed. This
                    ; state can occur, for example, when volume
                    ; mounts fail.
                    {:state :pod/failed
                     :reason "ContainersNotInitialized"}

                    (pod-containers-not-ready? pod-name pod-status)
                    {:state :pod/failed
                     :reason "ContainersNotReady"}

                    :default
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
                ; pod-unschedulable? uses a different timeout for synthetic pods, because
                ; they will be unschedulable by design, in order to trigger the cluster
                ; autoscaler to scale up. For non-synthetic pods, however, this state
                ; likely means something changed about the node we matched to. For example,
                ; if the ToBeDeletedByClusterAutoscaler taint gets added between when we
                ; saw available capacity on a node and when we submitted the pod to that
                ; node, then the pod will never get scheduled.
                (pod-unschedulable? pod)
                (let [synthesized-state {:state :pod/failed
                                         :reason "Unschedulable"}]
                  (log-structured/info "Encountered unschedulable pod"
                                       {:pod-name pod-name
                                        :pod-status (print-str pod-status)
                                        :synthesized-state (print-str synthesized-state)})
                  synthesized-state)

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
              (fn [^V1ContainerStatus c]
                (= cook-container-name-for-file-server (.getName c)))
              container-statuses))]
      (cond
        (nil? file-server-status) :unknown
        (.getReady file-server-status) :running
        :else :not-running))))

(defn delete-pod
  "Kill this kubernetes pod. This is the same as deleting it."
  [^ApiClient api-client compute-cluster-name ^V1Pod pod & {:keys [grace-period-seconds]}]
  (let [api (CoreV1Api. api-client)
        ^V1DeleteOptionsBuilder delete-options-builder
        (cond-> (V1DeleteOptionsBuilder.)
          true (.withPropagationPolicy "Background")
          grace-period-seconds (.withGracePeriodSeconds ^long grace-period-seconds))
        ^V1DeleteOptions deleteOptions (.build delete-options-builder)
        pod-name (-> pod .getMetadata .getName)
        pod-namespace (-> pod .getMetadata .getNamespace)]
    ; TODO: This likes to noisily throw NotFound multiple times as we delete away from kubernetes.
    ; I suspect our predicate of k8s-actual-state-equivalent needs tweaking.
    (prom/with-duration
      prom/delete-pod-duration {:compute-cluster compute-cluster-name}
      (timers/time!
        (metrics/timer "delete-pod" compute-cluster-name)
        (try
          (.deleteNamespacedPod
            api
            pod-name
            pod-namespace
            nil ; pretty
            nil ; dryRun
            nil ; gracePeriodSeconds
            nil ; orphanDependents
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
            (log-structured/info "Caught the https://github.com/kubernetes-client/java/issues/252 exception deleting pod"
                                 {:pod-name pod-name :compute-cluster compute-cluster-name}))
          (catch ApiException e
            (prom/inc prom/delete-pod-errors {:compute-cluster compute-cluster-name})
            (meters/mark! (metrics/meter "delete-pod-errors" compute-cluster-name))
            (let [code (.getCode e)
                  already-deleted? (contains? #{404} code)]
              (if already-deleted?
                (log-structured/info "Pod was already deleted"
                                     {:pod-name pod-name :compute-cluster compute-cluster-name})
                (throw e)))))))))

(defn delete-collect-results-finalizer
  "Delete the finalizer that collects the results of a pod completion state once the pod has a
  deletion timestamp"
  [{:keys [compute-cluster-name api-client name]} pod-name ^V1Pod pod]
  (let [^V1ObjectMeta pod-metadata (some-> pod .getMetadata)
        pod-deletion-timestamp (some-> pod .getMetadata .getDeletionTimestamp)
        ^List finalizers (some-> pod-metadata .getFinalizers)]
    (when (and finalizers (> (.size finalizers) 1))
      ; Finalizers are stored in a list and deleted by index. If there's more than one,
      ; cook can race with whatever deletes the other one, or even delete both (see race discussed below)
      (log-structured/error (print-str "Deleting finalizer pod. Cook's deletion of more than one finalizer on a pod is
                             racy and not supported. Finalizers are" finalizers)
                            {:pod-name pod-name :compute-cluster compute-cluster-name}))
    (when (and pod-deletion-timestamp (some #(= FinalizerHelper/collectResultsFinalizer %) finalizers))
      (log-structured/info "Deleting finalizer for pod"
                           {:pod-name pod-name :compute-cluster compute-cluster-name})
      (prom/with-duration
        prom/delete-finalizer-duration {:compute-cluster compute-cluster-name}
        (timers/time!
          (metrics/timer "delete-finalizer" compute-cluster-name)
          (try
            (FinalizerHelper/removeFinalizer api-client pod)
            (catch ApiException e
              (let [code (.getCode e)]
                ; There can be races here. Whenever we hit the state machine, we will run this code
                ; and may try to delete the finalizer twice. The second invocation can see the pod
                ; already deleted or it can see an unprocesssable entity as we try to delete a finalizer
                ; that's already gone.
                (case code
                  404
                  (do
                    (prom/inc prom/delete-finalizer-errors {:compute-cluster compute-cluster-name :type "expected"})
                    (meters/mark! (metrics/meter "delete-finalizer-expected-errors" compute-cluster-name))
                    (log-structured/info "Not Found error deleting finalizer for pod"
                                         {:pod-name pod-name :compute-cluster compute-cluster-name}))
                  422
                  (do
                    (prom/inc prom/delete-finalizer-errors {:compute-cluster compute-cluster-name :type "expected"})
                    (meters/mark! (metrics/meter "delete-finalizer-expected-errors" compute-cluster-name))
                    (log-structured/info "Unprocessable Entity error deleting finalizer for pod"
                                         {:pod-name pod-name :compute-cluster compute-cluster-name}))
                  (do
                    (prom/inc prom/delete-finalizer-errors {:compute-cluster compute-cluster-name :type "unexpected"})
                    (meters/mark! (metrics/meter "delete-finalizer-unexpected-errors" compute-cluster-name))
                    (log-structured/error "Error deleting finalizer for pod"
                                          {:pod-name pod-name :compute-cluster compute-cluster-name}
                                          e)))))
            (catch Exception e
              (prom/inc prom/delete-finalizer-errors {:compute-cluster compute-cluster-name :type "unexpected"})
              (meters/mark! (metrics/meter "delete-finalizer-unexpected-errors" compute-cluster-name))
              (log-structured/error "Error deleting finalizer for pod"
                                    {:pod-name pod-name :compute-cluster compute-cluster-name}
                                    e))))))))



(defn create-namespaced-pod
  "Delegates to the k8s API .createNamespacedPod function"
  [^CoreV1Api api namespace pod]
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
      (let [{:keys [^V1Pod pod]} launch-pod
            pod-name-from-pod (-> pod .getMetadata .getName)
            namespace (-> pod .getMetadata .getNamespace)
            api (CoreV1Api. api-client)
            synthetic? (synthetic-pod? pod-name)
            passport-base-map
            (merge
              (pod-name->job-map pod-name)
              {:compute-cluster compute-cluster-name
               :namespace namespace})
            start-time (System/currentTimeMillis)]
        (assert (= pod-name-from-pod pod-name)
                (str "Pod name from pod (" pod-name-from-pod ") "
                     "does not match pod name argument (" pod-name ")"))
        (log-structured/info "Launching pod"
                             {:compute-cluster compute-cluster-name
                              :pod-name pod-name
                              :pod-json (print-str (.serialize json pod))
                              :pod-namespace namespace})
        (try
          (prom/with-duration
            prom/launch-pod-duration {:compute-cluster compute-cluster-name}
            (timers/time!
              (metrics/timer "launch-pod" compute-cluster-name)
              (create-namespaced-pod api namespace pod)))
          (passport/log-event
            (merge
              passport-base-map
              {:duration-ms (- (System/currentTimeMillis) start-time)
               :event-type
               (if synthetic?
                 passport/synthetic-pod-submission-succeeded
                 passport/pod-submission-succeeded)}))
          {:terminal-failure? false}
          (catch ApiException e
            (let [duration-ms (- (System/currentTimeMillis) start-time)
                  code (.getCode e)
                  bad-pod-spec? (contains? #{400 404 422} code)
                  k8s-api-error? (= 500 code)
                  terminal-failure? (or bad-pod-spec? k8s-api-error?)
                  failure-reason
                  (when terminal-failure?
                    (cond
                      bad-pod-spec? :reason-task-invalid
                      k8s-api-error? :reason-pod-submission-api-error))
                  passport-event-type
                  (if synthetic?
                    passport/synthetic-pod-submission-failed
                    passport/pod-submission-failed)]
              (passport/log-event
                (merge
                  passport-base-map
                  {:bad-pod-spec? bad-pod-spec?
                   :code code
                   :duration-ms duration-ms
                   :event-type passport-event-type
                   :k8s-api-error? k8s-api-error?
                   :terminal-failure? terminal-failure?}))
              (log-structured/info "Error submitting pod"
                        {:compute-cluster compute-cluster-name
                         :bad-pod-spec? bad-pod-spec?
                         :code code
                         :duration-ms duration-ms
                         :failure-reason failure-reason
                         :k8s-api-error? k8s-api-error?
                         :pod-namespace namespace
                         :pod-name pod-name
                         :response-body (.getResponseBody e)
                         :terminal-failure? terminal-failure?}
                        e)
              (if bad-pod-spec?
                (do
                  (prom/inc prom/launch-pod-errors {:compute-cluster compute-cluster-name :bad-spec "true"})
                  (meters/mark! (metrics/meter "launch-pod-bad-spec-errors" compute-cluster-name)))
                (do
                  (prom/inc prom/launch-pod-errors {:compute-cluster compute-cluster-name :bad-spec "false"})
                  (meters/mark! (metrics/meter "launch-pod-good-spec-errors" compute-cluster-name))))
              {:failure-reason failure-reason
               :terminal-failure? terminal-failure?}))))
      (do
        ; Because of the complicated nature of task-metadata-seq, we can't easily run the pod
        ; creation code for launching a pod on a server restart. Thus, if we create an instance,
        ; store into datomic, but then the Cook Scheduler exits --- before k8s creates a pod (either
        ; the message isn't sent, or there's a k8s problem) --- we will be unable to create a new
        ; pod and we can't retry this at the kubernetes level.
        (log-structured/info "Unable to launch pod because we do not reconstruct the pod on startup:"
                             {:pod-name pod-name :compute-cluster compute-cluster-name})
        {:failure-reason :reason-could-not-reconstruct-pod
         :terminal-failure? true}))))


;; TODO: Need the 'stuck pod scanner' to detect stuck states and move them into killed.
