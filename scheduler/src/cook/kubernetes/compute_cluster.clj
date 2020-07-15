(ns cook.kubernetes.compute-cluster
  (:require [chime :refer [chime-at chime-ch]]
            [clj-time.core :as time]
            [clj-time.periodic :refer [periodic-seq]]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [cook.compute-cluster :as cc]
            [cook.compute-cluster.metrics :as ccmetrics]
            [cook.config :as config]
            [cook.kubernetes.api :as api]
            [cook.kubernetes.controller :as controller]
            [cook.kubernetes.metrics :as metrics]
            [cook.monitor :as monitor]
            [cook.pool]
            [cook.scheduler.constraints :as constraints]
            [cook.tools :as tools]
            [datomic.api :as d]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [plumbing.core :as pc])
  (:import (com.google.auth.oauth2 GoogleCredentials)
           (io.kubernetes.client.openapi ApiClient)
           (io.kubernetes.client.openapi.models V1Node V1Pod)
           (io.kubernetes.client.util Config)
           (java.io File FileInputStream)
           (java.util.concurrent Executors ExecutorService ScheduledExecutorService TimeUnit)
           (java.util UUID)
           (okhttp3 OkHttpClient$Builder)))

(defn schedulable-node-filter
  "Is a node schedulable?"
  [node-name->node node-name->pods {:keys [node-blocklist-labels] :as compute-cluster} [node-name _]]
  (if-let [^V1Node node (node-name->node node-name)]
    (api/node-schedulable? node (cc/max-tasks-per-host compute-cluster) node-name->pods node-blocklist-labels)
    (do
      (log/error "In" (cc/compute-cluster-name compute-cluster)
                 "compute cluster, unable to get node from node name" node-name)
      false)))

(defn total-resource
  "Given a map from node-name->resource-keyword->amount and a resource-keyword,
  returns the total amount of that resource for all nodes."
  [node-name->resource-map resource-keyword]
  (->> node-name->resource-map vals (map resource-keyword) (reduce +)))

(defn total-gpu-resource
  "Given a map from node-name->resource-keyword->amount,
  returns a map from gpu model to count for all nodes."
  [node-name->resource-map]
  (->> node-name->resource-map vals (map :gpus) (apply merge-with +)))

(defn generate-offers
  "Given a compute cluster and maps with node capacity and existing pods, return a map from pool to offers."
  [compute-cluster node-name->node node-name->pods]
  (let [compute-cluster-name (cc/compute-cluster-name compute-cluster)
        node-name->capacity (api/get-capacity node-name->node)
        node-name->consumed (api/get-consumption node-name->pods)
        node-name->available (tools/deep-merge-with - node-name->capacity node-name->consumed)
        ; Grab every unique GPU model being represented so that we can set counters for capacity and consumed for each GPU model
        gpu-models (->> node-name->capacity vals (map :gpus) (apply merge) keys set)
        ; The following variables are only being used setting counters for monitor
        gpu-model->total-capacity (total-gpu-resource node-name->capacity)
        gpu-model->total-consumed (total-gpu-resource node-name->consumed)]

    (log/info "In" compute-cluster-name "compute cluster, capacity:" node-name->capacity)
    (log/info "In" compute-cluster-name "compute cluster, consumption:" node-name->consumed)
    (log/info "In" compute-cluster-name "compute cluster, filtering out"
              (->> node-name->available
                   (remove #(schedulable-node-filter node-name->node node-name->pods compute-cluster %))
                   count)
              "nodes as not schedulable")

    (monitor/set-counter! (metrics/counter "capacity-cpus" compute-cluster-name)
                          (total-resource node-name->capacity :cpus))
    (monitor/set-counter! (metrics/counter "capacity-mem" compute-cluster-name)
                          (total-resource node-name->capacity :mem))
    (monitor/set-counter! (metrics/counter "consumption-cpus" compute-cluster-name)
                          (total-resource node-name->consumed :cpus))
    (monitor/set-counter! (metrics/counter "consumption-mem" compute-cluster-name)
                          (total-resource node-name->consumed :mem))

    (doseq [gpu-model gpu-models]
      (monitor/set-counter! (metrics/counter (str "capacity-gpu-" gpu-model) compute-cluster-name)
                            (get gpu-model->total-capacity gpu-model))
      (monitor/set-counter! (metrics/counter (str "consumption-gpu-" gpu-model) compute-cluster-name)
                            (get gpu-model->total-consumed gpu-model 0)))


    (->> node-name->available
         (filter #(schedulable-node-filter node-name->node node-name->pods compute-cluster %))
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
                               {:name "disk" :type :value-scalar :scalar 0.0}
                               {:name "gpus" :type :value-text->scalar :text->scalar (:gpus available)}]
                   :attributes (conj node-label-attributes
                                     {:name "compute-cluster-type"
                                      :type :value-text
                                      :text "kubernetes"})
                   :executor-ids []
                   :compute-cluster compute-cluster
                   :reject-after-match-attempt true
                   :offer-match-timer (timers/start (ccmetrics/timer "offer-match-timer" compute-cluster-name))}))))))

(defn taskids-to-scan
  "Determine all task ids to scan by union-ing task id's from cook expected state and existing task id maps. "
  [{:keys [cook-expected-state-map k8s-actual-state-map] :as kcc}]
  (->>
    (set/union (keys @cook-expected-state-map) (keys @k8s-actual-state-map))
    (into #{})))

(defn scan-tasks
  "Scan all taskids. Note: May block or be slow due to rate limits."
  [{:keys [name] :as kcc}]
  (log/info "In" name "compute cluster, starting taskid scan")
  ; TODO Add in rate limits; only visit non-running/running task so fast.
  ; TODO Add in maximum-visit frequency. Only visit a task once every XX seconds.
  (let [taskids (taskids-to-scan kcc)]
    (log/info "In" name "compute cluster, doing taskid scan. Visiting" (count taskids) "taskids")
    (doseq [^String taskid taskids]
      (try
        (log/info "In" name "compute cluster, doing scan of" taskid)
        (controller/scan-process kcc taskid)
        (catch Exception e
          (log/error e "In" name
                     "compute cluster, encountered exception scanning task"
                     taskid))))))

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
  [kcc]
  (fn pod-watch-callback
    [_ prev-pod pod]
    (try
      (if (nil? pod)
        (controller/pod-deleted kcc prev-pod)
        (controller/pod-update kcc pod))
      (catch Exception e
        (log/error e "Error processing status update")))))

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
  all tasks that are in the running state as well as all pods in kubernetes. We're given an already existing list of
  all running tasks entities (via (->> (cook.tools/get-running-task-ents)."
  [conn api-client compute-cluster-name running-tasks-ents]
  (let [db (d/db conn)
        [_ pod-name->pod] (api/try-forever-get-all-pods-in-kubernetes api-client compute-cluster-name)
        all-tasks-ids-in-pods (into #{} (keys pod-name->pod))
        _ (log/debug "All tasks in pods (for initializing cook expected state): " all-tasks-ids-in-pods)
        running-tasks-in-cc-ents (filter
                                   #(-> % cook.task/task-entity->compute-cluster-name (= compute-cluster-name))
                                   running-tasks-ents)
        running-task-id->task (task-ents->map-by-task-id running-tasks-in-cc-ents)
        cc-running-tasks-ids (->> running-task-id->task keys (into #{}))
        _ (log/debug "Running tasks in compute cluster in datomic: " cc-running-tasks-ids)
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
    (log/info "Initialized tasks on startup:"
              (count all-tasks-ids-in-pods) "tasks in pods and"
              (count running-task-id->task) "running tasks in compute cluster" compute-cluster-name "in datomic. "
              "We need to load an extra"
              (count extra-tasks-id->task) "pods that aren't running in datomic. "
              "For a total expected state size of"
              (count all-task-id->task) "tasks in cook expected state.")
    (doseq [[k v] all-task-id->task]
      (log/debug "Setting cook expected state for " k " ---> " (task-ent->cook-expected-state v)))
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
    (-> pods (merge starting-pods) vals)))

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
        pod-namespace (get-namespace-from-task-metadata namespace-config task-metadata)
        pod-name (:task-id task-metadata)
        ^V1Pod pod (api/task-metadata->pod pod-namespace name task-metadata)
        new-cook-expected-state-dict {:cook-expected-state :cook-expected-state/starting
                                      :launch-pod {:pod pod}}]
    (try
      (controller/update-cook-expected-state compute-cluster pod-name new-cook-expected-state-dict)
      (.stop timer-context)
      (catch Exception e
        (log/error e "In" name "compute cluster, encountered exception launching task"
                   {:pod-name pod-name
                    :pod-namespace pod-namespace
                    :task-metadata task-metadata})))))

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

(defrecord KubernetesComputeCluster [^ApiClient api-client name entity-id match-trigger-chan exit-code-syncer-state
                                     all-pods-atom current-nodes-atom pool->node-name->node
                                     node-name->pod-name->pod cook-expected-state-map cook-starting-pods k8s-actual-state-map
                                     pool->fenzo-atom namespace-config scan-frequency-seconds-config max-pods-per-node
                                     synthetic-pods-config node-blocklist-labels
                                     ^ExecutorService launch-task-executor-service]
  cc/ComputeCluster
  (launch-tasks [this pool-name matches process-task-post-launch-fn]
    (let [task-metadata-seq (mapcat :task-metadata-seq matches)]
      (log/info "In" pool-name "pool, launching tasks for" name "compute cluster"
                {:num-matches (count matches)
                 :num-tasks (count task-metadata-seq)})
      (let [futures
            (doall
              (map (fn [task-metadata]
                     (.submit
                       launch-task-executor-service
                       ^Callable (fn []
                                   (launch-task! this task-metadata)
                                   (process-task-post-launch-fn task-metadata))))
                   task-metadata-seq))]
        (run! deref futures))))

  (kill-task [this task-id]
    ; Note we can't use timer/time! because it wraps the body in a Callable, which rebinds 'this' to another 'this'
    ; causing breakage.
    (let [timer-context (timers/start (metrics/timer "cc-kill-tasks" name))]
      (controller/update-cook-expected-state this task-id {:cook-expected-state :cook-expected-state/killed})
      (.stop timer-context)))

  (decline-offers [this offer-ids]
    (log/debug "Rejecting offer ids" offer-ids))

  (db-id [this]
    entity-id)

  (compute-cluster-name [this]
    name)

  (initialize-cluster [this pool->fenzo running-task-ents]
    ; We may iterate forever trying to bring up kubernetes. However, our caller expects us to eventually return,
    ; so we launch within a future so that our caller can continue initializing other clusters.
    (future
      (try
        (log/info "Initializing Kubernetes compute cluster" name)

        ; We need to reset! the pool->fenzo atom before initializing the
        ; watches, because otherwise, we will start to see and handle events
        ; for pods, but we won't be able to update the corresponding Fenzo
        ; instance (e.g. unassigning a completed task from its host)
        (reset! pool->fenzo-atom pool->fenzo)

        ; Initialize the pod watch path.
        (let [conn cook.datomic/conn
              cook-pod-callback (make-cook-pod-watch-callback this)]
          ; We set cook expected state first because initialize-pod-watch sets (and invokes callbacks on and reacts to) the
          ; expected and the gradually discovered existing pods.
          (reset! cook-expected-state-map (determine-cook-expected-state-on-startup conn api-client name running-task-ents))

          (api/initialize-pod-watch this cook-pod-callback)
          (if scan-frequency-seconds-config
            (regular-scanner this (time/seconds scan-frequency-seconds-config))
            (log/info "State scan disabled because no interval has been set")))

        ; Initialize the node watch path.
        (api/initialize-node-watch this)

        (api/initialize-event-watch api-client name all-pods-atom)
        (catch Throwable e
          (log/error e "Failed to bring up compute cluster" name)
          (throw e))))

    ; We keep leadership indefinitely in kubernetes.
    (async/chan 1))

  (pending-offers [this pool-name]
    (log/info "In" name "compute cluster, looking for offers for pool" pool-name)
    (let [timer (timers/start (metrics/timer "cc-pending-offers-compute" name))
          pods (add-starting-pods this @all-pods-atom)
          nodes @current-nodes-atom
          offers-this-pool (generate-offers this (get-name->node-for-pool @pool->node-name->node pool-name)
                                            (->> (get-pods-in-pool this pool-name)
                                                 (add-starting-pods this)
                                                 (api/pods->node-name->pods)))
          offers-this-pool-for-logging (into #{}
                                             (map #(into {} (select-keys % [:hostname :resources]))
                                                  offers-this-pool))]
      (log/info "In" name "compute cluster, generated" (count offers-this-pool) "offers for pool" pool-name
                {:num-total-nodes-in-compute-cluster (count nodes)
                 :num-total-pods-in-compute-cluster (count pods)
                 :offers-this-pool offers-this-pool-for-logging})
      (timers/stop timer)
      offers-this-pool))

  (restore-offers [this pool-name offers])

  (autoscaling? [_ pool-name]
    (-> synthetic-pods-config :pools (contains? pool-name)))

  (autoscale! [this pool-name jobs adjust-job-resources-for-pool-fn]
    (try
      (assert (cc/autoscaling? this pool-name)
              (str "In " name " compute cluster, request to autoscale despite invalid / missing config"))
      (let [timer-context-autoscale (timers/start (metrics/timer "cc-synthetic-pod-autoscale" name))
            outstanding-synthetic-pods (->> (get-pods-in-pool this pool-name)
                                            (add-starting-pods this)
                                            (filter synthetic-pod->job-uuid))
            num-synthetic-pods (count outstanding-synthetic-pods)
            total-pods (-> @all-pods-atom keys count)
            total-nodes (-> @current-nodes-atom keys count)
            {:keys [image user command max-pods-outstanding max-total-pods max-total-nodes]
             :or {command "exit 0" max-total-pods 32000 max-total-nodes 1000}} synthetic-pods-config]
        (log/info "In" name "compute cluster there are" total-pods "pods and" total-nodes
                  "nodes of a max of" max-total-pods "pods and" max-total-nodes "nodes")
        (log/info "In" name "compute cluster, for pool" pool-name "there are" num-synthetic-pods
                  "outstanding synthetic pod(s), and a max of" max-pods-outstanding "are allowed")
        (let [max-launchable (min (- max-pods-outstanding num-synthetic-pods)
                                  (- max-total-nodes total-nodes)
                                  (- max-total-pods total-pods))]
          (if (not (pos? max-launchable))
            (log/info "In" name "compute cluster, cannot launch more synthetic pods")
            (let [using-pools? (config/default-pool)
                  synthetic-task-pool-name (when using-pools? pool-name)
                  new-jobs (remove (fn [{:keys [job/uuid]}]
                                     (some #(= (str uuid) (synthetic-pod->job-uuid %))
                                           outstanding-synthetic-pods))
                                   jobs)
                  user-from-synthetic-pods-config user
                  task-metadata-seq
                  (->> new-jobs
                       (map (fn [{:keys [job/user job/uuid] :as job}]
                              (let [pool-specific-resources
                                    ((adjust-job-resources-for-pool-fn pool-name) job (tools/job-ent->resources job))]
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
                                 ; like mapping status back to a job instance
                                 :pod-labels {api/cook-synthetic-pod-job-uuid-label (str uuid)}
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
                                                :job {:job/pool {:pool/name synthetic-task-pool-name}}}})))
                       (take max-launchable))
                  num-synthetic-pods-to-launch (count task-metadata-seq)]
              (meters/mark! (metrics/meter "cc-synthetic-pod-submit-rate" name) num-synthetic-pods-to-launch)
              (log/info "In" name "compute cluster, launching" num-synthetic-pods-to-launch
                        "synthetic pod(s) in" synthetic-task-pool-name "pool")
              (let [timer-context-launch-tasks (timers/start (metrics/timer "cc-synthetic-pod-launch-tasks" name))]
                (cc/launch-tasks this
                                 synthetic-task-pool-name
                                 [{:task-metadata-seq task-metadata-seq}]
                                 (fn [_]))
                (.stop timer-context-launch-tasks)))))
        (.stop timer-context-autoscale))
      (catch Throwable e
        (log/error e "In" name "compute cluster, encountered error launching synthetic pod(s) in"
                   pool-name "pool"))))

  (use-cook-executor? [_] false)

  (container-defaults [_]
    ; We don't currently support specifying
    ; container defaults for k8s compute clusters
    {})

  (max-tasks-per-host [_] max-pods-per-node)

  (num-tasks-on-host [this hostname]
    (->> (get @node-name->pod-name->pod hostname {})
         (add-starting-pods this)
         (api/num-pods-on-node hostname)))

  (retrieve-sandbox-url-path
    ;; Constructs a URL to query the sandbox directory of the task.
    ;; Users will need to add the file path & offset to their query.
    ;; Refer to the 'Using the output_url' section in docs/scheduler-rest-api.adoc for further details.
    [_ {:keys [instance/sandbox-url]}]
    sandbox-url))

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
  [scoped-credentials]
  (.refresh scoped-credentials)
  (str "Bearer " (.getTokenValue (.getAccessToken scoped-credentials))))

(def ^ScheduledExecutorService bearer-token-executor (Executors/newSingleThreadScheduledExecutor))

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
    - If config-file is specified, initializes the api file from the file at config-file
    - If base-path is specified, sets the cluster base path
    - If verifying-ssl is specified, sets verifying ssl
    - If use-google-service-account? is true, gets google application default credentials and generates
      a bearer token for authenticating with kubernetes
    - bearer-token-refresh-seconds: interval to refresh the bearer token"
  [^String config-file base-path ^String use-google-service-account? bearer-token-refresh-seconds verifying-ssl ^String ssl-cert-path]
  (let [api-client (if (some? config-file)
                     (Config/fromConfig config-file)
                     (ApiClient.))
        ; Reset to a more sane timeout from the default 10 seconds.
        http-client (-> (OkHttpClient$Builder.) (.readTimeout 120 TimeUnit/SECONDS) .build)]
    (.setHttpClient api-client http-client)
    (when base-path
      (.setBasePath api-client base-path))
    (when (some? verifying-ssl)
      (.setVerifyingSsl api-client verifying-ssl))
    ; Loading ssl-cert-path must be last SSL operation we do in setting up API Client. API bug.
    ; See explanation in comments in https://github.com/kubernetes-client/java/pull/200
    (when (some? ssl-cert-path)
      (.setSslCaCert api-client
                     (FileInputStream. (File. ssl-cert-path))))
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
                (-> synthetic-pods-config :pools set?)
                (-> synthetic-pods-config :pools count pos?))
      (throw (ex-info (str "In " compute-cluster-name " compute cluster, invalid synthetic pods config")
                      synthetic-pods-config)))))

(defn factory-fn
  [{:keys [base-path
           bearer-token-refresh-seconds
           ca-cert-path
           compute-cluster-name
           ^String config-file
           launch-task-num-threads
           max-pods-per-node
           namespace
           node-blocklist-labels
           scan-frequency-seconds
           synthetic-pods
           use-google-service-account?
           verifying-ssl]
    :or {bearer-token-refresh-seconds 300
         launch-task-num-threads 8
         max-pods-per-node 32
         namespace {:kind :static
                    :namespace "cook"}
         node-blocklist-labels (list)
         scan-frequency-seconds 120
         use-google-service-account? true}
    :as compute-cluster-config}
   {:keys [exit-code-syncer-state
           trigger-chans]}]
  (guard-invalid-synthetic-pods-config compute-cluster-name synthetic-pods)
  (when (not (< 0 launch-task-num-threads 64))
    (throw
      (ex-info
        "Please configure :launch-task-num-threads to > 0 and < 64 in your config file."
        compute-cluster-config)))
  (let [conn cook.datomic/conn
        cluster-entity-id (get-or-create-cluster-entity-id conn compute-cluster-name)
        api-client (make-api-client config-file base-path use-google-service-account? bearer-token-refresh-seconds verifying-ssl ca-cert-path)
        launch-task-executor-service (Executors/newFixedThreadPool launch-task-num-threads)
        compute-cluster (->KubernetesComputeCluster api-client 
                                                    compute-cluster-name
                                                    cluster-entity-id
                                                    (:match-trigger-chan trigger-chans)
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
                                                    launch-task-executor-service)]
    (cc/register-compute-cluster! compute-cluster)
    compute-cluster))
