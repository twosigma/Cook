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
            [cook.util :as util]
            [datomic.api :as d]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [plumbing.core :as pc])
  (:import (com.google.auth.oauth2 GoogleCredentials)
           (com.twosigma.cook.kubernetes TokenRefreshingAuthenticator)
           (io.kubernetes.client.openapi ApiClient)
           (io.kubernetes.client.openapi.models V1Node V1Pod)
           (io.kubernetes.client.util ClientBuilder Config KubeConfig)
           (java.nio.charset StandardCharsets)
           (java.io ByteArrayInputStream File FileInputStream InputStreamReader)
           (java.util.concurrent Executors ExecutorService ScheduledExecutorService TimeUnit)
           (java.util Base64 UUID)
           (okhttp3 OkHttpClient$Builder)))

(defn schedulable-node-filter
  "Is a node schedulable?"
  [compute-cluster node-name->node node-name->pods [node-name _]]
  (if-let [^V1Node node (node-name->node node-name)]
    (api/node-schedulable? compute-cluster node (cc/max-tasks-per-host compute-cluster) node-name->pods)
    (do
      (log/error "In" (cc/compute-cluster-name compute-cluster)
                 "compute cluster, unable to get node from node name" node-name)
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
        number-nodes-total (count node-name->node)]

    (log/info "In" compute-cluster-name "compute cluster, generating offers"
              {:first-10-capacity (take 10 node-name->capacity)
               :first-10-consumed (take 10 node-name->consumed)
               :number-nodes-not-schedulable (- number-nodes-total number-nodes-schedulable)
               :number-nodes-schedulable number-nodes-schedulable
               :number-nodes-total number-nodes-total})

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
    (doseq [disk-type disk-types]
      (monitor/set-counter! (metrics/counter (str "capacity-disk-" disk-type) compute-cluster-name)
                            (get disk-type->total-capacity disk-type))
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
  all tasks that are in the running state as well as all pods in kubernetes. We query for a list of
  all running tasks entities."
  [conn api-client compute-cluster-name]
  (let [db (d/db conn)
        [_ pod-name->pod] (api/try-forever-get-all-pods-in-kubernetes api-client compute-cluster-name)
        all-tasks-ids-in-pods (into #{} (keys pod-name->pod))
        _ (log/debug "All tasks in pods (for initializing cook expected state): " all-tasks-ids-in-pods)
        running-tasks-in-cc-ents (map #(d/entity db %) (cc/get-job-instance-ids-for-cluster-name db compute-cluster-name))
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
        ^V1Pod pod (api/task-metadata->pod pod-namespace compute-cluster task-metadata)
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

(defrecord KubernetesComputeCluster [^ApiClient api-client name entity-id exit-code-syncer-state
                                     all-pods-atom current-nodes-atom pool->node-name->node
                                     node-name->pod-name->pod cook-expected-state-map cook-starting-pods k8s-actual-state-map
                                     pool->fenzo-atom namespace-config scan-frequency-seconds-config max-pods-per-node
                                     synthetic-pods-config node-blocklist-labels
                                     ^ExecutorService launch-task-executor-service
                                     cluster-definition state-atom state-locked?-atom dynamic-cluster-config?
                                     compute-cluster-launch-rate-limiter cook-pool-taint-name cook-pool-taint-prefix cook-pool-label-name]
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
    (let [state @state-atom]
      (if (= state :deleted)
        (log/error "In" name "compute cluster, attempting to delete task, with ID" task-id
                   "but the current cluster state is :deleted. Can't perform any client API calls"
                   "when the cluster has been deleted. Will not attempt to kill task.")
        ; Note we can't use timer/time! because it wraps the body in a Callable, which rebinds 'this' to another 'this'
        ; causing breakage.
        (let [timer-context (timers/start (metrics/timer "cc-kill-tasks" name))]
          (controller/update-cook-expected-state this task-id {:cook-expected-state :cook-expected-state/killed})
          (.stop timer-context)))))

  (decline-offers [this offer-ids]
    (log/debug "Rejecting offer ids" offer-ids))

  (db-id [this]
    entity-id)

  (compute-cluster-name [this]
    name)

  (initialize-cluster [this pool->fenzo]
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
          (reset! cook-expected-state-map (determine-cook-expected-state-on-startup conn api-client name))

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
    (let [state @state-atom]
      (if-not (= state :running)
        (log/info "In" name "compute cluster, skipping generating offers for pool" pool-name
                  "because the current state," state ", is not :running.")
        (let [node-name->node (get @pool->node-name->node pool-name)]
          (if-not (or (cc/autoscaling? this pool-name) node-name->node)
            (log/info "In" name "compute cluster, not looking for offers for pool" pool-name
                      ". Skipping pool because it is not a known Kubernetes pool.")
            (do
              (log/info "In" name "compute cluster, looking for offers for pool" pool-name)
              (let [timer (timers/start (metrics/timer "cc-pending-offers-compute" name))
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
                         (map tools/format-resource-map))]
                (log/info "In" name "compute cluster, generated offers for pool"
                          {:first-10-offers-this-pool offers-this-pool-for-logging
                           :number-offers-this-pool (count offers-this-pool)
                           :number-total-nodes-in-compute-cluster (count nodes)
                           :number-total-pods-in-compute-cluster (count pods)
                           :pool-name pool-name})
                (timers/stop timer)
                offers-this-pool)))))))

  (restore-offers [this pool-name offers])

  (autoscaling? [_ pool-name]
    (and (-> synthetic-pods-config :pools (contains? pool-name))
         (= @state-atom :running)))

  (autoscale! [this pool-name jobs adjust-job-resources-for-pool-fn]
    (if-not (cc/autoscaling? this pool-name)
      (log/warn "In" name "compute cluster, ignoring request to autoscale in pool" pool-name
                "because autoscaling? is now false. This should almost never happen. But might benignly"
                "happen because of a race." {:state @state-atom})
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
               :or {command "sleep 300" max-total-pods 32000 max-total-nodes 1000}} synthetic-pods-config]

          (when (>= total-pods max-total-pods)
            (log/warn "In" name "compute cluster, total pods are maxed out"
                      {:max-total-pods max-total-pods
                       :total-pods total-pods}))
          (when (>= total-nodes max-total-nodes)
            (log/warn "In" name "compute cluster, nodes are maxed out"
                      {:max-total-nodes max-total-nodes
                       :total-nodes total-nodes}))
          (when (>= num-synthetic-pods max-pods-outstanding)
            (log/warn "In" name "compute cluster, synthetic pods are maxed out"
                      {:max-synthetic-pods max-pods-outstanding
                       :synthetic-pods num-synthetic-pods}))

          (let [max-launchable (min (- max-pods-outstanding num-synthetic-pods)
                                    (- max-total-nodes total-nodes)
                                    (- max-total-pods total-pods))]
            (if (not (pos? max-launchable))
              (log/warn "In" name "compute cluster, cannot launch more synthetic pods"
                        {:max-synthetic-pods max-pods-outstanding
                         :max-total-nodes max-total-nodes
                         :max-total-pods max-total-pods
                         :synthetic-pods num-synthetic-pods
                         :total-nodes total-nodes
                         :total-pods total-pods})
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
                         (map (fn [{:keys [job/user job/uuid job/environment] :as job}]
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
                                                api/workload-class-label "infrastructure"
                                                api/workload-id-label "synthetic-pod"
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
                                                        :job/environment environment}
                                                  ; Need to pass in resources to task-metadata->pod for gpu count
                                                  :resources pool-specific-resources}}))))
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
                     pool-name "pool")))))

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
    sandbox-url)

  (launch-rate-limiter
    [_] compute-cluster-launch-rate-limiter))

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
        http-client-with-readtimeout (-> api-client
                                         .getHttpClient
                                         .newBuilder
                                         (.readTimeout
                                           read-timeout-seconds
                                           TimeUnit/SECONDS)
                                         .build)
        _ (.setHttpClient api-client http-client-with-readtimeout)]
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
           cook-pool-label-name
           ^String config-file
           dynamic-cluster-config?
           compute-cluster-launch-rate-limits
           kubeconfig-context
           launch-task-num-threads
           max-pods-per-node
           name
           namespace
           node-blocklist-labels
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
         launch-task-num-threads 8
         max-pods-per-node 32
         namespace {:kind :static
                    :namespace "cook"}
         node-blocklist-labels (list)
         read-timeout-seconds 120
         scan-frequency-seconds 120
         state :running
         state-locked? false
         use-google-service-account? true
         cook-pool-taint-name "cook-pool"
         cook-pool-taint-prefix ""
         cook-pool-label-name "cook-pool"
         use-token-refreshing-authenticator? false}
    :as compute-cluster-config}
   {:keys [exit-code-syncer-state]}]
  (guard-invalid-synthetic-pods-config name synthetic-pods)
  (when (not (< 0 launch-task-num-threads 512))
    (throw
      (ex-info
        "Please configure :launch-task-num-threads to > 0 and < 512 in your config."
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
        launch-task-executor-service (Executors/newFixedThreadPool launch-task-num-threads)
        compute-cluster-launch-rate-limiter (cook.rate-limit/create-compute-cluster-launch-rate-limiter name compute-cluster-launch-rate-limits)
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
                                                    launch-task-executor-service
                                                    {:factory-fn 'cook.kubernetes.compute-cluster/factory-fn
                                                     :config compute-cluster-config}
                                                    (atom state)
                                                    (atom state-locked?)
                                                    dynamic-cluster-config?
                                                    compute-cluster-launch-rate-limiter cook-pool-taint-name cook-pool-taint-prefix cook-pool-label-name)]
    (cc/register-compute-cluster! compute-cluster)
    compute-cluster))
