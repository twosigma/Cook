(ns cook.kubernetes.compute-cluster
  (:require [chime :refer [chime-at chime-ch]]
            [clj-time.core :as time]
            [clj-time.periodic :refer [periodic-seq]]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [cook.compute-cluster :as cc]
            [cook.config :as config]
            [cook.kubernetes.api :as api]
            [cook.kubernetes.controller :as controller]
            [cook.pool]
            [cook.tools :as tools]
            [datomic.api :as d]
            [plumbing.core :as pc])
  (:import (com.google.auth.oauth2 GoogleCredentials)
           (io.kubernetes.client ApiClient)
           (io.kubernetes.client.models V1Node)
           (io.kubernetes.client.util Config)
           (java.io FileInputStream File)
           (java.util UUID)
           (java.util.concurrent Executors ScheduledExecutorService TimeUnit)))

(defn schedulable-node-filter
  "Is a node schedulable?"
  [node-name->node [node-name _] compute-cluster pods]
  (if-let [^V1Node node (node-name->node node-name)]
    (api/node-schedulable? node (cc/max-tasks-per-host compute-cluster) pods)
    (do
      (log/error "In" (cc/compute-cluster-name compute-cluster)
                 "compute cluster, unable to get node from node name" node-name)
      false)))


(defn generate-offers
  "Given a compute cluster and maps with node capacity and existing pods, return a map from pool to offers."
  [compute-cluster node-name->node pods]
  (let [node-name->capacity (api/get-capacity node-name->node)
        node-name->consumed (api/get-consumption pods)
        node-name->available (pc/map-from-keys (fn [node-name]
                                                 (merge-with -
                                                             (node-name->capacity node-name)
                                                             (node-name->consumed node-name)))
                                               (keys node-name->capacity))
        compute-cluster-name (cc/compute-cluster-name compute-cluster)]
    (log/info "In" compute-cluster-name "compute cluster, capacity:" node-name->capacity)
    (log/info "In" compute-cluster-name "compute cluster, consumption:" node-name->consumed)
    (log/info "In" compute-cluster-name "compute cluster, filtering out"
              (->> node-name->available
                   (remove #(schedulable-node-filter node-name->node % compute-cluster pods))
                   count)
              "nodes as not schedulable")
    (->> node-name->available
         (filter #(schedulable-node-filter node-name->node % compute-cluster pods))
         (map (fn [[node-name available]]
                {:id {:value (str (UUID/randomUUID))}
                 :framework-id compute-cluster-name
                 :slave-id {:value node-name}
                 :hostname node-name
                 :resources [{:name "mem" :type :value-scalar :scalar (max 0.0 (:mem available))}
                             {:name "cpus" :type :value-scalar :scalar (max 0.0 (:cpus available))}
                             {:name "disk" :type :value-scalar :scalar 0.0}]
                 :attributes []
                 :executor-ids []
                 :compute-cluster compute-cluster
                 :reject-after-match-attempt true}))
         (group-by (fn [offer] (-> offer :hostname node-name->node api/get-node-pool))))))

(defn taskids-to-scan
  "Determine all taskids to scan by unioning task id's from cook expected state and existing taskid maps. "
  [{:keys [cook-expected-state-map k8s-actual-state-map] :as kcc}]
  (->>
    (set/union (keys @cook-expected-state-map) (keys @k8s-actual-state-map))
    (into #{})))

(defn scan-tasks
  "Scan all taskids. Note: May block or be slow due to rate limits."
  [{:keys [name] :as kcc}]
  (log/info "In compute cluster" name ", starting taskid scan")
  ; TODO Add in rate limits; only visit non-running/running task so fast.
  ; TODO Add in maximum-visit frequency. Only visit a task once every XX seconds.
  (let [taskids (taskids-to-scan kcc)]
    (log/info "In compute cluster" name ", doing taskid scan. Visiting" (count taskids) "taskids")
    (doseq [^String taskid taskids]
      (log/info "In compute cluster" name ", doing scan of " taskid)
      (controller/scan-process kcc taskid))))

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
        [_ pod-name->pod] (api/get-all-pods-in-kubernetes api-client)
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
    (log/info "Initialized tasks on startup: "
              (count all-tasks-ids-in-pods) " tasks in pods and "
              (count running-task-id->task) " running tasks in this compute cluster in datomic. "
              "We need to load an extra "
              (count extra-tasks-id->task) " pods that aren't running in datomic. "
              "For a total expected state size of "
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

(defn all-pods
  [compute-cluster pods]
  (let [starting-pods (controller/starting-namespaced-pod-name->pod compute-cluster)]
    (-> pods (merge starting-pods) vals)))

(defrecord KubernetesComputeCluster [^ApiClient api-client name entity-id match-trigger-chan exit-code-syncer-state
                                     all-pods-atom current-nodes-atom cook-expected-state-map k8s-actual-state-map
                                     pool->fenzo-atom namespace-config scan-frequency-seconds-config max-pods-per-node
                                     last-autoscale-atom synthetic-tasks]
  cc/ComputeCluster
  (launch-tasks [this offers task-metadata-seq]
    (doseq [task-metadata task-metadata-seq]
      (let [pod-namespace (get-namespace-from-task-metadata namespace-config task-metadata)]
        (controller/update-cook-expected-state
          this
          (:task-id task-metadata)
          {:cook-expected-state :cook-expected-state/starting :launch-pod {:pod (api/task-metadata->pod pod-namespace name task-metadata)}}))))

  (kill-task [this task-id]
    (controller/update-cook-expected-state this task-id {:cook-expected-state :cook-expected-state/killed}))

  (decline-offers [this offer-ids]
    (log/debug "Rejecting offer ids" offer-ids))

  (db-id [this]
    entity-id)

  (compute-cluster-name [this]
    name)

  (initialize-cluster [this pool->fenzo running-task-ents]
    ; Initialize the pod watch path.
    (log/info "Initializing Kubernetes compute cluster" name)
    (let [conn cook.datomic/conn
          cook-pod-callback (make-cook-pod-watch-callback this)]
      ; We set cook expected state first because initialize-pod-watch sets (and invokes callbacks on and reacts to) the
      ; expected and the gradually discovered existing pods.
      (reset! cook-expected-state-map (determine-cook-expected-state-on-startup conn api-client name running-task-ents))

      (api/initialize-pod-watch api-client name all-pods-atom cook-pod-callback)
      (if scan-frequency-seconds-config
        (regular-scanner this (time/seconds scan-frequency-seconds-config))
        (log/info "State scan disabled because no interval has been set")))

    ; Initialize the node watch path.
    (api/initialize-node-watch api-client current-nodes-atom)

    (reset! pool->fenzo-atom pool->fenzo)

    ; We keep leadership indefinitely in kubernetes.
    (async/chan 1))

  (pending-offers [this pool-name]
    (let [nodes @current-nodes-atom
          pods (all-pods this @all-pods-atom)
          offers-all-pools (generate-offers this nodes pods)
          ; TODO: We are generating offers for every pool here, and filtering out only offers for this one pool.
          ; TODO: We should be smarter here and generate once, then reuse for each pool, instead of generating for each pool each time and only keeping one
          offers-this-pool (get offers-all-pools pool-name)]
      (log/info "Generated" (count offers-this-pool) "offers for pool" pool-name "in compute cluster" name
                     (into [] (map #(into {} (select-keys % [:hostname :resources])) offers-this-pool)))
      offers-this-pool))

  (restore-offers [this pool-name offers])

  (autoscaling? [_]
    (and
      (some-> synthetic-tasks :image count pos?)
      (some-> synthetic-tasks :user count pos?)
      (some-> synthetic-tasks :max-tasks pos?)))

  (autoscale! [this pool-name task-requests]
    (try
      (assert (cc/autoscaling? this)
              (str "Request to autoscale despite invalid / missing config"))
      (let [using-pools? (config/default-pool)
            synthetic-task-pool-name (if using-pools? pool-name nil)
            {:keys [image user command max-tasks] :or {command "exit 0"}} synthetic-tasks
            task-metadata-seq (->>
                                task-requests
                                (map (fn [{:keys [job]}]
                                       {:task-id (str (UUID/randomUUID))
                                        :command {:user user :value command}
                                        :container {:docker {:image image}}
                                        :task-request {:resources (tools/job-ent->resources job)
                                                       :job {:job/pool {:pool/name synthetic-task-pool-name}}}
                                        ; We need to label the synthetic tasks so that we
                                        ; can opt them out of some of the normal plumbing,
                                        ; like mapping status back to a job instance
                                        :labels {controller/cook-synthetic-task-label "true"}}))
                                (take max-tasks))]
        (log/info "In" name "compute cluster, launching" (count task-metadata-seq) "synthetic task(s) for"
                  (count task-requests) "un-matched task(s) in" synthetic-task-pool-name "pool")
        (reset! last-autoscale-atom (time/now))
        (cc/launch-tasks this
                         nil ; offers (not used by KubernetesComputeCluster)
                         task-metadata-seq))
      (catch Throwable e
        (log/error e "In" name "compute cluster, encountered error launching synthetic tasks for"
                   (count task-requests) "un-matched task(s) in" pool-name "pool"))))

  (last-autoscale-time [_]
    @last-autoscale-atom)

  (use-cook-executor? [_] false)

  (container-defaults [_]
    ; We don't currently support specifying
    ; container defaults for k8s compute clusters
    {})

  (max-tasks-per-host [_] max-pods-per-node)

  (num-tasks-on-host [this hostname]
    (->> @all-pods-atom
         (all-pods this)
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

(defn make-api-client
  "Builds an ApiClient from the given configuration parameters:
    - If config-file is specified, initializes the api file from the file at config-file
    - If base-path is specified, sets the cluster base path
    - If verifying-ssl is specified, sets verifying ssl
    - If google-credentials is specified, loads the credentials from the file at google-credentials and generates
      a bearer token for authenticating with kubernetes
    - bearer-token-refresh-seconds: interval to refresh the bearer token"
  [^String config-file base-path ^String google-credentials bearer-token-refresh-seconds verifying-ssl]
  (let [api-client (if (some? config-file)
                     (Config/fromConfig config-file)
                     (ApiClient.))]
    ; Reset to a more sane timeout from the default 10 seconds.
    (some-> api-client .getHttpClient (.setReadTimeout 120 TimeUnit/SECONDS))
    (when base-path
      (.setBasePath api-client base-path))
    (when (some? verifying-ssl)
      (.setVerifyingSsl api-client verifying-ssl))
    (when google-credentials
      (with-open [file-stream (FileInputStream. (File. google-credentials))]
        (let [credentials (GoogleCredentials/fromStream file-stream)
              scoped-credentials (.createScoped credentials ["https://www.googleapis.com/auth/cloud-platform"
                                                             "https://www.googleapis.com/auth/userinfo.email"])
              bearer-token (get-bearer-token scoped-credentials)]
          (.scheduleAtFixedRate bearer-token-executor
                                (make-bearer-token-refresh-task api-client scoped-credentials)
                                bearer-token-refresh-seconds
                                bearer-token-refresh-seconds
                                TimeUnit/SECONDS)
          (.setApiKey api-client bearer-token))))

    api-client))

(defn factory-fn
  [{:keys [compute-cluster-name
           ^String config-file
           base-path
           google-credentials
           verifying-ssl
           bearer-token-refresh-seconds
           namespace
           scan-frequency-seconds
           max-pods-per-node
           synthetic-tasks]
    :or {bearer-token-refresh-seconds 300
         namespace {:kind :static
                    :namespace "cook"}
         scan-frequency-seconds 120
         max-pods-per-node 32}}
   {:keys [exit-code-syncer-state
           trigger-chans]}]
  (let [conn cook.datomic/conn
        cluster-entity-id (get-or-create-cluster-entity-id conn compute-cluster-name)
        api-client (make-api-client config-file base-path google-credentials bearer-token-refresh-seconds verifying-ssl)
        compute-cluster (->KubernetesComputeCluster api-client compute-cluster-name cluster-entity-id
                                                    (:match-trigger-chan trigger-chans)
                                                    exit-code-syncer-state (atom {}) (atom {})
                                                    (atom {})
                                                    (atom {})
                                                    (atom nil)
                                                    namespace
                                                    scan-frequency-seconds
                                                    max-pods-per-node
                                                    (atom (time/epoch))
                                                    synthetic-tasks)]
    (cc/register-compute-cluster! compute-cluster)
    compute-cluster))
