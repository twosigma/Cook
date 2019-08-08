(ns cook.kubernetes.compute-cluster
  (:require [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [cook.compute-cluster :as cc]
            [cook.config :as config]
            [cook.kubernetes.api :as api]
            [cook.kubernetes.controller :as controller]
            [cook.pool]
            [datomic.api :as d]
            [plumbing.core :as pc])
  (:import (com.google.auth.oauth2 GoogleCredentials)
           (io.kubernetes.client ApiClient)
           (io.kubernetes.client.apis CoreV1Api)
           (io.kubernetes.client.util Config)
           (java.io FileInputStream File)
           (java.util UUID)
           (java.util.concurrent Executors ScheduledExecutorService TimeUnit)
           (com.squareup.okhttp.logging HttpLoggingInterceptor HttpLoggingInterceptor$Level)
           (com.squareup.okhttp OkHttpClient)))

(defn generate-offers
  [node-name->node pod-name->pod compute-cluster]
  (let [node-name->capacity (api/get-capacity node-name->node)
        node-name->consumed (api/get-consumption pod-name->pod)
        ; TODO(pschorf): Should also include recently launched jobs
        node-name->available (pc/map-from-keys (fn [node-name]
                                                 (merge-with -
                                                             (node-name->capacity node-name)
                                                             (node-name->consumed node-name)))
                                               (keys node-name->capacity))]
    (log/info "Capacity: " node-name->capacity "Consumption:" node-name->consumed)
    (map (fn [[node-name available]]
           {:id {:value (str (UUID/randomUUID))}
            :framework-id (cc/compute-cluster-name compute-cluster)
            :slave-id {:value node-name}
            :hostname node-name
            :resources [{:name "mem" :type :value-scalar :scalar (max 0.0 (:mem available))}
                        {:name "cpus" :type :value-scalar :scalar (max 0.0 (:cpus available))}
                        {:name "disk" :type :value-scalar :scalar 0.0}]
            :attributes []
            :executor-ids []
            :compute-cluster compute-cluster
            :reject-after-match-attempt true})
         node-name->available)))

(defn make-pod-watch-callback
  [kcc]
  (fn pod-watch-callback
    [prev-pod pod]
    (try
      (if (nil? pod)
        (controller/pod-deleted kcc prev-pod)
        (controller/pod-update kcc pod))
      (catch Exception e
        (log/error e "Error processing status update")))))

(defn task-ents->map-by-task-id
  [task-ents]
  (->> task-ents
       (map (fn [task-ent] [(str (:instance/task-id task-ent)) task-ent]))
       (into {})))

(defn task-ent->expected-state
  [task-ent]
  
  (case (:instance/status task-ent)
    :instance.status/unknown {:expected-state :expected/starting}
    :instance.status/running {:expected-state :expected/running}
    :instance.status/failed {:expected-state :expected/completed}
    :instance.status/success {:expected-state :expected/completed}))

(defn determine-expected-state
  "We need to determine everything we should be tracking when we construct the expected state. We should be tracking all tasks that are in the running state as well
  as all pods in kubernetes. We're given an already existing list of all running tasks entities (via (->> (cook.tools/get-running-task-ents)."
  [conn compute-cluster-name running-tasks-ents current-pods-atom]
  (let [db (d/db conn)
        all-tasks-ids-in-pods (->> @current-pods-atom keys (into #{}))
        _ (log/debug "All tasks in pods: " all-tasks-ids-in-pods)
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
              (count all-task-id->task) "tasks in expected state.")
    (doseq [[k v] all-task-id->task]
      (log/debug "Setting expected state for " k " ---> " (task-ent->expected-state v)))
    (into {}
          (map (fn [[k v]] [k (task-ent->expected-state v)]) all-task-id->task))))

(defrecord KubernetesComputeCluster [^ApiClient api-client name entity-id match-trigger-chan exit-code-syncer-state
                                     current-pods-atom current-nodes-atom expected-state-map existing-state-map
                                     pool->fenzo-atom]
  cc/ComputeCluster
  (launch-tasks [this offers task-metadata-seq]
    (let [api (CoreV1Api. api-client)]
      (doseq [task-metadata task-metadata-seq]
        (controller/update-expected-state
          this
          (:task-id task-metadata)
          {:expected-state :expected/starting :launch-pod (api/task-metadata->pod task-metadata)}))))

  (kill-task [this task-id]
    (controller/update-expected-state this task-id {:expected-state :expected/killed}))

  (decline-offers [this offer-ids]
    (log/debug "Rejecting offer ids" offer-ids))

  (db-id [this]
    entity-id)

  (compute-cluster-name [this]
    name)

  (initialize-cluster [this pool->fenzo running-task-ents]
    ; Initialize the pod watch path.
    (let [conn cook.datomic/conn
          pod-callback (make-pod-watch-callback this)]
      (api/initialize-pod-watch api-client current-pods-atom pod-callback)
      ; We require that initialize-pod-watch sets current-pods-atom before completing.
      (reset! expected-state-map (determine-expected-state conn name running-task-ents current-pods-atom)))

    ; Initialize the node watch path.
    (api/initialize-node-watch api-client current-nodes-atom)
    ; TODO: Need to visit every state to refresh (i.e., do a single pass of state scanner)

    (reset! pool->fenzo-atom pool->fenzo)

    ; TODO(pschorf): Deliver when leadership lost
    (async/chan 1))

  (current-leader? [this]
    true)

  (pending-offers [this pool-name]
    (if (or (= pool-name (config/default-pool)) ; TODO(pschorf): Support pools
            (= pool-name "no-pool"))
      (let [nodes @current-nodes-atom
            pods @current-pods-atom]
        (generate-offers nodes pods this))
      []))

  (restore-offers [this pool-name offers]))

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
  [scoped-credentials]
  (.refresh scoped-credentials)
  (str "Bearer " (.getTokenValue (.getAccessToken scoped-credentials))))

(def ^ScheduledExecutorService bearer-token-executor (Executors/newSingleThreadScheduledExecutor))

(defn make-bearer-token-refresh-task
  [api-client scoped-credentials]
  (reify
    Runnable
    (run [_]
      (try
        (let [bearer-token (get-bearer-token scoped-credentials)]
          (.setApiKey api-client bearer-token))
        (catch Exception ex
          (log/error ex "Error refreshing bearer token"))))))

(defn make-http-client
  []
  (let [logging-interceptor (HttpLoggingInterceptor.)
        _ (.setLevel logging-interceptor HttpLoggingInterceptor$Level/HEADERS)
        client (OkHttpClient.)
        _ (-> client
              .interceptors
              (.add logging-interceptor))]
    client))

(defn make-api-client
  [^String config-file base-path ^String google-credentials bearer-token-refresh-seconds verifying-ssl]
  (let [api-client (if (some? config-file)
                     (Config/fromConfig config-file)
                     (ApiClient.))
        _ (.setHttpClient api-client (make-http-client))]
    (when base-path
      (.setBasePath api-client base-path))
    (when (some? verifying-ssl)
      (.setVerifyingSsl api-client verifying-ssl))
    (when google-credentials
      (with-open [file-stream (FileInputStream. (File. google-credentials))]
        (let [credentials (GoogleCredentials/fromStream file-stream)
              scoped-credentials (.createScoped credentials ["https://www.googleapis.com/auth/cloud-platform"])
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
           bearer-token-refresh-seconds]
    :or {bearer-token-refresh-seconds 300}}
   {:keys [exit-code-syncer-state
           trigger-chans]}]
  (let [conn cook.datomic/conn
        cluster-entity-id (get-or-create-cluster-entity-id conn compute-cluster-name)
        api-client (make-api-client config-file base-path google-credentials bearer-token-refresh-seconds verifying-ssl)
        compute-cluster (->KubernetesComputeCluster api-client compute-cluster-name cluster-entity-id
                                                    (:match-trigger-chan trigger-chans)
                                                    exit-code-syncer-state (atom {}) (atom {})
                                                    (atom {:type :expected-state-map})
                                                    (atom {:type :existing-state-map})
                                                    (atom nil))]
    (cc/register-compute-cluster! compute-cluster)
    compute-cluster))