;;
;; Copyright (c) Two Sigma Open Source, LLC
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;  http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(ns cook.mesos.api
  (:require [datomic.api :as d :refer (q)]
            [metatransaction.core :refer (db)]
            [schema.core :as s]
            [cook.mesos]
            [compojure.core :refer (routes ANY)]
            [compojure.api.sweet :as c-api]
            [compojure.api.middleware :as c-mw]
            [plumbing.core :refer [map-keys mapply]]
            [camel-snake-kebab.core :refer [->snake_case]]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [clojure.core.cache :as cache]
            [clojure.walk :as walk]
            [clj-http.client :as http]
            [ring.middleware.json]
            [clojure.data.json :as json]
            [liberator.core :as liberator]
            [liberator.util :refer [combine]]
            [cheshire.core :as cheshire]
            [cook.mesos.util :as util]
            [clj-time.core :as t]
            [cook.mesos.share :as share]
            [cook.mesos.quota :as quota]
            [plumbing.core :refer (map-vals)]
            [cook.mesos.reason :as reason]
            [clojure.walk :refer (keywordize-keys)])
  (:import java.util.UUID))

(def PosNum
  (s/both s/Num (s/pred pos? 'pos?)))

(def PosInt
  (s/both s/Int (s/pred pos? 'pos?)))

(def PosDouble
  (s/both double (s/pred pos? 'pos?)))

(defn render-error
  [ctx]
  {:error (::error ctx)})

(def cook-liberator-attrs
  {:available-media-types ["application/json"]
   ;; necessary to play well with as-response below
   :handle-no-content (fn [ctx] "No content.")
   ;; Don't serialize the response; leave that to compojure-api
   :as-response (fn [data ctx] (combine {:body data} ctx))
   :handle-forbidden render-error
   :handle-malformed render-error
   :handle-not-found render-error})

(defn base-cook-handler
  [resource-attrs]
  (mapply liberator/resource (merge cook-liberator-attrs resource-attrs)))


;;
;; /rawscheduler
;;

(def PortMapping
  "Schema for Docker Portmapping"
  {:host-port (s/both s/Int (s/pred #(<= 0 % 65536) 'between-0-and-65536))
   :container-port (s/both s/Int (s/pred #(<= 0 % 65536) 'between-0-and-65536))
   (s/optional-key :protocol) s/Str})

(def DockerInfo
  "Schema for a DockerInfo"
  {:image s/Str
   (s/optional-key :network) s/Str
   (s/optional-key :force-pull-image) s/Bool
   (s/optional-key :parameters) [{:key s/Str :value s/Str}]
   (s/optional-key :port-mapping) [PortMapping]})

(def Volume
  "Schema for a Volume"
  {(s/optional-key :container-path) s/Str
   :host-path s/Str
   (s/optional-key :mode) s/Str})

(def Container
  "Schema for a Mesos Container"
  {:type s/Str
   (s/optional-key :docker) DockerInfo
   (s/optional-key :volumes) [Volume]})

(def Uri
  "Schema for a Mesos fetch URI, which has many options"
  {:value s/Str
   (s/optional-key :executable?) s/Bool
   (s/optional-key :extract?) s/Bool
   (s/optional-key :cache?) s/Bool})

(def UriRequest
  "Schema for a Mesos fetch URI as it would be included in JSON requests
  or responses.  Avoids question-marks in the key names."
  {:value s/Str
   (s/optional-key :executable) s/Bool
   (s/optional-key :extract) s/Bool
   (s/optional-key :cache) s/Bool})

(def NonEmptyString
  (s/both s/Str (s/pred #(not (zero? (count %))) 'empty-string)))

(def Job
  "A schema for a job"
  {:uuid s/Uuid
   :command s/Str
   ;; Make sure the job name is a valid string which can only contain '.', '_', '-' or any word characters and has
   ;; length at most 128.
   :name (s/both s/Str (s/pred #(re-matches #"[\.a-zA-Z0-9_-]{0,128}" %) 'under-128-characters-and-alphanum?))
   :priority (s/both s/Int (s/pred #(<= 0 % 100) 'between-0-and-100))
   :max-retries (s/both s/Int (s/pred pos? 'pos?))
   :max-runtime (s/both s/Int (s/pred pos? 'pos?))
   (s/optional-key :uris) [Uri]
   (s/optional-key :ports) (s/pred #(not (neg? %)) 'nonnegative?)
   (s/optional-key :env) {NonEmptyString s/Str}
   (s/optional-key :labels) {NonEmptyString s/Str}
   (s/optional-key :container) Container
   :cpus PosDouble
   :mem PosDouble
   (s/optional-key :gpus) (s/both s/Int (s/pred pos? 'pos?))
   ;; Make sure the user name is valid. It must begin with a lower case character, end with
   ;; a lower case character or a digit, and has length between 2 to (62 + 2).
   :user (s/both s/Str (s/pred #(re-matches #"\A[a-z][a-z0-9_-]{0,62}[a-z0-9]\z" %) 'lowercase-alphanum?))})

(def JobRequest
  "Schema for the part of a request that launches a single job."
  (-> (map-keys #(if (keyword? %) (->snake_case %) %) Job)
      (dissoc :user)
      (merge {(s/optional-key :env) {s/Keyword s/Str}
              (s/optional-key :labels) {s/Keyword s/Str}
              (s/optional-key :uris) [UriRequest]
              :cpus PosNum
              :mem PosNum})))

(def JobOrInstanceIds
  "Schema for any number of job and/or instance uuids"
  {(s/optional-key :job) [s/Uuid]
   (s/optional-key :instance) [s/Uuid]})

(def Instance
  "Schema for a description of a single job instance."
  {:status s/Str
   :task_id s/Uuid
   :executor_id s/Uuid
   :slave_id s/Str
   :hostname s/Str
   :preempted s/Bool
   :backfilled s/Bool
   :ports [s/Int]
   (s/optional-key :start_time) s/Int
   (s/optional-key :end_time) s/Int
   (s/optional-key :reason_code) s/Int
   (s/optional-key :reason_string) s/Str})

(def JobResponse
  "Schema for a description of a job (as returned by the API).
  The structure is similar to JobRequest, but has some differences.
  For example, it can include descriptions of instances for the job."
  (merge (dissoc JobRequest :user)
         {:framework_id (s/maybe s/Str)
          :status s/Str
          :submit-time s/Str
          (s/optional-key :gpus) s/Int
          (s/optional-key :instances) [Instance]}))

(defn- mk-container-params
  "Helper for build-container.  Transforms parameters into the datomic schema."
  [cid params]
  (when-not (empty? params)
    {:docker/parameters (mapv (fn [{:keys [key value]}]
                                {:docker.param/key key
                                 :docker.param/value value})
                              params)}))

(defn- mkvolumes
  "Helper for build-container.  Transforms volumes into the datomic schema."
  [cid vols]
  (when-not (empty? vols)
    (let [vol-maps (mapv (fn [{:keys [container-path host-path mode]}]
                           (merge (when container-path
                                    {:container.volume/container-path container-path})
                                  (when host-path
                                    {:container.volume/host-path host-path})
                                  (when mode
                                    {:container.volume/mode (clojure.string/upper-case mode)})))
                         vols)]
      {:container/volumes vol-maps})))

(defn- mk-docker-ports
  "Helper for build-container.  Transforms port-mappings into the datomic schema."
  [cid ports]
  (when-not (empty? ports)
    (let [port-maps (mapv (fn [{:keys [container-port host-port protocol]}]
                            (merge (when container-port
                                     {:docker.portmap/container-port container-port})
                                   (when host-port
                                     {:docker.portmap/host-port host-port})
                                   (when protocol
                                     {:docker.portmap/protocol (clojure.string/upper-case protocol)})))
                          ports)]
      {:docker/port-mapping port-maps})))

(defn- build-container
  "Helper for submit-jobs, deal with container structure."
  [id container]
  (let [container-id (d/tempid :db.part/user)
        docker-id (d/tempid :db.part/user)
        ctype (:type container)
        volumes (or (:volumes container) [])]
    (if (= (clojure.string/lower-case ctype) "docker")
      (let [docker (:docker container)
            params (or (:parameters docker) [])
            port-mappings (or (:port-mapping docker) [])]
        [[:db/add id :job/container container-id]
         (merge {:db/id container-id
                 :container/type "DOCKER"}
                (mkvolumes container-id volumes))
         [:db/add container-id :container/docker docker-id]
         (merge {:db/id docker-id
                 :docker/image (:image docker)
                 :docker/force-pull-image (:force-pull-image docker false)
                 :docker/network (:network docker)}
                (mk-container-params docker-id params)
                (mk-docker-ports docker-id port-mappings))])
      {})))

(s/defn submit-jobs
  [conn jobs :- [Job]]
  (doseq [{:keys [uuid command max-retries max-runtime priority cpus mem gpus user name ports uris env labels container]} jobs
          :let [id (d/tempid :db.part/user)
                ports (when (and ports (not (zero? ports)))
                        [[:db/add id :job/ports ports]])
                uris (mapcat (fn [{:keys [value executable? cache? extract?]}]
                               (let [uri-id (d/tempid :db.part/user)
                                     optional-params {:resource.uri/executable? executable?
                                                      :resource.uri/extract? extract?
                                                      :resource.uri/cache? cache?}]
                                 [[:db/add id :job/resource uri-id]
                                  (reduce-kv
                                    ;; This only adds the optional params to the DB if they were explicitly set
                                    (fn [txn-map k v]
                                      (if-not (nil? v)
                                        (assoc txn-map k v)
                                        txn-map))
                                    {:db/id uri-id
                                     :resource/type :resource.type/uri
                                     :resource.uri/value value}
                                    optional-params)]))
                             uris)
                env (mapcat (fn [[k v]]
                              (let [env-var-id (d/tempid :db.part/user)]
                                [[:db/add id :job/environment env-var-id]
                                 {:db/id env-var-id
                                  :environment/name k
                                  :environment/value v}]))
                            env)
                labels (mapcat (fn [[k v]]
                              (let [label-var-id (d/tempid :db.part/user)]
                                [[:db/add id :job/label label-var-id]
                                 {:db/id label-var-id
                                  :label/key k
                                  :label/value v}]))
                            labels)
                container (if (nil? container) [] (build-container id container))
                ;; These are optionally set datoms w/ default values
                maybe-datoms (concat
                               (when (and priority (not= util/default-job-priority priority))
                                 [[:db/add id :job/priority priority]])
                               (when (and max-runtime (not= Long/MAX_VALUE max-runtime))
                                 [[:db/add id :job/max-runtime max-runtime]])
                               (when (and gpus (not (zero? gpus)))
                                 (let [gpus-id (d/tempid :db.part/user)]
                                   [[:db/add id :job/resource gpus-id]
                                    {:db/id gpus-id
                                     :resource/type :resource.type/gpus
                                     :resource/amount (double gpus)}])))
                commit-latch-id (d/tempid :db.part/user)
                commit-latch {:db/id commit-latch-id
                              :commit-latch/uuid (java.util.UUID/randomUUID)
                              :commit-latch/committed? true}
                txn {:db/id id
                     :job/commit-latch commit-latch-id
                     :job/uuid uuid
                     :job/submit-time (java.util.Date.)
                     :job/name (or name "cookjob") ; set the default job name if not provided.
                     :job/command command
                     :job/custom-executor false
                     :job/user user
                     :job/max-retries max-retries
                     :job/state :job.state/waiting
                     :job/resource [{:resource/type :resource.type/cpus
                                     :resource/amount cpus}
                                    {:resource/type :resource.type/mem
                                     :resource/amount mem}]}]]
    
    ;; TODO batch these transactions to improve performance
    @(d/transact conn (-> ports
                          (into uris)
                          (into env)
                          (into labels)
                          (into container)
                          (into maybe-datoms)
                          (conj txn)
                          (conj commit-latch))))
  "ok")

(defn used-uuid?
  "Returns true iff the given uuid is used in datomic"
  [db uuid]
  (boolean (seq (q '[:find ?j
                     :in $ ?uuid
                     :where
                     [?j :job/uuid ?uuid]]
                   db uuid))))

(defn ensure-uuid-unused
  "Throws if the given uuid is used in datomic"
  [db uuid]
  (if (used-uuid? db uuid)
    (throw (ex-info (str "UUID " uuid " already used") {:uuid uuid}))
    true))

(defn validate-and-munge-job
  "Takes the user and the parsed json from the job and returns proper
   Job objects, or else throws an exception"
  [db user task-constraints gpu-enabled? {:keys [cpus mem gpus uuid command priority max_retries max_runtime name uris ports env labels container] :as job}]
  (let [munged (merge
                (dissoc job :max_retries :max_runtime)
                 {:user user
                  :name (or name "cookjob") ; Add default job name if user does not provide a name.
                  :priority (or priority util/default-job-priority)
                  :max-retries max_retries
                  :max-runtime (or max_runtime Long/MAX_VALUE)
                  :ports (or ports 0)
                  :cpus (double cpus)
                  :mem (double mem)}
                 (when gpus
                   {:gpus (int gpus)})
                 (when env
                   {:env (walk/stringify-keys env)})
                 (when uris
                   {:uris (map (fn [{:keys [value executable cache extract]}]
                                 (merge {:value value}
                                        (when executable {:executable? executable})
                                        (when cache {:cache? cache})
                                        (when extract {:extract? extract})))
                               uris)})
                 (when labels
                   {:labels (walk/stringify-keys labels)}))]
    (when (and (:gpus munged) (not gpu-enabled?))
      (throw (ex-info (str "GPU support is not enabled") {:gpus gpus})))
    (s/validate Job munged)
    (when (> cpus (:cpus task-constraints))
      (throw (ex-info (str "Requested " cpus " cpus, but only allowed to use " (:cpus task-constraints))
                      {:constraints task-constraints
                       :job job})))
    (when (> mem (* 1024 (:memory-gb task-constraints)))
      (throw (ex-info (str "Requested " mem "mb memory, but only allowed to use "
                           (* 1024 (:memory-gb task-constraints)))
                      {:constraints task-constraints
                       :job job})))
    (doseq [{:keys [executable? extract?] :as uri} (:uris munged)
            :when (and (not (nil? executable?)) (not (nil? extract?)))]
      (throw (ex-info "Uri cannot set executable and extract" uri)))
    (ensure-uuid-unused db (:uuid munged))
    munged))

(defn get-executor-states-impl
  "Builds an indexed version of all executor states on the specified slave. Has no cache; takes 100-500ms
   to run."
  [framework-id hostname]
  (let [timeout-millis (* 5 1000)
        ;; Throw SocketTimeoutException or ConnectionTimeoutException when timeout
        slave-state (:body (http/get (str "http://" hostname ":5051/state.json")
                                     {:socket-timeout timeout-millis
                                      :conn-timeout timeout-millis
                                      :as :json-string-keys
                                      :spnego-auth true}))
        framework-executors (for [framework (reduce into 
                                                    [] 
                                                    [(get slave-state "frameworks")
                                                     (get slave-state "completed_frameworks")])
                                  :when (= framework-id (get framework "id"))
                                  e (reduce into 
                                            []
                                            [(get framework "executors")
                                             (get framework "completed_executors")])]
                              e)]
    (reduce (fn [m {:strs [id] :as executor-state}]
              (assoc m id executor-state))
            {}
            framework-executors)))

(let [cache (-> {}
                (cache/fifo-cache-factory :threshold 10000)
                (cache/ttl-cache-factory :ttl (* 1000 60))
                atom)]
  (defn get-executor-states
    "Builds an indexed version of all executor states on the specified slave. Cached"
    [framework-id hostname]
    (let [run (delay (try (get-executor-states-impl framework-id hostname)
                          (catch Exception e
                            (log/debug e "Failed to get executor state, purging from cache...")
                            (swap! cache cache/evict hostname)
                            nil)))
          cs (swap! cache (fn [c]
                            (if (cache/has? c hostname)
                              (cache/hit c hostname)
                              (cache/miss c hostname run))))
          val (cache/lookup cs hostname)]
      (if val @val @run))))

(defn executor-state->url-path
  "Takes the executor state from the slave json and constructs a URL to query it. Hardcodes fun
   stuff like the port we run the slave on. Users will need to add the file path & offset to their query"
  [host executor-state]
  (str "http://" host ":5051"
       "/files/read.json?path="
       (java.net.URLEncoder/encode (get executor-state "directory") "UTF-8")))

(defn fetch-job-map
  [db fid job-uuid]
  (let [job (d/entity db [:job/uuid job-uuid])
        resources (util/job-ent->resources job)]
    (merge
      {:command (:job/command job)
       :uuid (str (:job/uuid job))
       :name (:job/name job "cookjob")
       :priority (:job/priority job util/default-job-priority)
       :submit-time (str (:job/submit-time job))
       :cpus (:cpus resources)
       :mem (:mem resources)
       :gpus (int (:gpus resources 0))
       :max_retries  (:job/max-retries job) ; Consistent with input
       :max_runtime (:job/max-runtime job Long/MAX_VALUE) ; Consistent with input
       :framework_id fid
       :status (name (:job/state job))
       :uris (:uris resources)
       :env (util/job-ent->env job)
       :labels (util/job-ent->label job)
       :ports (:job/ports job 0)
       :instances
       (map (fn [instance]
              (let [hostname (:instance/hostname instance)
                    executor-states (get-executor-states fid hostname)
                    url-path (try
                               (executor-state->url-path hostname (get executor-states (:instance/executor-id instance)))
                               (catch Exception e
                                 nil))
                    start (:instance/start-time instance)
                    end (:instance/end-time instance)
                    reason (reason/instance-entity->reason-entity db instance)
                    base {:task_id (:instance/task-id instance)
                          :hostname hostname
                          :ports (:instance/ports instance)
                          :backfilled (:instance/backfilled? instance false)
                          :preempted (:instance/preempted? instance false)
                          :slave_id (:instance/slave-id instance)
                          :executor_id (:instance/executor-id instance)
                          :status (name (:instance/status instance))}
                    base (if url-path
                           (assoc base :output_url url-path)
                           base)
                    base (if start
                           (assoc base :start_time (.getTime start))
                           base)
                    base (if end
                           (assoc base :end_time (.getTime end))
                           base)
                    base (if reason
                           (assoc base
                                  :reason_code (:reason/code reason)
                                  :reason_string (:reason/string reason))
                           base)]
                base))
            (:job/instance job))})))

(defn instance-uuid->job-uuid
  "Queries for the job uuid from an instance uuid.
   Returns nil if the instance uuid doesn't correspond
   a job"
  [db instance-uuid]
  (->> (d/entity db [:instance/task-id (str instance-uuid)])
       :job/_instance
       :job/uuid))


(defn retrieve-jobs
  [conn instances-too? ctx]
  (let [jobs (get-in ctx [:request :query-params :job])
        instances (if instances-too? (get-in ctx [:request :query-params :instance]) [])]
    (let [instance-uuid->job-uuid #(instance-uuid->job-uuid (d/db conn) %)
          instance-jobs (mapv instance-uuid->job-uuid instances)
          used? (partial used-uuid? (db conn))]
      (cond
        (and (every? used? jobs)
             (every? (complement nil?) instance-jobs))
        [true {::jobs (into jobs instance-jobs)}]
        (some nil? instance-jobs)
        [false {::error (str "UUID "
                             (str/join
                              \space
                              (filter (comp nil? instance-uuid->job-uuid)
                                      instances))
                             " didn't correspond to an instance")}]
        :else
        [false {::error (str "UUID "
                             (str/join
                              \space
                              (remove used? jobs))
                             " didn't correspond to a job")}]))))

(defn check-job-params-present
  [ctx]
  (let [jobs (get-in ctx [:request :query-params :job])
        instances (get-in ctx [:request :query-params :instance])]
    (if (or (seq jobs) (seq instances))
      false
      [true {::error "must supply at least one job or instance query param"}])))

(defn job-request-allowed?
  [conn is-authorized-fn ctx]
  (let [uuids (::jobs ctx)
        user (get-in ctx [:request :authorization/user])
        job-user (fn [uuid]
                   (:job/user (d/entity (db conn) [:job/uuid uuid])))
        authorized? (fn [uuid]
                      (is-authorized-fn user (get-in ctx [:request :request-method]) {:owner (job-user uuid) :item :job}))]
    (if (every? authorized? uuids)
      true
      [false {::error (str "You are not authorized to view access the following jobs "
                           (str/join \space (remove authorized? uuids)))}])))

(defn render-jobs-for-response
  [conn fid ctx]
  (mapv (partial fetch-job-map (db conn) fid) (::jobs ctx)))


;;; On GET; use repeated job argument
(defn read-jobs-handler
  [conn fid task-constraints gpu-enabled? is-authorized-fn]
  (base-cook-handler
   {:allowed-methods [:get]
    :malformed? check-job-params-present
    :allowed? (partial job-request-allowed? conn is-authorized-fn)
    :exists? (partial retrieve-jobs conn true)
    :handle-ok (partial render-jobs-for-response conn fid)}))


;;; On DELETE; use repeated job argument
(defn destroy-jobs-handler
  [conn fid task-constraints gpu-enabled? is-authorized-fn]
  (base-cook-handler
   {:allowed-methods [:delete]
    :malformed? check-job-params-present
    :allowed? (partial job-request-allowed? conn is-authorized-fn)
    :exists? (partial retrieve-jobs conn false)
    :delete! (fn [ctx]
               (cook.mesos/kill-job conn (::jobs ctx)))
    :handle-ok (partial render-jobs-for-response conn fid)}))


;;; On POST; JSON blob that looks like:
;;; {"jobs": [{"command": "echo hello world",
;;;            "uuid": "123898485298459823985",
;;;            "max_retries": 3
;;;            "cpus": 1.5,
;;;            "mem": 1000}]}
;;;
(defn create-jobs-handler
  [conn fid task-constraints gpu-enabled? is-authorized-fn]
  (base-cook-handler
   {:allowed-methods [:post]
    :malformed? (fn [ctx]
                  (let [params (get-in ctx [:request :body-params])
                        user (get-in ctx [:request :authorization/user])]
                    (try
                      [false {::jobs (mapv #(validate-and-munge-job
                                             (db conn)
                                             user
                                             task-constraints
                                             gpu-enabled?
                                             %)
                                           (get params :jobs))}]
                      (catch Exception e
                        (log/warn e "Malformed raw api request")
                        [true {::error (str e)}]))))
    :processable? (fn [ctx]
                    (try
                      (log/info "Submitting jobs through raw api:" (::jobs ctx))
                      (submit-jobs conn (::jobs ctx))
                      true
                      (catch Exception e
                        (log/error e "Error submitting jobs through raw api")
                        [false (str e)])))
    :post! (fn [ctx]
             ;; We did the actual logic in processable?, so there's nothing left to do
             {::results (str/join \space (cons "submitted jobs" (map (comp str :uuid) (::jobs ctx))))})

    :handle-created (fn [ctx] (::results ctx))}))


;;
;; /queue
;;

(defn waiting-jobs
  [mesos-pending-jobs-fn is-authorized-fn]
  (-> (liberator/resource
        :available-media-types ["application/json"]
        :allowed-methods [:get]
        :malformed? (fn [ctx]
                      (try
                        (if-let [limit (Integer/parseInt (get-in ctx [:request :params "limit"] "1000"))]
                          (if-not (pos? limit)
                            [true {::error (str "Limit " limit " most be positive")}]
                            [false {::limit limit}]))
                        (catch Exception e
                          [true {::error (str (.getMessage e))}])))
        :allowed? (fn [ctx]
                       (let [user (get-in ctx [:request :authorization/user])]
                         (if (is-authorized-fn user :read {:owner ::system :item :queue})
                           true
                           (do
                             (log/info user " has failed auth")
                             [false {::error "Unauthorized"}]))))
        :handle-forbidden (fn [ctx]
                            (log/info (get-in ctx [:request :authorization/user]) " is not authorized to access queue")
                            (render-error ctx))
        :handle-malformed render-error
        :handle-ok (fn [ctx]
                     (-> (map-vals (fn [queue]
                                     (->> queue
                                          (take (::limit ctx))
                                          (map d/touch)))
                                   (mesos-pending-jobs-fn))
                         cheshire/generate-string)))
      ring.middleware.json/wrap-json-params))


;;
;; /running
;;

(defn running-jobs
  [conn is-authorized-fn]
  (-> (liberator/resource
        :available-media-types ["application/json"]
        :allowed-methods [:get]
        :malformed? (fn [ctx]
                      (try
                        (if-let [limit (Integer/parseInt (get-in ctx [:request :params "limit"] "1000"))]
                          (if-not (pos? limit)
                            [true {::error (str "Limit " limit " most be positive")}]
                            [false {::limit limit}]))
                        (catch Exception e
                          [true {::error (str (.getMessage e))}])))
        :allowed? (fn [ctx]
                       (let [user (get-in ctx [:request :authorization/user])]
                         (if (is-authorized-fn user :read {:owner ::system :item :running})
                           true
                           [false {::error "Unauthorized"}])))
        :handle-forbidden render-error
        :handle-malformed render-error
        :handle-ok (fn [ctx]
                     (->> (util/get-running-task-ents (db conn))
                          (take (::limit ctx))
                          (map d/touch)
                          cheshire/generate-string)))
      ring.middleware.json/wrap-json-params))


;;
;; /retry
;;

(def ReadRetriesRequest
  {:job s/Uuid})
(def UpdateRetriesRequest
  (merge ReadRetriesRequest {:retries PosNum}))

(defn display-retries
  [conn ctx]
  (:job/max-retries (d/entity (db conn) [:job/uuid (::job ctx)])))

(defn check-job-exists
  [conn ctx]
  (let [job (or (get-in ctx [:request :query-params :job])
                (get-in ctx [:request :body-params :job]))]
    (if (used-uuid? (d/db conn) job)
      [true {::job job}]
      [false {::error (str "UUID " job " does not correspond to a job" )}])))

(defn check-retry-allowed
  [conn is-authorized-fn ctx]
  (let [request-user (get-in ctx [:request :authorization/user])
        job (::job ctx)
        job-owner (:job/user (d/entity (db conn) [:job/uuid job]))]
    (if-not (is-authorized-fn request-user :retry {:owner job-owner :item :job})
      [false {::error (str "You are not authorized to access that job")}]
      true)))

(defn base-retries-handler
  [conn is-authorized-fn liberator-attrs]
  (base-cook-handler
   (merge {:allowed? (partial check-retry-allowed conn is-authorized-fn)}
          liberator-attrs)))

(defn read-retries-handler
  [conn is-authorized-fn]
  (base-retries-handler
   conn is-authorized-fn
   {:allowed-methods [:get]
    :exists? (partial check-job-exists conn)
    :handle-ok (partial display-retries conn)}))

(defn post-retries-handler
  [conn is-authorized-fn]
  (base-retries-handler
   conn is-authorized-fn
   {:allowed-methods [:post]
    ;; we need to check if it's a valid job UUID in malformed? here,
    ;; because this endpoint currently isn't restful (POST used for what is
    ;; actually an idempotent update; it should be PUT).
    :malformed? (fn [ctx]
                  ;; negate the first element of vec returned by exists?
                  (let [exists (check-job-exists conn ctx)]
                        (assoc exists 0 (not (first exists)))))
    :handle-created (partial display-retries conn)
    :post! (fn [ctx]
             (util/retry-job! conn (::job ctx)
                              (get-in ctx [:request :body-params :retries])))}))

(defn put-retries-handler
  [conn is-authorized-fn]
  (base-retries-handler
   conn is-authorized-fn
   {:allowed-methods [:put]
    :exists? (partial check-job-exists conn)
    :handle-created (partial display-retries conn)
    :put! (fn [ctx]
            (util/retry-job! conn (::job ctx)
                             (get-in ctx [:request :body-params :retries])))}))


;;
;; /share
;;

(def UserParam {:user s/Str})

(def SetShareParams
  {:body-params (merge UserParam {:share {s/Keyword s/Num}})})

(def UserLimitsResponse
  {String s/Num})

(defn retrieve-user-share
  [conn ctx]
  (->> (or (get-in ctx [:request :query-params :user])
           (get-in ctx [:request :body-params :user]))
       (share/get-share (db conn))
       walk/stringify-keys))

(defn check-share-allowed
  [is-authorized-fn ctx]
  (let [request-user (get-in ctx [:request :authorization/user])
        request-method (get-in ctx [:request :request-method])]
    (if-not (is-authorized-fn request-user request-method {:owner ::system :item :share})
      [false {::error (str "You are not authorized to access share information")}]
      true)))

(defn base-share-handler
  [is-authorized-fn resource-attrs]
  (base-cook-handler (merge {:allowed? (partial check-share-allowed is-authorized-fn)}
                            resource-attrs)))

(defn read-share-handler
  [conn is-authorized-fn]
  (base-share-handler
   is-authorized-fn
   {:handle-ok (partial retrieve-user-share conn)}))

(defn delete-share-handler
  [conn is-authorized-fn]
  (base-share-handler
   is-authorized-fn
   {:allowed-methods [:delete]
    :delete!  (fn [ctx]
                (share/retract-share! conn (get-in ctx [:request :query-params :user])))}))

(defn update-share-handler
  [conn is-authorized-fn]
  (base-share-handler
   is-authorized-fn
   {:allowed-methods [:post]
    :handle-created (partial retrieve-user-share conn)
    :malformed? (fn [ctx]
                 (let [resource-types (set (util/get-all-resource-types (d/db conn)))
                       shares (->> (get-in ctx [:request :body-params :share])
                                   keywordize-keys
                                   (map-vals double))]
                   (cond
                     (not (seq shares))
                     [true {::error "No shares set. Are you specifying that this is application/json?"}]
                     (not (every? (partial contains? resource-types) (keys shares)))
                     [true {::error (str "Unknown resource type(s)" (str/join \space (remove (partial contains? resource-types) (keys shares))))}]
                     :else
                     [false {::shares shares}])))

    :post! (fn [ctx]
             (apply share/set-share! conn (get-in ctx [:request :body-params :user]) (reduce into [] (::shares ctx))))}))


;;
;; /quota
;;

(def SetQuotaParams
  {:body-params (merge UserParam {:quota {s/Keyword s/Num}})})

(defn retrieve-user-quota
  [conn ctx]
  (->> (or (get-in ctx [:request :query-params :user])
           (get-in ctx [:request :body-params :user]))
       (quota/get-quota (db conn))
       walk/stringify-keys))

(defn check-quota-allowed
  [is-authorized-fn ctx]
  (let [request-user (get-in ctx [:request :authorization/user])
        request-method (get-in ctx [:request :request-method])]
    (if-not (is-authorized-fn request-user request-method {:owner ::system :item :quota})
      [false {::error (str "You are not authorized to access quota information")}]
      true)))

(defn base-quota-handler
  [is-authorized-fn resource-attrs]
  (base-cook-handler (merge
                      {:allowed? (partial check-quota-allowed is-authorized-fn)}
                      resource-attrs)))

(defn read-quota-handler
  [conn is-authorized-fn]
  (base-share-handler
   is-authorized-fn
   {:handle-ok (partial retrieve-user-quota conn)}))

(defn delete-quota-handler
  [conn is-authorized-fn]
  (base-quota-handler
   is-authorized-fn
   {:allowed-methods [:delete]
    :delete!  (fn [ctx]
                (quota/retract-quota! conn (get-in ctx [:request :query-params :user])))}))

(defn update-quota-handler
  [conn is-authorized-fn]
  (base-quota-handler
   is-authorized-fn
   {:allowed-methods [:post]
    :handle-created (partial retrieve-user-quota conn)
    :malformed? (fn [ctx]
                  (let [resource-types (set (conj (util/get-all-resource-types (d/db conn)) :count))
                        quotas (->> (get-in ctx [:request :body-params :quota])
                                    keywordize-keys
                                    (map-vals double))
                        quotas (update-in quotas [:count] int)]
                    (cond
                      (not (seq quotas))
                      [true {::error "No quota set. Are you specifying that this is application/json?"}]
                      (not (every? (partial contains? resource-types) (keys quotas)))
                      [true {::error (str "Unknown resource type(s)" (str/join \space (remove (partial contains? resource-types) (keys quotas))))}]
                      :else
                      [false {::quotas quotas}])))

    :post! (fn [ctx]
             (apply quota/set-quota! conn (get-in ctx [:request :body-params :user]) (reduce into [] (::quotas ctx))))}))


;;
;; "main" - the entry point that routes to other handlers
;;

(defn main-handler
  [conn fid task-constraints gpu-enabled? mesos-pending-jobs-fn is-authorized-fn]
  (routes
   (c-api/api
    {:swagger {:ui "/swagger-ui"
               :spec "/swagger-docs"
               :data {:info {:title "Cook API"
                             :description "How to Cook things"}
                      :tags [{:name "api", :description "some apis"}]}}}

    (c-api/context
     "/rawscheduler" []
     (c-api/resource
      {:get {:summary "Returns info about a set of Jobs"
             :parameters {:query-params JobOrInstanceIds}
             :responses {200 {:schema [JobResponse]
                              :description "The jobs and their instances were returned."}
                         400 {:description "Non-UUID values were passed as jobs."}
                         403 {:description "The supplied UUIDs don't correspond to valid jobs."}}
             :handler (read-jobs-handler conn fid task-constraints gpu-enabled? is-authorized-fn)}
       :post {:summary "Schedules one or more jobs."
              :parameters {:body-params {:jobs [JobRequest]}}
              :responses {200 {:schema [JobResponse]}}
              :handler (create-jobs-handler conn fid task-constraints gpu-enabled? is-authorized-fn)}
       :delete {:summary "Cancels jobs, halting execution when possible."
                :responses {204 {:description "The jobs have been marked for termination."}
                            400 {:description "Non-UUID values were passed as jobs."}
                            403 {:description "The supplied UUIDs don't correspond to valid jobs."}}
                :parameters {:query-params {:job [s/Uuid]}}
                :handler (destroy-jobs-handler conn fid task-constraints gpu-enabled? is-authorized-fn)}}))

    (c-api/context
     "/share" []
     (c-api/resource
      {:produces ["application/json"],
       :responses {200 {:schema UserLimitsResponse
                        :description "User's share found"}
                   401 {:description "Not authorized to read shares."}
                   403 {:description "Invalid request format."}}
       :get {:summary "Read a user's share"
             :parameters {:query-params UserParam}
             :handler (read-share-handler conn is-authorized-fn)}
       :post {:summary "Change a user's share"
              :parameters SetShareParams
              :handler (update-share-handler conn is-authorized-fn)}
       :delete {:summary "Reset a user's share to the default"
                :parameters {:query-params UserParam}
                :handler (delete-share-handler conn is-authorized-fn)}}))

    (c-api/context
     "/quota" []
     (c-api/resource
      {:produces ["application/json"],
       :responses {200 {:schema UserLimitsResponse
                        :description "User's quota found"}
                   401 {:description "Not authorized to read quota."}
                   403 {:description "Invalid request format."}}
       :get {:summary "Read a user's quota"
             :parameters {:query-params UserParam}
             :handler (read-quota-handler conn is-authorized-fn)}
       :post {:summary "Change a user's quota"
              :parameters SetQuotaParams
              :handler (update-quota-handler conn is-authorized-fn)}
       :delete {:summary "Reset a user's quota to the default"
                :parameters {:query-params UserParam}
                :handler (delete-quota-handler conn is-authorized-fn)}}))

    (c-api/context
     "/retry" []
     (c-api/resource
      {:produces ["application/json"],
       :get {:summary "Read a job's retry count"
             :parameters {:query-params ReadRetriesRequest}
             :handler (read-retries-handler conn is-authorized-fn)
             :responses {200 {:schema PosInt
                              :description "The number of retries for the job"}
                         400 {:description "Invalid request format."}
                         404 {:description "The UUID doesn't correspond to a job."}}}
       :put
       {:summary "Change a job's retry count"
        :parameters {:body-params UpdateRetriesRequest}
        :handler (put-retries-handler conn is-authorized-fn)
        :responses {201 {:schema PosInt
                         :description "The number of retries for the job"}
                    400 {:description "Invalid request format."}
                    401 {:description "Request user not authorized to access that job."}
                    404 {:description "Unrecognized job UUID."}}}
       :post
       {:summary "Change a job's retry count (deprecated)"
        :parameters {:body-params UpdateRetriesRequest}
        :handler (post-retries-handler conn is-authorized-fn)
        :responses {201 {:schema PosInt
                         :description "The number of retries for the job"}
                    400 {:description "Invalid request format or bad job UUID."}
                    401 {:description "Request user not authorized to access that job."}}}})))

    (ANY "/queue" []
         (waiting-jobs mesos-pending-jobs-fn is-authorized-fn))
    (ANY "/running" []
         (running-jobs conn is-authorized-fn))
    ))

