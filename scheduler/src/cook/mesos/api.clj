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
            [schema.macros :as sm]

            [compojure.core :refer (routes ANY)]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [clojure.core.cache :as cache]
            [clojure.walk :as walk]
            [clj-http.client :as http]
            [ring.middleware.json]
            [clojure.data.json :as json]
            [liberator.core :refer [resource defresource]]
            [cheshire.core :as cheshire]
            [clj-time.core :as t]

            [cook.mesos]
            [cook.authorization :as auth :refer [owner is-authorized?]]
            [cook.mesos.util :as util]
)
  (:import java.util.UUID))

(def PosDouble
  (s/both double (s/pred pos? 'pos?)))

(def PortMapping
  "Schema for Docker Portmapping"
  {:host-port (s/both s/Int (s/pred #(<= 0 % 65536) 'between-0-and-65536))
   :container-port (s/both s/Int (s/pred #(<= 0 % 65536) 'between-0-and-65536))
   (s/optional-key :protocol) s/Str})

(def DockerInfo
  "Schema for a DockerInfo"
  {:image s/Str
   (s/optional-key :network) s/Str
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

(def NonEmptyString
  (s/both s/Str (s/pred #(not (zero? (count %))) 'empty-string)))


(s/defrecord Job
    [uuid :- (s/pred #(instance? UUID %) 'uuid?)
     command :- s/Str
     ;; Make sure the job name is a valid string which can only contain '.', '_', '-' or any word characters and has
     ;; length at most 128.
     name :- (s/both s/Str (s/pred #(re-matches #"[\.a-zA-Z0-9_-]{0,128}" %) 'under-128-characters-and-alphanum?))
     priority :- (s/both s/Int (s/pred #(<= 0 % 100) 'between-0-and-100))
     max-retries :- (s/both s/Int (s/pred pos? 'pos?))
     max-runtime :- (s/both s/Int (s/pred pos? 'pos?))
     cpus :- PosDouble
     mem :- PosDouble
     ;; Make sure the user name is valid. It must begin with a lower case character, end with
     ;; a lower case character or a digit, and has length between 2 to (62 + 2).
     user :- (s/both s/Str (s/pred #(re-matches #"\A[a-z][a-z0-9_-]{0,62}[a-z0-9]\z" %) 'lowercase-alphanum?))     
     ]
  {  
   (s/optional-key :uris) [Uri]
   (s/optional-key :ports) [(s/pred zero? 'zero)] ;;TODO add to docs the limited uri/port support
   (s/optional-key :env) {NonEmptyString s/Str}
   (s/optional-key :labels) {NonEmptyString s/Str}
   (s/optional-key :container) Container
   }

  auth/Ownable
  (owner [this] (:user this)))

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
                 :docker/network (:network docker)}
                (mk-container-params docker-id params)
                (mk-docker-ports docker-id port-mappings))])
      {})))

(sm/defn submit-jobs
  [conn jobs :- [Job]]
  (doseq [{:keys [uuid command max-retries max-runtime priority cpus mem user name ports uris env labels container]} jobs
          :let [id (d/tempid :db.part/user)
                ports (mapv (fn [port]
                              ;;TODO this schema might not work b/c all ports are zero
                              [:db/add id :job/port port])
                            ports)
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
                               (when (and name (not= name "cookjob"))
                                 [[:db/add id :job/name name]])
                               (when (and priority (not= util/default-job-priority priority))
                                 [[:db/add id :job/priority priority]])
                               (when (and max-runtime (not= Long/MAX_VALUE max-runtime))
                                 [[:db/add id :job/max-runtime max-runtime]]))
                txn {:db/id id
                     :job/uuid uuid
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
                          (conj txn))))
  "ok")

(defn unused-uuid?
  "Throws if the given uuid is used in datomic"
  [db uuid]
  (when (seq (q '[:find ?j
                  :in $ ?uuid
                  :where
                  [?j :job/uuid ?uuid]]
                db uuid))
    (throw (ex-info (str "UUID " uuid " already used") {:uuid uuid}))))


(defn job-owner-by-uuid
  "Returns the username that owns the given UUID, or nil if no job with
  that UUID exists."
  [db uuid]
  (let [uuid (if (string? uuid)
               (UUID/fromString uuid)
               uuid)]
    (some-> (d/q '[:find ?owner
                   :in $ ?uuid
                   :where [?e :job/uuid ?uuid ] [?e :job/user ?owner]]
                 db 
                 uuid)
            first first)))


(defn validate-and-munge-job
  "Takes the user and the parsed json from the job and returns proper
   Job instances, or else throws an exception"
  [db user task-constraints {:strs [cpus mem uuid command priority max_retries max_runtime name uris ports env labels container] :as job}]
  (let [munged (merge
                 {:user user
                  :uuid (UUID/fromString uuid)
                  :name (or name "cookjob") ; Add default job name if user does not provide a name.
                  :command command
                  :priority (or priority util/default-job-priority)
                  :max-retries max_retries
                  :max-runtime (or max_runtime Long/MAX_VALUE)
                  :ports (or ports [])
                  :cpus (double cpus)
                  :mem (double mem)}
                 (when container
                   {:container (walk/keywordize-keys container)})
                 (when uris
                   {:uris (map (fn [{:strs [value executable cache extract]}]
                                 (merge {:value value}
                                        (when executable {:executable? executable})
                                        (when cache {:cache? cache})
                                        (when extract {:extract? extract})))
                               uris)})
                 (when env
                   ;; Remains strings
                   {:env env})
                 (when labels 
                   ;; Remains strings
                   {:labels labels}))
        record (map->Job munged)]
    (s/validate Job record)
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
    (unused-uuid? db (:uuid munged))
    record))

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
        framework-executors (for [framework (concat (get slave-state "frameworks")
                                                    (get slave-state "completed_frameworks"))
                                  :when (= framework-id (get framework "id"))
                                  e (concat (get framework "executors")
                                            (get framework "completed_executors"))]
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
  "Fetches metadata about the given job ID from Datomic.
   Returns nil if there is no job with that UUID in datomic"
  [db fid job-uuid]
  (if-let [job (d/entity db [:job/uuid job-uuid])]
    (let [resources (util/job-ent->resources job)]
      (map->Job
       {:command (:job/command job)
        :uuid (str (:job/uuid job))
        :user (:job/user job)
        :name (:job/name job "cookjob")
        :priority (:job/priority job util/default-job-priority)
        :cpus (:cpus resources)
        :mem (:mem resources)
        :max_retries  (:job/max-retries job) ; Consistent with input
        :max_runtime (:job/max-runtime job Long/MAX_VALUE) ; Consistent with input
        :framework_id fid
        :status (name (:job/state job))
        :uris (:uris resources)
        :env (util/job-ent->env job)
        :labels (util/job-ent->label job)
        ;;TODO include ports
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
                     reason-code (:instance/reason-code instance)
                     base {:task_id (:instance/task-id instance)
                           :hostname hostname
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
                     base (if reason-code
                            (assoc base :reason_code reason-code)
                            base)]
                 base))
             (:job/instance job))}))))

;;; On POST; JSON blob that looks like:
;;; {"jobs": [{"command": "echo hello world",
;;;            "uuid": "123898485298459823985",
;;;            "max_retries": 3
;;;            "cpus": 1.5,
;;;            "mem": 1000}]}
;;;
;;; On GET; use repeated job argument
(defn job-resource
  [conn fid task-constraints auth-fn]
  (-> (resource
        :available-media-types ["application/json"]
        :allowed-methods [:post :get :delete]
        :malformed? (fn [ctx]
                      (condp contains? (get-in ctx [:request :request-method])
                        #{:get :delete}
                        (if-let [jobs (get-in ctx [:request :params "job"])]
                          (let [jobs (if-not (vector? jobs) [jobs] jobs)]
                            (try
                              [false {::jobs (mapv #(UUID/fromString %) jobs)}]
                              (catch Exception e
                                [true {::error e}])))
                          [true {::error "must supply at least one job query param"}])
                        #{:post}
                        (let [params (get-in ctx [:request :params])
                              user (get-in ctx [:request :authorization/user])]
                          (try
                            (cond
                              (empty? params)
                              [true {::error "must supply at least one job to start. Are you specifying that this is application/json?"}]
                              :else
                              [false {::jobs (mapv #(validate-and-munge-job
                                                      (db conn)
                                                      user
                                                      task-constraints
                                                      %)
                                                   (get params "jobs"))}])
                            (catch Exception e
                              (log/warn e "Malformed raw api request")
                              [true {::error e}])))))
        :allowed? (fn [ctx]
                    (condp contains? (get-in ctx [:request :request-method])
                      #{:get :delete}
                      (let [user  (get-in ctx [:request :authorization/user])
                            uuids (::jobs ctx)

                            datomic (db conn)
                            used? (fn [uuid]
                                    (try
                                      (unused-uuid? datomic uuid)
                                      false
                                      (catch Exception e
                                        true)))

                            ;; Changes each UUID into a map describing
                            ;; whether it's allowed, and if not, why
                            ;; it's not allowed.
                            uuid-filter (fn [uuid]
                                          (let [job (fetch-job-map (db conn)
                                                                   fid
                                                                   uuid)]
                                            (cond (nil? job) (do
                                                               (log/info "[job-resource] Couldn't find a job with UUID" uuid)
                                                               {:is-allowed false
                                                                :message (str "No job found with UUID " uuid)
                                                                :uuid uuid})
                                                  (not (auth-fn
                                                        user
                                                        :access
                                                        job)) (do
                                                                (log/info "[job-resource] User" user " is not authorized to accesss job UUID" uuid)
                                                                {:is-allowed false
                                                                 ;; Message does not tell user that they weren't allowed to access this job, to
                                                                 ;; avoid leaking information about whether a given UUID exists in the system.
                                                                 :message (str "No job found with UUID " uuid)
                                                                 :uuid uuid})
                                                        :else {:is-allowed true
                                                               :uuid uuid})))
                            
                            uuids (map uuid-filter uuids)]
                        ;; Return true if every UUID is in use, or
                        ;; else return false with a list of
                        ;; nonexistant UUIDs.
                        (if (every? true? (map :is-allowed uuids))
                          true
                          (let [message  (->> (map :message uuids)
                                                 (remove nil?)
                                                 (str/join "; " ))]
                            (log/info "[job-resource] Denying access:" message)
                            [false {::error message}])))
                      #{:post}
                      true))
        :handle-malformed (fn [ctx]
                            (str (::error ctx)))
        :handle-forbidden (fn [ctx]
                            (str (::error ctx)))
        :processable? (fn [ctx]
                        (if (= :post (get-in ctx [:request :request-method]))
                          (try
                            (log/info "Submitting jobs through raw api:" (::jobs ctx))
                            (submit-jobs conn (::jobs ctx))
                            true
                            (catch Exception e
                              (log/error e "Error submitting jobs through raw api")
                              [false (str e)]))
                          true))
        :post! (fn [ctx]
                 ;; We did the actual logic in processable?, so there's nothing left to do
                 {::results (str/join \space (cons "submitted jobs" (map (comp str :uuid) (::jobs ctx))))})
        :delete! (fn [ctx]
                   (cook.mesos/kill-job conn (::jobs ctx)))
        :handle-ok (fn [ctx]
                     (mapv (partial fetch-job-map (db conn) fid) (::jobs ctx)))
        :handle-created (fn [ctx]
                          (::results ctx)))
      ring.middleware.json/wrap-json-params))

(defn waiting-jobs
  [mesos-pending-jobs-fn auth-fn]
  (-> (resource
        :available-media-types ["application/json"]
        :allowed-methods [:get]
        :allowed? (fn [ctx]
                    (let [user  (get-in ctx [:request :authorization/user])]
                      ;; Only allow superusers:
                      (if (auth-fn user :access cook.authorization/system)
                        true
                        (do
                          (log/info "[waiting-jobs] Queried by non-admin" user "; denying access.")
                          [false {::error "Unauthorized"}]))))
        :handle-ok (fn [ctx]
                     (->> (mesos-pending-jobs-fn)
                          (map d/touch)
                          cheshire/generate-string)))
      ring.middleware.json/wrap-json-params))

(defn running-jobs
  [conn auth-fn]
  (-> (resource
        :available-media-types ["application/json"]
        :allowed-methods [:get]
        :allowed? (fn [ctx]
                    (let [user  (get-in ctx [:request :authorization/user])]
                      ;; Only allow superusers:
                      (if (auth-fn user :access cook.authorization/system)
                        true
                        (do
                          (log/info "[running-jobs] Queried by non-admin" user "; denying access.")
                          [false {::error "Unauthorized"}]))))
        :handle-ok (fn [ctx]
                     (->> (util/get-running-task-ents (db conn))
                          (map d/touch)
                          cheshire/generate-string)))
      ring.middleware.json/wrap-json-params))


(defn handler
  [conn fid task-constraints mesos-pending-jobs-fn auth-config]
  (let [auth-fn (partial is-authorized? auth-config)]
    (routes
     (ANY "/rawscheduler" []
          (job-resource conn fid task-constraints auth-fn))
     (ANY "/queue" []
          (waiting-jobs mesos-pending-jobs-fn auth-fn))
     (ANY "/running" []
          (running-jobs conn auth-fn)))))
