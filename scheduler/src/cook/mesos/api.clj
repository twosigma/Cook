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
            [cook.mesos]
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
            [cook.mesos.util :as util]
            [clj-time.core :as t]
            [cook.mesos.share :as share]
            [cook.mesos.quota :as quota]
            [plumbing.core :refer (map-vals)]
            [cook.mesos.reason :as reason]
            [clojure.walk :refer (keywordize-keys)])
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

(def NonEmptyString
  (s/both s/Str (s/pred #(not (zero? (count %))) 'empty-string)))

(def Job
  "A schema for a job"
  {:uuid (s/pred #(instance? UUID %) 'uuid?)
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
                 :docker/force-pull-image (:force-pull-image docker)
                 :docker/network (:network docker)}
                (mk-container-params docker-id params)
                (mk-docker-ports docker-id port-mappings))])
      {})))

(sm/defn submit-jobs
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
                txn {:db/id id
                     :job/uuid uuid
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

(defn validate-and-munge-job
  "Takes the user and the parsed json from the job and returns proper
   Job objects, or else throws an exception"
  [db user task-constraints gpu-enabled? {:strs [cpus mem gpus uuid command priority max_retries max_runtime name uris ports env labels container] :as job}]
  (let [munged (merge
                 {:user user
                  :uuid (UUID/fromString uuid)
                  :name (or name "cookjob") ; Add default job name if user does not provide a name.
                  :command command
                  :priority (or priority util/default-job-priority)
                  :max-retries max_retries
                  :max-runtime (or max_runtime Long/MAX_VALUE)
                  :ports (or ports 0)
                  :cpus (double cpus)
                  :mem (double mem)}
                 (when gpus
                   {:gpus (int gpus)})
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
                   {:labels labels}))]
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
    (unused-uuid? db (:uuid munged))
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
  [db fid job-uuid]
  (let [job (d/entity db [:job/uuid job-uuid])
        resources (util/job-ent->resources job)]
    (merge
      {:command (:job/command job)
       :uuid (str (:job/uuid job))
       :name (:job/name job "cookjob")
       :priority (:job/priority job util/default-job-priority)
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

;;; On POST; JSON blob that looks like:
;;; {"jobs": [{"command": "echo hello world",
;;;            "uuid": "123898485298459823985",
;;;            "max_retries": 3
;;;            "cpus": 1.5,
;;;            "mem": 1000}]}
;;;
;;; On GET; use repeated job argument
(defn job-resource
  [conn fid task-constraints gpu-enabled? is-authorized-fn]
  (-> (resource
        :available-media-types ["application/json"]
        :allowed-methods [:post :get :delete]
        :malformed? (fn [ctx]
                      (condp contains? (get-in ctx [:request :request-method])
                        #{:get :delete}
                        (try
                          (if-let [jobs (get-in ctx [:request :params "job"])]
                            (let [jobs (if-not (vector? jobs) [jobs] jobs)
                                  jobs (mapv #(UUID/fromString %) jobs)
                                  used? (fn used? [uuid]
                                          (try
                                            (unused-uuid? (db conn) uuid)
                                            false
                                            (catch Exception e
                                              true)))]
                              (if (every? used? jobs)
                                [false {::jobs jobs}]
                                [true {::error (str "UUID "
                                                     (str/join
                                                       \space
                                                       (remove used? (::jobs ctx)))
                                                     " didn't correspond to a job")}]))
                            [true {::error "must supply at least one job query param"}])
                          (catch Exception e
                            [true {::error e}]))
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
                                                      gpu-enabled?
                                                      %)
                                                   (get params "jobs"))}])
                            (catch Exception e
                              (log/warn e "Malformed raw api request")
                              [true {::error e}])))))
        :allowed? (fn [ctx]
                    (condp contains? (get-in ctx [:request :request-method])
                      #{:get :delete}
                      (let [uuids (::jobs ctx)
                            user (get-in ctx [:request :authorization/user])
                            job-user (fn [uuid]
                                       (:job/user (d/entity (db conn) [:job/uuid uuid])))
                            authorized? (fn [uuid]
                                          (is-authorized-fn user (get-in ctx [:request :request-method]) {:owner (job-user uuid) :item :job}))]
                        (if (every? authorized? uuids)
                          true
                          [false {::error (str "You are not authorized to view access the following jobs "
                                               (str/join \space (remove authorized? uuids)))}]))

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
  [mesos-pending-jobs-fn is-authorized-fn]
  (-> (resource
        :available-media-types ["application/json"]
        :allowed-methods [:get]
        :allowed? (fn [ctx]
                       (let [user (get-in ctx [:request :authorization/user])]
                         (if (is-authorized-fn user :read {:owner ::system :item :queue})
                           true
                           (do
                             (log/info user " has failed auth")
                             [false {::error "Unauthorized"}]))))
        :handle-forbidden (fn [ctx]
                               (log/info (get-in ctx [:request :authorization/user]) " is not authorized to access queue")
                               (str (::error ctx)))
        :handle-ok (fn [ctx]
                     (->> (mesos-pending-jobs-fn)
                          (map (fn [[k v]] [k (map d/touch v)]))
                          (into {})
                          cheshire/generate-string)))
      ring.middleware.json/wrap-json-params))


(defn running-jobs
  [conn is-authorized-fn]
  (-> (resource
        :available-media-types ["application/json"]
        :allowed-methods [:get]
        :allowed? (fn [ctx]
                       (let [user (get-in ctx [:request :authorization/user])]
                         (if (is-authorized-fn user :read {:owner ::system :item :running})
                           true
                           [false {::error "Unauthorized"}])))
        :handle-forbidden (fn [ctx]
                               (str (::error ctx)))
        :handle-ok (fn [ctx]
                     (->> (util/get-running-task-ents (db conn))
                          (map d/touch)
                          cheshire/generate-string)))
      ring.middleware.json/wrap-json-params))

(defn share
  [conn is-authorized-fn]
  (-> (resource
        :available-media-types ["application/json"]
        :allowed-methods [:get :delete :post]
        :malformed? (fn [ctx]
                      (if-let [user (get-in ctx [:request :params "user"])]
                        (if (= :post (get-in ctx [:request :request-method]))
                          (let [resource-types (set (util/get-all-resource-types (d/db conn)))
                                shares (->> (get-in ctx [:request :params "share"])
                                            keywordize-keys
                                            (map-vals double))]
                            (cond
                              (not (seq shares))
                              [true {::error "No shares set. Are you specifying that this is application/json?"}]
                              (not (every? (partial contains? resource-types) (keys shares)))
                              [true {::error (str "Unknown resource type(s)" (str/join \space (remove (partial contains? resource-types) (keys shares))))}]
                              :else
                              [false {::shares shares}]))
                          false)
                        [true {::error "Must set the user to operate on"}]))
        :allowed? (fn [ctx]
                    (let [request-user (get-in ctx [:request :authorization/user])
                          request-method (get-in ctx [:request :request-method])]
                      ;; Can't used :authorized? here (though I would like to) because TS assumes
                      ;; that 401 means to retry with kerb creds which seems to break everything...
                      (if-not (is-authorized-fn request-user request-method {:owner ::system :item :share})
                        [false {::error (str "You are not authorized to access share information")}]
                        true)))
        :handle-forbidden (fn [ctx]
                            (str (::error ctx)))
        :handle-malformed (fn [ctx]
                            (str (::error ctx)))
        :post! (fn [ctx]
                 (apply share/set-share! conn (get-in ctx [:request :params "user"]) (apply concat (::shares ctx))))
        :delete! (fn [ctx]
                   (share/retract-share! conn (get-in ctx [:request :params "user"])))
        :handle-ok (fn [ctx]
                     (share/get-share (db conn) (get-in ctx [:request :params "user"])))
        :handle-created (fn [ctx]
                          (share/get-share (db conn) (get-in ctx [:request :params "user"])))
        :handle-accepted (fn [ctx]
                          (share/get-share (db conn) (get-in ctx [:request :params "user"]))))
      ring.middleware.json/wrap-json-params))

(defn quota
  [conn is-authorized-fn]
  (-> (resource
        :available-media-types ["application/json"]
        :allowed-methods [:get :delete :post]
        :malformed? (fn [ctx]
                      (if-let [user (get-in ctx [:request :params "user"])]
                        (if (= :post (get-in ctx [:request :request-method]))
                          (let [resource-types (set (conj (util/get-all-resource-types (d/db conn)) :count))
                                quotas (->> (get-in ctx [:request :params "quota"])
                                            keywordize-keys
                                            (map-vals double))
                                quotas (update-in quotas [:count] int)]
                            (cond
                              (not (seq quotas))
                              [true {::error "No quotas set. Are you specifying that this is application/json?"}]
                              (not (every? (partial contains? resource-types) (keys quotas)))
                              [true {::error (str "Unknown resource type(s) " (str/join \space (remove (partial contains? resource-types) (keys quotas))))}]
                              :else
                              [false {::quotas quotas}]))
                          false)
                        [true {::error "Must set the user to operate on"}]))
        :allowed? (fn [ctx]
                    (let [request-user (get-in ctx [:request :authorization/user])
                          request-method (get-in ctx [:request :request-method])]
                      ;; Can't used :authorized? here (though I would like to) because TS assumes
                      ;; that 401 means to retry with kerb creds which seems to break everything...
                      (if-not (is-authorized-fn request-user request-method {:owner ::system :item :quota})
                        [false {::error (str "You are not authorized to access quota information")}]
                        true)))
        :handle-forbidden (fn [ctx]
                            (str (::error ctx)))
        :handle-malformed (fn [ctx]
                            (str (::error ctx)))
        :post! (fn [ctx]
                 (apply quota/set-quota! conn (get-in ctx [:request :params "user"]) (apply concat (::quotas ctx))))
        :delete! (fn [ctx]
                   (quota/retract-quota! conn (get-in ctx [:request :params "user"])))
        :handle-ok (fn [ctx]
                     (quota/get-quota (db conn) (get-in ctx [:request :params "user"])))
        :handle-created (fn [ctx]
                          (quota/get-quota (db conn) (get-in ctx [:request :params "user"])))
        :handle-accepted (fn [ctx]
                           (quota/get-quota (db conn) (get-in ctx [:request :params "user"]))))
      ring.middleware.json/wrap-json-params))

(defn retries
  [conn is-authorized-fn]
  (-> (resource
        :available-media-types ["application/json"]
        :allowed-methods [:get :post]
        :malformed? (fn [ctx]
                      (try
                        (let [job (get-in ctx [:request :params "job"])
                              new-retries (get-in ctx [:request :params "retries"])
                              request-method (get-in ctx [:request :request-method])]
                          (when-not job
                            (throw (ex-info (str "'job' must be provided") {})))
                          (when (and (= :post request-method) (not new-retries))
                            (throw (ex-info "'retries' must be provided" {})))
                          (let [job-uuid (UUID/fromString job)
                                used? (fn used? [uuid]
                                        (try
                                          (unused-uuid? (db conn) uuid)
                                          false
                                          (catch Exception e
                                            true)))]
                            (when-not (used? job-uuid)
                              (throw (ex-info (str "UUID" job " does not correspond to a job" ) {})))
                            (if (= :post request-method)
                              (let [new-retries (Integer/parseInt new-retries)]
                                (when-not (pos? new-retries)
                                  (throw (ex-info (str "Retries (" new-retries ") must be positive") {})))
                                [false {::retries new-retries ::job job-uuid}])
                              [false {::job job-uuid}])))
                        (catch Exception e
                          [true {::error (.getMessage e)}])))
        :allowed? (fn [ctx]
                    (let [request-user (get-in ctx [:request :authorization/user])
                          request-method (get-in ctx [:request :request-method])
                          job (::job ctx)
                          job-owner (:job/user (d/entity (db conn) [:job/uuid job]))]
                      ;; Can't used :authorized? here (though I would like to) because TS assumes
                      ;; that 401 means to retry with kerb creds which seems to break everything...
                      (if-not (is-authorized-fn request-user :retry {:owner job-owner :item :job})
                        [false {::error (str "You are not authorized to access that job")}]
                        true)))
        :handle-forbidden (fn [ctx]
                            (str (::error ctx)))
        :handle-malformed (fn [ctx]
                            (str (::error ctx)))
        :post! (fn [ctx]
                 (util/retry-job! conn (::job ctx) (::retries ctx)))
        :handle-ok (fn [ctx]
                     (str (:job/max-retries (d/entity (db conn) [:job/uuid (::job ctx)]))))
        :handle-created (fn [ctx]
                          (str (:job/max-retries (d/entity (db conn) [:job/uuid (::job ctx)])))))
      ring.middleware.json/wrap-json-params))

(defn handler
  [conn fid task-constraints gpu-enabled? mesos-pending-jobs-fn is-authorized-fn]
  (routes
    (ANY "/rawscheduler" []
         (job-resource conn fid task-constraints gpu-enabled? is-authorized-fn))
    (ANY "/queue" []
         (waiting-jobs mesos-pending-jobs-fn is-authorized-fn))
    (ANY "/running" []
         (running-jobs conn is-authorized-fn))
    (ANY "/share" []
         (share conn is-authorized-fn))
    (ANY "/quota" []
         (quota conn is-authorized-fn))
    (ANY "/retry" []
         (retries conn is-authorized-fn))))
