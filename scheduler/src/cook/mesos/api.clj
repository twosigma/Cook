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
            [camel-snake-kebab.core :refer [->snake_case ->kebab-case]]
            [compojure.core :refer (GET POST ANY routes)]
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
            [me.raynes.conch :as sh]
            [clj-time.core :as t]
            [metrics.timers :as timers]
            [cook.mesos.schema :refer (host-placement-types straggler-handling-types)]
            [cook.mesos.share :as share]
            [cook.mesos.quota :as quota]
            [plumbing.core :refer (map-vals map-from-vals)]
            [cook.mesos.reason :as reason]
            [swiss.arrows :refer :all]
            [clojure.walk :refer (keywordize-keys)])
  (:import java.util.UUID))

;; This is necessary to prevent a user from requesting a uid:gid
;; pair other than their own (for example, root)
(sh/let-programs [_id "/usr/bin/id"]
  (defn uid [user-name]
    (clojure.string/trim (_id "-u" user-name)))
  (defn gid [user-name]
    (clojure.string/trim (_id "-g" user-name))))

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
   :handle-not-found render-error
   :handle-unprocessable-entity render-error})

(defn base-cook-handler
  [resource-attrs]
  (mapply liberator/resource (merge cook-liberator-attrs resource-attrs)))


;;
;; /rawscheduler
;;

(defn prepare-schema-response
  "Takes a schema and outputs a new schema which conforms to our responses.

   Modifications:
   1. changes keys to snake_case"
  [schema]
  (map-keys (fn [k]
              (if (instance? schema.core.OptionalKey k)
                (update k :k ->snake_case)
                (->snake_case k)))
            schema))

(def PosNum
  (s/both s/Num (s/pred pos? 'pos?)))

(def PosInt
  (s/both s/Int (s/pred pos? 'pos?)))

(def NonNegInt
  (s/both s/Int (s/pred (comp not neg?) 'non-negative?)))

(def PosDouble
  (s/both double (s/pred pos? 'pos?)))

(def NonEmptyString
  (s/both s/Str (s/pred #(not (zero? (count %))) 'not-empty-string)))

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
   (s/optional-key :output_url) s/Str
   (s/optional-key :reason_string) s/Str})

(def Job
  "A schema for a job"
  {:uuid s/Uuid
   :command s/Str
   ;; Make sure the job name is a valid string which can only contain '.', '_', '-' or any word characters and has
   ;; length at most 128.
   :name (s/both s/Str (s/pred #(re-matches #"[\.a-zA-Z0-9_-]{0,128}" %) 'under-128-characters-and-alphanum?))
   :priority (s/both s/Int (s/pred #(<= 0 % 100) 'between-0-and-100))
   :max-retries PosInt
   :max-runtime PosInt
   (s/optional-key :uris) [Uri]
   (s/optional-key :ports) (s/pred #(not (neg? %)) 'nonnegative?)
   (s/optional-key :env) {NonEmptyString s/Str}
   (s/optional-key :labels) {NonEmptyString s/Str}
   (s/optional-key :container) Container
   (s/optional-key :group) s/Uuid
   :cpus PosDouble
   :mem PosDouble
   (s/optional-key :gpus) (s/both s/Int (s/pred pos? 'pos?))
   ;; Make sure the user name is valid. It must begin with a lower case character, end with
   ;; a lower case character or a digit, and has length between 2 to (62 + 2).
   :user (s/both s/Str (s/pred #(re-matches #"\A[a-z][a-z0-9_-]{0,62}[a-z0-9]\z" %) 'lowercase-alphanum?))})

(def JobRequest
  "Schema for the part of a request that launches a single job."
  (-> Job
    ;; make max-runtime optional.
    ;; It is *not* optional internally but don't want to force users to set it
    (dissoc :max-runtime)
    (dissoc :user)
    (merge {(s/optional-key :uris) [UriRequest]
            (s/optional-key :env) {s/Keyword s/Str}
            (s/optional-key :labels) {s/Keyword s/Str}
            (s/optional-key :max-runtime) PosInt
            :cpus PosNum
            :mem PosNum})))

(def JobResponse
  "Schema for a description of a job (as returned by the API).
  The structure is similar to JobRequest, but has some differences.
  For example, it can include descriptions of instances for the job."
  (-> JobRequest
      (dissoc :user)
      (dissoc (s/optional-key :group))
      (merge {:framework-id (s/maybe s/Str)
              :status s/Str
              :submit-time PosInt
              :retries-remaining NonNegInt
              (s/optional-key :groups) [s/Uuid]
              (s/optional-key :gpus) s/Int
              (s/optional-key :instances) [Instance]})
      prepare-schema-response))

(def JobOrInstanceIds
  "Schema for any number of job and/or instance uuids"
  {(s/optional-key :job) [s/Uuid]
   (s/optional-key :instance) [s/Uuid]})

(def Attribute-Equals-Parameters
  "A schema for the parameters of a host placement with type attribute-equals"
  {:attribute s/Str})

(def HostPlacement
  "A schema for host placement"
  (s/conditional
    #(= (:type %) :attribute-equals)
      {:type (s/eq :attribute-equals)
       :parameters Attribute-Equals-Parameters}
    :else
      {:type (s/pred #(contains? (set (map util/without-ns host-placement-types)) %)
                     'host-placement-type-exists?)
       (s/optional-key :parameters) {}}))

(def Quantile-Deviation-Parameters
  {:multiplier (s/both s/Num (s/pred #(> % 1.0) 'greater-than-one))
   :quantile (s/both s/Num (s/pred #(< 0.0 % 1.0) 'between-zero-one))})

(def StragglerHandling
  "A schema for host placement"
  (s/conditional
    #(= (:type %) :quantile-deviation)
      {:type (s/eq :quantile-deviation)
       :parameters Quantile-Deviation-Parameters}
    :else
      {:type (s/pred #(contains? (set (map util/without-ns straggler-handling-types)) %)
                     'straggler-handling-type-exists?)
       (s/optional-key :parameters) {}}))

(def Group
  "A schema for a job group"
  {:uuid s/Uuid
   (s/optional-key :host-placement) HostPlacement
   (s/optional-key :straggler-handling) StragglerHandling
   (s/optional-key :name) s/Str})

(def GroupResponse
  "A schema for a group http response"
  (-> Group
      (merge
        {:jobs [s/Uuid]
          (s/optional-key :waiting) s/Int
          (s/optional-key :running) s/Int
          (s/optional-key :completed) s/Int})
      prepare-schema-response))

(def RawSchedulerRequest
  "Schema for a request to the raw scheduler endpoint."
  {:jobs [JobRequest]
   (s/optional-key :groups) [Group]})


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
  [user id container]
  (let [container-id (d/tempid :db.part/user)
        docker-id (d/tempid :db.part/user)
        ctype (:type container)
        volumes (or (:volumes container) [])]
    (if (= (clojure.string/lower-case ctype) "docker")
      (let [docker (:docker container)
            params (or (:parameters docker) [])
            port-mappings (or (:port-mapping docker) [])
            user-params (filter #(= (:key %) "user") params)
            expected-user-param (str (uid user) ":" (gid user))]
        (when (some #(not= expected-user-param (:value %)) user-params)
          (throw (ex-info "user parameter must match uid and gid of user submitting."
                          {:expected-user-param expected-user-param
                           :user-params-submitted user-params})))
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

(s/defn make-job-txn
  "Creates the necessary txn data to insert a job into the database"
  [job :- Job]
  (let [{:keys [uuid command max-retries max-runtime priority cpus mem gpus user name ports uris env labels container group]
         :or {group nil}
         } job
        db-id (d/tempid :db.part/user)
        ports (when (and ports (not (zero? ports)))
                [[:db/add db-id :job/ports ports]])
        uris (mapcat (fn [{:keys [value executable? cache? extract?]}]
                       (let [uri-id (d/tempid :db.part/user)
                             optional-params {:resource.uri/executable? executable?
                                              :resource.uri/extract? extract?
                                              :resource.uri/cache? cache?}]
                         [[:db/add db-id :job/resource uri-id]
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
                        [[:db/add db-id :job/environment env-var-id]
                         {:db/id env-var-id
                          :environment/name k
                          :environment/value v}]))
                    env)
        labels (mapcat (fn [[k v]]
                      (let [label-var-id (d/tempid :db.part/user)]
                        [[:db/add db-id :job/label label-var-id]
                         {:db/id label-var-id
                          :label/key k
                          :label/value v}]))
                    labels)
        container (if (nil? container) [] (build-container user db-id container))
        ;; These are optionally set datoms w/ default values
        maybe-datoms (reduce into
                             []
                             [(when (and priority (not= util/default-job-priority priority))
                                [[:db/add db-id :job/priority priority]])
                              (when (and max-runtime (not= Long/MAX_VALUE max-runtime))
                                [[:db/add db-id :job/max-runtime max-runtime]])
                              (when (and gpus (not (zero? gpus)))
                                  (let [gpus-id (d/tempid :db.part/user)]
                                    [[:db/add db-id :job/resource gpus-id]
                                     {:db/id gpus-id
                                      :resource/type :resource.type/gpus
                                      :resource/amount (double gpus)}]))])
        commit-latch-id (d/tempid :db.part/user)
        commit-latch {:db/id commit-latch-id
                      :commit-latch/uuid (java.util.UUID/randomUUID)
                      :commit-latch/committed? true}
        txn {:db/id db-id
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
                             :resource/amount mem}]}]

    ;; TODO batch these transactions to improve performance
    (-> ports
        (into uris)
        (into env)
        (into labels)
        (into container)
        (into maybe-datoms)
        (conj txn)
        (conj commit-latch))))

(s/defn make-host-placement-txn
  "Creates the transaction data necessary to insert a host placement into the database.
   Returns a vector [host-placement-id txns]"
  [hp :- [HostPlacement]]
  (let [params (:parameters hp)
        params-txn-id (d/tempid :db.part/user)
        type (keyword "host-placement.type" (name (:type hp)))
        params-txn (when (= type :host-placement.type/attribute-equals)
                      {:db/id params-txn-id
                       :host-placement.attribute-equals/attribute (:attribute params)})
        hp-txn-id (d/tempid :db.part/user)
        hp-txn (merge {:db/id hp-txn-id
                        :host-placement/type [:host-placement.type/name type]}
                       (when params-txn
                         {:host-placement/parameters params-txn-id}))
        txns (if (nil? params-txn)
               [hp-txn]
               [params-txn hp-txn])]
    [hp-txn-id txns]))

(defn namespace-straggler-handling-parameters
  "Adds the namespace to straggler-handling parameters

   Follows the convention that the namespace is:
   'straggler-handling.<type of straggler-handling>/<parameter name>"
  [straggler-handling]
  (map-keys #(keyword (str "straggler-handling." (name (:type straggler-handling))) (name %))
            (:parameters straggler-handling)))

(s/defn make-straggler-handling-txn
  "Returns a map to create the straggler-handling txn.
   Uses the fact that straggler-handling and its components are marked with
   isComponent true"
  [straggler-handling :- [StragglerHandling]]
  (merge {:straggler-handling/type (keyword "straggler-handling.type"
                                            (name (:type straggler-handling)))}
         (when (:parameters straggler-handling)
           {:straggler-handling/parameters
            (namespace-straggler-handling-parameters straggler-handling)})))

(s/defn make-group-txn
  "Creates the transaction data necessary to insert a group to the database. job-db-ids is the
   list of all db-ids (datomic temporary ids) of jobs that belong to this group"
  [group :- Group job-db-ids]
  (let [uuid (:uuid group)
        [hp-txn-id hp-txns] (-> group
                                :host-placement
                                make-host-placement-txn)
        group-txn {:db/id (d/tempid :db.part/user)
                   :group/uuid uuid
                   :group/name (:name group)
                   :group/host-placement hp-txn-id
                   :group/job job-db-ids
                   :group/straggler-handling (-> group
                                                 :straggler-handling
                                                 (make-straggler-handling-txn))}]
    (conj hp-txns group-txn)))

(defn group-exists?
  "True if a group with guuid exists in the database, false otherwise"
  [db guuid]
  (not (nil? (d/entity db [:group/uuid guuid]))))

(defn job-exists?
  "True if a job with juuid exists in the database, false otherwise"
  [db juuid]
  (not (nil? (d/entity db [:job/uuid juuid]))))

(defn valid-group-uuid?
  "Returns truth-y if the provided guuid corresponds to a valid group for a new
   job to belong to. Returns false otherwise."
  [db new-guuids commit-latch-id override-immutability? guuid]
  (let [group-exists (group-exists? db guuid)]
    (cond
      (contains? new-guuids guuid) :created-in-transaction
      (not group-exists) :new
      (and group-exists override-immutability?) :override-existing
      :else false)))

(defn make-default-host-placement
  []
  {:type :all})

(def default-straggler-handling
  {:type :none})

(defn make-default-group
  [guuid]
  ; Have to validate (check uuid is unused)
  {:uuid guuid
   :name "defaultgroup"
   :host-placement (make-default-host-placement)
   :straggler-handling default-straggler-handling})

(defn validate-and-munge-job
  "Takes the user, the parsed json from the job and a list of the uuids of
   new-groups (submitted in the same request as the job). Returns proper Job
   objects, or else throws an exception"
  [db user task-constraints gpu-enabled? new-group-uuids
   {:keys [cpus mem gpus uuid command priority max-retries max-runtime name
           uris ports env labels container group]
    :or {group nil}
    :as job}
   & {:keys [commit-latch-id override-group-immutability?]
      :or {commit-latch-id nil
           override-group-immutability? false}}]
  (let [group-uuid (when group group)
        munged (merge
                 {:user user
                  :uuid uuid
                  :command command
                  :name (or name "cookjob") ; Add default job name if user does not provide a name.
                  :priority (or priority util/default-job-priority)
                  :max-retries max-retries
                  :max-runtime (or max-runtime Long/MAX_VALUE)
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
                   {:labels (walk/stringify-keys labels)})
                 (when group-uuid
                   {:group group-uuid}))]
    (s/validate Job munged)
    (when (and (:gpus munged) (not gpu-enabled?))
      (throw (ex-info (str "GPU support is not enabled") {:gpus gpus})))
    (when (> cpus (:cpus task-constraints))
      (throw (ex-info (str "Requested " cpus " cpus, but only allowed to use "
                           (:cpus task-constraints))
                      {:constraints task-constraints
                       :job job})))
    (when (> mem (* 1024 (:memory-gb task-constraints)))
      (throw (ex-info (str "Requested " mem "mb memory, but only allowed to use "
                           (* 1024 (:memory-gb task-constraints)))
                      {:constraints task-constraints
                       :job job})))
    (when (> max-retries (:retry-limit task-constraints))
      (throw (ex-info (str "Requested " max-retries " exceeds the maximum retry limit")
                      {:constraints task-constraints
                       :job job})))
    (doseq [{:keys [executable? extract?] :as uri} (:uris munged)
            :when (and (not (nil? executable?)) (not (nil? extract?)))]
      (throw (ex-info "Uri cannot set executable and extract" uri)))
    (when (job-exists? db uuid)
      (throw (ex-info (str "Job UUID " uuid " already used") {:uuid uuid})))
    (when (and group-uuid
               (not (valid-group-uuid? db
                                       new-group-uuids
                                       commit-latch-id
                                       override-group-immutability?
                                       group-uuid)))
      (throw (ex-info (str "Invalid group UUID " group-uuid " provided. A valid"
                           " group UUID either: 1. belongs to a group created "
                           "in the same request or 2. does not belong to any "
                           "existing group, or group created in this request")
                      {:uuid group-uuid})))
    munged))

(defn validate-and-munge-group
  "Takes the parsed json from the group and returns proper Group objects, or else throws an
   exception"
  [db group]
  (let [group-name (:name group)
        hp (:host-placement group)
        group (-> group
                  (assoc :name (or group-name "cookgroup"))
                  (assoc :host-placement (or hp (make-default-host-placement)))
                  (assoc :straggler-handling (or (:straggler-handling group)
                                                 default-straggler-handling)))]
    (s/validate Group group)
    (when (group-exists? db (:uuid group))
      (throw (ex-info (str "Group UUID " (:uuid group) " already used") {:uuid group})))
    group))


(defn get-executor-states-impl
  "Builds an indexed version of all executor states on the specified slave. Has no cache; takes
   100-500ms to run."
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
    (map-from-vals #(get % "id") framework-executors)))

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
        resources (util/job-ent->resources job)
        groups (:group/_job job)]
    (merge
      {:command (:job/command job)
       :uuid (str (:job/uuid job))
       :name (:job/name job "cookjob")
       :priority (:job/priority job util/default-job-priority)
       :submit_time (when (:job/submit-time job) ; due to a bug, submit time may not exist for some jobs
                      (.getTime (:job/submit-time job)))
       :cpus (:cpus resources)
       :mem (:mem resources)
       :gpus (int (:gpus resources 0))
       :max_retries  (:job/max-retries job) ; consistent with input
       :retries_remaining (- (:job/max-retries job) (util/job-ent->attempts-consumed job))
       :max_runtime (:job/max-runtime job Long/MAX_VALUE) ; consistent with input
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
            (:job/instance job))}
      (when groups
        {:groups (map #(str (:group/uuid %)) groups)}))))

(defn fetch-group-job-details
  [db guuid]
  (let [group (d/entity db [:group/uuid guuid])
        jobs (:group/job group)
        jobs-by-state (group-by :job/state jobs)]
          {:waiting (count (:job.state/waiting jobs-by-state))
           :running (count (:job.state/running jobs-by-state))
           :completed (count (:job.state/completed jobs-by-state))}))

(defn fetch-group-map
  [db guuid]
  (let [group (d/entity db [:group/uuid guuid])
        jobs (:group/job group)]
    (-> (into {} group)
        ;; Remove job because we don't want to walk entire job list
        (dissoc :group/job)
        util/remove-datomic-namespacing
        (assoc :jobs (map :job/uuid jobs))
        (update-in [:host-placement :type] (comp util/without-ns :name))
        (update-in [:straggler-handling :type] util/without-ns))))

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
          used? (partial job-exists? (db conn))]
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

(defn vectorize
  "If x is not a vector (or nil), turns it into a vector"
  [x]
  (if (or (nil? x) (vector? x))
      x
      [x]))

;;TODO (Diego): add docs for this
(def cook-coercer
  (constantly
    (-> c-mw/default-coercion-matchers
      (update :body
              (fn [their-matchers]
                (fn [s]
                  (or (their-matchers s)
                      (get {;; can't use form->kebab-case because env and label
                            ;; accept arbitrary kvs
                            JobRequest (partial map-keys ->kebab-case)
                            Group (partial map-keys ->kebab-case)
                            HostPlacement (fn [hp]
                                            (update hp :type keyword))
                            StragglerHandling (fn [sh]
                                                (update sh :type keyword))}
                           s)))))
      (update :response
              (fn [their-matchers]
                (fn [s]
                  (or (their-matchers s)
                      (get {JobResponse (partial map-keys ->snake_case)
                            GroupResponse (partial map-keys ->snake_case)
                            s/Uuid str}
                           s))))))))

(def override-group-str "override-group-immutability")

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
                        jobs (get params :jobs)
                        groups (get params :groups)
                        user (get-in ctx [:request :authorization/user])
                        override-group-immutability? (boolean (get params override-group-str))]
                    (try
                      (cond
                        (empty? params)
                        [true {::error (str "Must supply at least one job or group to start."
                                            "Are you specifying that this is application/json?")}]
                        :else
                        (let [groups (mapv #(validate-and-munge-group (db conn) %) groups)
                              jobs (mapv #(validate-and-munge-job
                                            (db conn)
                                            user
                                            task-constraints
                                            gpu-enabled?
                                            (set (map :uuid groups))
                                            %
                                            :override-group-immutability?
                                            override-group-immutability?) jobs)]
                          [false {::groups groups ::jobs jobs}]))
                      (catch Exception e
                        (log/warn e "Malformed raw api request")
                        [true {::error (.getMessage e)}]))))
    :processable? (fn [ctx]
                    (try
                      (log/info "Submitting jobs through raw api:" (::jobs ctx))
                      (let [jobs (::jobs ctx)
                            groups (::groups ctx)
                            group-uuids (set (map :uuid groups))
                            ; Create new implicit groups (with all default settings)
                            implicit-groups (->> jobs
                                                 (map :group)
                                                 (remove nil?)
                                                 distinct
                                                 (remove #(contains? group-uuids %))
                                                 (map make-default-group))
                            groups (concat implicit-groups (::groups ctx))
                            job-txns (mapcat #(make-job-txn %) jobs)
                            job-uuids->dbids (->> job-txns
                                                  ;; Not all txns are for the top level job
                                                  (filter :job/uuid)
                                                  (map (juxt :job/uuid :db/id))
                                                  (into {}))
                            group-uuid->job-dbids (->> jobs
                                                        (group-by :group)
                                                        (map-vals (fn [jobs]
                                                                    (map #(job-uuids->dbids (:uuid %))
                                                                         jobs))))
                            group-txns (mapcat
                                         #(make-group-txn % (get group-uuid->job-dbids (:uuid %) []))
                                               groups)]
                        @(d/transact
                         conn
                         (concat job-txns group-txns)))
                      true
                      (catch Exception e
                        (log/error e "Error submitting jobs through raw api")
                        [false {::error (str e)}])))
    :post! (fn [ctx]
             ;; We did the actual logic in processable?, so there's nothing left to do
             {::results (str/join \space (concat ["submitted jobs"]
                                                 (map (comp str :uuid) (::jobs ctx))
                                                 (if (not (empty? (::groups ctx)))
                                                   (concat ["submitted groups"]
                                                           (map (comp str :uuid) (::groups ctx))))))})

    :handle-created (fn [ctx] (::results ctx))}))


(defn read-groups-handler
  [conn fid task-constraints is-authorized-fn]
  (base-cook-handler
    {:allowed-methods [:get]
     :malformed? (fn [ctx]
                   (try
                     (let [requested-guuids (->> (get-in ctx [:request :query-params "uuid"])
                                                 vectorize
                                                 (mapv #(UUID/fromString %)))
                           not-found-guuids (remove #(group-exists? (db conn) %) requested-guuids)]
                       (if (empty? not-found-guuids)
                         [false {::guuids requested-guuids}]
                         [true {::error (str "UUID "
                                                 (str/join
                                                   \space
                                                   not-found-guuids)
                                                 " didn't correspond to a group")}]))
                     (catch Exception e
                       [true {::error e}])))
     :allowed? (fn [ctx]
                 (let [user (get-in ctx [:request :authorization/user])
                       guuids (::guuids ctx)
                       group-user (fn [guuid] (-> (d/entity (db conn) [:group/uuid guuid])
                                                  :group/job first :job/user))
                       authorized? (fn [guuid] (is-authorized-fn user
                                                                 (get-in ctx [:request :request-method])
                                                                 {:owner (group-user guuid) :item :job}))
                       unauthorized-guuids (mapv :uuid (remove authorized? guuids))]
                   (if (empty? unauthorized-guuids)
                     true
                     [false {::error (str "You are not authorized to view access the following groups "
                                          (str/join \space unauthorized-guuids))}]
                     )))
     :handle-ok (fn [ctx]
                  (if (get-in ctx [:request :query-params "detailed"])
                    (mapv #(merge (fetch-group-map (db conn) %)
                                  (fetch-group-job-details (db conn) %))
                          (::guuids ctx))
                    (mapv #(fetch-group-map (db conn) %) (::guuids ctx))))}))

;;
;; /queue

(defn waiting-jobs
  [mesos-pending-jobs-fn is-authorized-fn]
  (-> (liberator/resource
        :available-media-types ["application/json"]
        :allowed-methods [:get]
        :malformed? (fn [ctx]
                      (try
                        (if-let [limit (Integer/parseInt (get-in ctx [:request :query-params "limit"] "1000"))]
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
                        (if-let [limit (Integer/parseInt (get-in ctx [:request :query-params "limit"] "1000"))]
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
    (if (job-exists? (d/db conn) job)
      [true {::job job}]
      [false {::error (str "UUID " job " does not correspond to a job" )}])))

(defn validate-retries
  [conn task-constraints ctx]
  (let [retries (or (get-in ctx [:request :query-params :retries])
                    (get-in ctx [:request :body-params :retries]))]
    (cond
      (> retries (:retry-limit task-constraints))
      [true {::error (str "'retries' exceeds the maximum retry limit of " (:retry-limit task-constraints))}]
      :else
      [false {::retries retries}])))

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
  [conn is-authorized-fn task-constraints]
  (base-retries-handler
   conn is-authorized-fn
   {:allowed-methods [:post]
    ;; we need to check if it's a valid job UUID in malformed? here,
    ;; because this endpoint currently isn't restful (POST used for what is
    ;; actually an idempotent update; it should be PUT).
    :exists? (partial check-job-exists conn)
    :malformed? (partial validate-retries conn task-constraints)
    :handle-created (partial display-retries conn)
    :post! (fn [ctx]
             (util/retry-job! conn (::job ctx) (::retries ctx)))}))

(defn put-retries-handler
  [conn is-authorized-fn task-constraints]
  (base-retries-handler
   conn is-authorized-fn
   {:allowed-methods [:put]
    :exists? (partial check-job-exists conn)
    :malformed? (partial validate-retries conn task-constraints)
    :handle-created (partial display-retries conn)
    :put! (fn [ctx]
            (util/retry-job! conn (::job ctx) (ctx ::retries)))}))

;; /share and /quota
(def UserParam {:user s/Str})

(def UserLimitsResponse
  {String s/Num})

(defn set-limit-params
  [limit-type]
  {:body-params (merge UserParam {limit-type {s/Keyword s/Num}})})

(defn retrieve-user-limit
  [get-limit-fn conn ctx]
  (->> (or (get-in ctx [:request :query-params :user])
           (get-in ctx [:request :body-params :user]))
       (get-limit-fn (db conn))
       walk/stringify-keys))

(defn check-limit-allowed
  [limit-type is-authorized-fn ctx]
  (let [request-user (get-in ctx [:request :authorization/user])
        request-method (get-in ctx [:request :request-method])]
    (if-not (is-authorized-fn request-user request-method {:owner ::system :item limit-type})
      [false {::error (str "You are not authorized to access " (name limit-type) " information")}]
      true)))

(defn base-limit-handler
  [limit-type is-authorized-fn resource-attrs]
  (base-cook-handler
   (merge {:allowed? (partial check-limit-allowed limit-type is-authorized-fn)}
          resource-attrs)))

(defn read-limit-handler
  [limit-type get-limit-fn conn is-authorized-fn]
  (base-limit-handler
   limit-type is-authorized-fn
   {:handle-ok (partial retrieve-user-limit get-limit-fn conn)}))

(defn destroy-limit-handler
  [limit-type retract-limit-fn conn is-authorized-fn]
  (base-limit-handler
   limit-type is-authorized-fn
   {:allowed-methods [:delete]
    :delete!  (fn [ctx]
                (retract-limit-fn conn (get-in ctx [:request :query-params :user])))}))

(defn coerce-limit-values
  [limits-from-request]
  (into {} (map (fn [[k v]] [k (if (= k :count) (int v) (double v))])
                limits-from-request)))

(defn update-limit-handler
  [limit-type extra-resource-types get-limit-fn set-limit-fn conn is-authorized-fn]
  (base-limit-handler
   limit-type is-authorized-fn
   {:allowed-methods [:post]
    :handle-created (partial retrieve-user-limit get-limit-fn conn)
    :malformed? (fn [ctx]
                  (let [resource-types
                        (set (apply conj (util/get-all-resource-types (d/db conn))
                                    extra-resource-types))
                        limits (->> (get-in ctx [:request :body-params limit-type])
                                    keywordize-keys
                                    coerce-limit-values)]
                    (cond
                      (not (seq limits))
                      [true {::error "No " (name limit-type) " set. Are you specifying that this is application/json?"}]
                      (not (every? (partial contains? resource-types) (keys limits)))
                      [true {::error (str "Unknown resource type(s)" (str/join \space (remove (partial contains? resource-types) (keys limits))))}]
                      :else
                      [false {::limits limits}])))

    :post! (fn [ctx]
             (apply set-limit-fn conn (get-in ctx [:request :body-params :user]) (reduce into [] (::limits ctx))))}))

(defn- str->state-attr
  [state-str]
  (when (contains? #{"running" "waiting" "completed"} state-str)
    (keyword (format "job.state/%s" state-str))))

(defn- parse-int-default
  [s d]
  (if (nil? s)
    d
    (Integer/parseInt s)))

(defn- parse-long-default
  [s d]
  (if (nil? s)
    d
    (Long/parseLong s)))

(timers/deftimer [cook-scheduler handler fetch-jobs])
(timers/deftimer [cook-scheduler handler list-endpoint])

(defn list-resource
  [db framework-id is-authorized-fn]
  (-> (liberator/resource
        :available-media-types ["application/json"]
        :allowed-methods [:get]
        :malformed? (fn [ctx]
                      ;; since-hours-ago is included for backwards compatibility but is deprecated
                      ;; please use start-ms and end-ms instead
                      (let [{:keys [state user since-hours-ago start-ms end-ms limit]
                             :as params}
                              (keywordize-keys (or (get-in ctx [:request :query-params])
                                                   (get-in ctx [:request :body-params])))]
                        (if (and state user)
                          (let [state-strs (clojure.string/split state #"\+")
                                states (->> state-strs
                                            (map str->state-attr)
                                            (filter (comp not nil?)))]
                            (if (= (count state-strs) (count states))
                              (try
                                [false {::states states
                                        ::user user
                                        ::since-hours-ago (parse-int-default since-hours-ago 24)
                                        ::start-ms (parse-long-default start-ms nil)
                                        ::limit (parse-int-default limit Integer/MAX_VALUE)
                                        ::end-ms (parse-long-default end-ms (System/currentTimeMillis))}]
                                (catch NumberFormatException e
                                  [true {::error (.toString e)}]))
                              [true {::error (str "unsupported state in " state ", must be running, waiting, or completed")}]))
                          [true {::error "must supply the state and the user name"}])))
        :allowed? (fn [ctx]
                    (let [{limit ::limit
                           user ::user
                           since-hours-ago ::since-hours-ago
                           start-ms ::start-ms
                           end-ms ::end-ms} ctx
                          request-user (get-in ctx [:request :authorization/user])]
                      (cond
                        (not (is-authorized-fn request-user :get {:owner user :item :job}))
                        [false {::error (str "You are not authorized to list jobs for " user)}]
                        (or (< 168 since-hours-ago)
                            (> 0 since-hours-ago))
                        [false {::error (str "since-hours-ago must be between 0 and 168 (7 days)")}]
                        (> 1 limit)
                        [false {::error (str "limit must be positive")}]
                        (and start-ms (> start-ms end-ms))
                        [false {::error (str "start-ms (" start-ms ") must be before end-ms (" end-ms ")")}]
                        :else true)))
        :handle-malformed ::error
        :handle-forbidden ::error
        :handle-ok (fn [ctx]
                     (timers/time!
                       list-endpoint
                       (let [{states ::states
                              user ::user
                              start-ms ::start-ms
                              end-ms ::end-ms
                              since-hours-ago ::since-hours-ago
                              limit ::limit} ctx
                             start (if start-ms
                                     (java.util.Date. start-ms)
                                     (java.util.Date. (- end-ms (-> since-hours-ago t/hours t/in-millis))))
                             end (java.util.Date. end-ms)
                             job-uuids (->> (timers/time!
                                              fetch-jobs
                                              ;; Get valid timings
                                              (doall
                                                (mapcat #(util/get-jobs-by-user-and-state db
                                                                                          user
                                                                                          %
                                                                                          start
                                                                                          end)
                                                        states)))
                                            (sort-by :job/submit-time)
                                            reverse
                                            (map :job/uuid))
                             job-uuids (if (nil? limit)
                                         job-uuids
                                         (take limit job-uuids))]
                         (mapv (partial fetch-job-map db framework-id) job-uuids)))))
      ring.middleware.json/wrap-json-params))

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
                      :tags [{:name "api", :description "some apis"}]}}
     :coercion cook-coercer}

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
              :parameters {:body-params RawSchedulerRequest}
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
             :handler (read-limit-handler :share share/get-share conn is-authorized-fn)}
       :post {:summary "Change a user's share"
              :parameters (set-limit-params :share)
              :handler (update-limit-handler :share []
                                             share/get-share share/set-share!
                                             conn is-authorized-fn)}
       :delete {:summary "Reset a user's share to the default"
                :parameters {:query-params UserParam}
                :handler (destroy-limit-handler :share share/retract-share! conn is-authorized-fn)}}))

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
             :handler (read-limit-handler :quota quota/get-quota conn is-authorized-fn)}
       :post {:summary "Change a user's quota"
              :parameters (set-limit-params :quota)
              :handler (update-limit-handler :quota [:count]
                                             quota/get-quota quota/set-quota!
                                             conn is-authorized-fn)}
       :delete {:summary "Reset a user's quota to the default"
                :parameters {:query-params UserParam}
                :handler (destroy-limit-handler :delete quota/retract-quota! conn is-authorized-fn)}}))

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
        :handler (put-retries-handler conn is-authorized-fn task-constraints)
        :responses {201 {:schema PosInt
                         :description "The number of retries for the job"}
                    400 {:description "Invalid request format."}
                    401 {:description "Request user not authorized to access that job."}
                    404 {:description "Unrecognized job UUID."}}}
       :post
       {:summary "Change a job's retry count (deprecated)"
        :parameters {:body-params UpdateRetriesRequest}
        :handler (post-retries-handler conn is-authorized-fn task-constraints)
        :responses {201 {:schema PosInt
                         :description "The number of retries for the job"}
                    400 {:description "Invalid request format or bad job UUID."}
                    401 {:description "Request user not authorized to access that job."}}}}))
     (c-api/context
     "/group" []
     (c-api/resource
      {:get {:summary "Returns info about a set of groups"
             :parameters {:query-params {:uuid [s/Uuid] (s/optional-key :detailed) s/Bool}}
             :responses {200 {:schema [GroupResponse]
                              :description "The groups were returned."}
                         400 {:description "Non-UUID values were passed."}
                         403 {:description "The supplied UUIDs don't correspond to valid groups."}}
             :handler (read-groups-handler conn fid task-constraints is-authorized-fn)}})))
    (ANY "/queue" []
         (waiting-jobs mesos-pending-jobs-fn is-authorized-fn))
    (ANY "/running" []
         (running-jobs conn is-authorized-fn))
    (ANY "/list" []
         (list-resource (db conn) fid is-authorized-fn))))
