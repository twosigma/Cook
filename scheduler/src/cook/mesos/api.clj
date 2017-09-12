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
  (:require [camel-snake-kebab.core :refer [->snake_case ->kebab-case]]
            [cheshire.core :as cheshire]
            [clj-http.client :as http]
            [clj-time.core :as t]
            [clojure.core.cache :as cache]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk :refer (keywordize-keys)]
            [compojure.api.middleware :as c-mw]
            [compojure.api.sweet :as c-api]
            [compojure.core :refer (ANY GET POST routes)]
            [cook.mesos.quota :as quota]
            [cook.mesos.reason :as reason]
            [cook.mesos.schema :refer (constraint-operators host-placement-types straggler-handling-types)]
            [cook.mesos.share :as share]
            [cook.mesos.unscheduled :as unscheduled]
            [cook.mesos.util :as util]
            [cook.mesos]
            [datomic.api :as d :refer (q)]
            [liberator.core :as liberator]
            [liberator.util :refer [combine]]
            [me.raynes.conch :as sh]
            [mesomatic.scheduler]
            [metatransaction.core :refer (db)]
            [metrics.timers :as timers]
            [plumbing.core :refer [map-from-vals map-keys map-vals mapply]]
            [ring.middleware.format-params :as format-params]
            [ring.util.response :as res]
            [schema.core :as s]
            [swiss.arrows :refer :all])
  (:import (clojure.lang Atom Var)
           com.codahale.metrics.riemann.RiemannReporter
           (java.io OutputStreamWriter)
           (java.net ServerSocket URLEncoder)
           (java.util Date UUID)
           javax.servlet.ServletResponse
           org.apache.curator.test.TestingServer
           org.joda.time.Minutes
           schema.core.OptionalKey))

;; This is necessary to prevent a user from requesting a uid:gid
;; pair other than their own (for example, root)
(sh/let-programs
  [_id "/usr/bin/id"]
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
              (if (instance? OptionalKey k)
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

(def UserName
  (s/both s/Str (s/pred #(re-matches #"\A[a-z][a-z0-9_-]{0,62}[a-z0-9]\z" %) 'lowercase-alphanum?)))

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
   (s/optional-key :mesos_start_time) s/Int
   (s/optional-key :end_time) s/Int
   (s/optional-key :reason_code) s/Int
   (s/optional-key :output_url) s/Str
   (s/optional-key :cancelled) s/Bool
   (s/optional-key :reason_string) s/Str
   (s/optional-key :exit_code) s/Int
   (s/optional-key :progress) s/Int
   (s/optional-key :progress_message) s/Str
   (s/optional-key :sandbox_directory) s/Str})

(defn max-128-characters-and-alphanum?
  "Returns true if s contains only '.', '_', '-' or
  any word characters and has length at most 128"
  [s]
  (re-matches #"[\.a-zA-Z0-9_-]{0,128}" s))

(defn non-empty-max-128-characters-and-alphanum?
  "Returns true if s contains only '.', '_', '-' or any
  word characters and has length at least 1 and most 128"
  [s]
  (re-matches #"[\.a-zA-Z0-9_-]{1,128}" s))

(def Application
  "Schema for the application a job corresponds to"
  {:name (s/constrained s/Str non-empty-max-128-characters-and-alphanum?)
   :version (s/constrained s/Str non-empty-max-128-characters-and-alphanum?)})

(def Constraint
  "Schema for user defined job host constraint"
  [(s/one NonEmptyString "attribute")
   (s/one (s/pred #(contains? (set (map name constraint-operators))
                              (str/lower-case %))
                  'constraint-operator-exists?)
          "operator")
   (s/one NonEmptyString "pattern")])

(s/defschema JobName
  (s/both s/Str (s/both s/Str (s/pred max-128-characters-and-alphanum? 'max-128-characters-and-alphanum?))))

(s/defschema JobNameListFilter
  (s/both s/Str (s/pred (fn [s]
                          (re-matches #"[\.a-zA-Z0-9_\-\*]*" s)))))

(s/defschema JobPriority
  (s/both s/Int (s/pred #(<= 0 % 100) 'between-0-and-100)))

(defn valid-runtimes?
  "Returns false if the expected-runtime of the given job
  is greater than the max-runtime; otherwise, returns true"
  [{:keys [expected-runtime max-runtime]}]
  (if (and expected-runtime max-runtime)
    (<= expected-runtime max-runtime)
    true))

(def JobMap
  "Schema for the fields of a job"
  {:uuid s/Uuid
   :command s/Str
   :name JobName
   :priority JobPriority
   :max-retries PosInt
   :max-runtime PosInt
   (s/optional-key :uris) [Uri]
   (s/optional-key :ports) (s/pred #(not (neg? %)) 'nonnegative?)
   (s/optional-key :env) {NonEmptyString s/Str}
   (s/optional-key :labels) {NonEmptyString s/Str}
   (s/optional-key :constraints) [Constraint]
   (s/optional-key :container) Container
   (s/optional-key :group) s/Uuid
   (s/optional-key :disable-mea-culpa-retries) s/Bool
   :cpus PosDouble
   :mem PosDouble
   (s/optional-key :gpus) (s/both s/Int (s/pred pos? 'pos?))
   ;; Make sure the user name is valid. It must begin with a lower case character, end with
   ;; a lower case character or a digit, and has length between 2 to (62 + 2).
   :user UserName
   (s/optional-key :application) Application
   (s/optional-key :expected-runtime) PosInt})

(def Job
  "Full schema for a job"
  (s/constrained JobMap valid-runtimes?))

(def JobRequestMap
  "Schema for the fields of a job request"
  (-> JobMap
      (dissoc :max-runtime :name :priority :user)
      (assoc :cpus PosNum
             :mem PosNum
             (s/optional-key :env) {s/Keyword s/Str}
             (s/optional-key :labels) {s/Keyword s/Str}
             (s/optional-key :max-runtime) PosInt
             (s/optional-key :name) JobName
             (s/optional-key :priority) JobPriority
             (s/optional-key :uris) [UriRequest])))

(def JobRequest
  "Schema for the part of a request that launches a single job."
  (s/constrained JobRequestMap valid-runtimes?))

(def JobResponse
  "Schema for a description of a job (as returned by the API).
  The structure is similar to JobRequest, but has some differences.
  For example, it can include descriptions of instances for the job."
  (-> JobRequestMap
      (dissoc (s/optional-key :group))
      (merge {:framework-id (s/maybe s/Str)
              :retries-remaining NonNegInt
              :status s/Str
              :state s/Str
              :submit-time (s/maybe PosInt)
              :user UserName
              (s/optional-key :gpus) s/Int
              (s/optional-key :groups) [s/Uuid]
              (s/optional-key :instances) [Instance]})
      prepare-schema-response))

(def JobOrInstanceIds
  "Schema for any number of job and/or instance uuids"
  {(s/optional-key :job) [s/Uuid]
   (s/optional-key :instance) [s/Uuid]})

(def QueryJobsParams
  "Schema for querying for jobs by job and/or instance uuid, allowing optionally
  for 'partial' results, meaning that some uuids can be valid and others not"
  (-> JobOrInstanceIds
      (assoc (s/optional-key :partial) s/Bool)))

(def Attribute-Equals-Parameters
  "A schema for the parameters of a host placement with type attribute-equals"
  {:attribute s/Str})

(def Balanced-Parameters
  "A schema for the parameters of a host placement with type balanced"
  {:attribute s/Str
   :minimum PosInt})

(def HostPlacement
  "A schema for host placement"
  (s/conditional
    #(= (:type %) :attribute-equals)
    {:type (s/eq :attribute-equals)
     :parameters Attribute-Equals-Parameters}
    #(= (:type %) :balanced)
    {:type (s/eq :balanced)
     :parameters Balanced-Parameters}
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

(def QueryGroupsParams
  "Schema for querying for groups, allowing optionally for 'partial'
  results, meaning that some uuids can be valid and others not"
  {:uuid [s/Uuid]
   (s/optional-key :detailed) s/Bool
   (s/optional-key :partial) s/Bool})

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
   (s/optional-key :override-group-immutability) s/Bool
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
                 :docker/force-pull-image (:force-pull-image docker false)}
                (when (:network docker) {:docker/network (:network docker)})
                (mk-container-params docker-id params)
                (mk-docker-ports docker-id port-mappings))])
      {})))

(s/defn make-job-txn
  "Creates the necessary txn data to insert a job into the database"
  [job :- Job]
  (let [{:keys [uuid command max-retries max-runtime expected-runtime priority cpus mem gpus
                user name ports uris env labels container group application disable-mea-culpa-retries
                constraints]
         :or {group nil
              disable-mea-culpa-retries false}} job
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
        constraints (mapcat (fn [[attribute operator pattern]]
                              (let [constraint-var-id (d/tempid :db.part/user)]
                                [[:db/add db-id :job/constraint constraint-var-id]
                                 {:db/id constraint-var-id
                                  :constraint/attribute attribute
                                  :constraint/operator (keyword "constraint.operator"
                                                                (str/lower-case operator))
                                  :constraint/pattern pattern}]))
                            constraints)
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
                      :commit-latch/committed? true
                      :commit-latch/uuid (UUID/randomUUID)}
        txn (cond-> {:db/id db-id
                     :job/command command
                     :job/commit-latch commit-latch-id
                     :job/custom-executor false
                     :job/disable-mea-culpa-retries disable-mea-culpa-retries
                     :job/max-retries max-retries
                     :job/name (or name "cookjob") ; set the default job name if not provided.
                     :job/resource [{:resource/type :resource.type/cpus
                                     :resource/amount cpus}
                                    {:resource/type :resource.type/mem
                                     :resource/amount mem}]
                     :job/state :job.state/waiting
                     :job/submit-time (Date.)
                     :job/user user
                     :job/uuid uuid}
                    application (assoc :job/application
                                       {:application/name (:name application)
                                        :application/version (:version application)})
                    expected-runtime (assoc :job/expected-runtime expected-runtime))]

    ;; TODO batch these transactions to improve performance
    (-> ports
        (into uris)
        (into env)
        (into constraints)
        (into labels)
        (into container)
        (into maybe-datoms)
        (conj txn)
        (conj commit-latch))))

(defn make-type-parameter-txn
  "Makes txn map for an entity conforming to the pattern of
   {:<namespace>/type <type>
    :<namespace>/parameters {<parameters>}}
   The entity must be marked with isComponent true

   Example:
   (make-type-parameter-txn {:type :quantile-deviation
                             :parameters {:quantile 0.5 :multiplier 2.5}}
                             :straggler-handling)
   {:straggler-handling/type :straggler-handling.type/quantile-deviation
    :straggler-handling/parameters {:straggler-handling.quantile-deviation/quantile 0.5
                                    :straggler-handling.quantile-deviation/multiplier 2.5}}"
  [{:keys [type parameters]} name-space]
  (let [namespace-fn (partial util/namespace-datomic name-space)]
    (merge
      {(namespace-fn :type) (namespace-fn :type type)}
      (when (seq parameters)
        {(namespace-fn :parameters) (map-keys (partial namespace-fn type)
                                              parameters)}))))

(s/defn make-group-txn
  "Creates the transaction data necessary to insert a group to the database. job-db-ids is the
   list of all db-ids (datomic temporary ids) of jobs that belong to this group"
  [group :- Group job-db-ids]
  {:db/id (d/tempid :db.part/user)
   :group/uuid (:uuid group)
   :group/name (:name group)
   :group/job job-db-ids
   :group/host-placement (-> group
                             :host-placement
                             (make-type-parameter-txn :host-placement))
   :group/straggler-handling (-> group
                                 :straggler-handling
                                 (make-type-parameter-txn :straggler-handling))})

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
   {:keys [cpus mem gpus uuid command priority max-retries max-runtime expected-runtime name
           uris ports env labels container group application disable-mea-culpa-retries
           constraints]
    :or {group nil
         disable-mea-culpa-retries false}
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
                  :mem (double mem)
                  :disable-mea-culpa-retries disable-mea-culpa-retries}
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
                 (when constraints
                   ;; Rest framework keywordifies all keys, we want these to be strings!
                   {:constraints constraints})
                 (when group-uuid
                   {:group group-uuid})
                 (when container
                   {:container container})
                 (when expected-runtime
                   {:expected-runtime expected-runtime})
                 (when application
                   {:application application}))]
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
    (when (and (:retry-limit task-constraints)
               (> max-retries (:retry-limit task-constraints)))
      (throw (ex-info (str "Requested " max-retries " exceeds the maximum retry limit")
                      {:constraints task-constraints
                       :job job})))
    (doseq [{:keys [executable? extract?] :as uri} (:uris munged)
            :when (and (not (nil? executable?)) (not (nil? extract?)))]
      (throw (ex-info "Uri cannot set executable and extract" uri)))
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

(defn get-executor-id->sandbox-directory-impl
  "Builds an indexed version of all executor-id to sandbox directory on the specified agent. Has no cache; takes
   100-500ms to run."
  [framework-id agent-hostname]
  (let [timeout-millis (* 5 1000)
        ;; Throw SocketTimeoutException or ConnectionTimeoutException when timeout
        {:strs [completed_frameworks frameworks]} (-> (str "http://" agent-hostname ":5051/state.json")
                                                      (http/get
                                                        {:as :json-string-keys
                                                         :conn-timeout timeout-millis
                                                         :socket-timeout timeout-millis
                                                         :spnego-auth true})
                                                      :body)
        framework-filter (fn framework-filter [{:strs [id] :as framework}]
                           (when (= framework-id id) framework))
        ;; there should be at most one framework entry for a given framework-id
        target-framework (or (some framework-filter frameworks)
                             (some framework-filter completed_frameworks))
        {:strs [completed_executors executors]} target-framework
        framework-executors (reduce into [] [completed_executors executors])]
    (->> framework-executors
         (map (fn executor-state->executor-id->sandbox-directory [{:strs [id directory]}]
                [id directory]))
         (into {}))))

(defn get-executor-id->sandbox-directory-cache-entry
  "Builds an indexed version of all executor-id to sandbox directory for the specified agent. Cached"
  [framework-id agent-hostname cache]
  (let [run (delay (try (get-executor-id->sandbox-directory-impl framework-id agent-hostname)
                        (catch Exception e
                          (log/debug e "Failed to get executor state, purging from cache...")
                          (swap! cache cache/evict agent-hostname)
                          nil)))
        cs (swap! cache (fn [c]
                          (if (cache/has? c agent-hostname)
                            (cache/hit c agent-hostname)
                            (cache/miss c agent-hostname run))))
        val (cache/lookup cs agent-hostname)]
    (if val @val @run)))

(defn retrieve-url-path
  "Constructs a URL to query the sandbox directory of the task.
   Uses either the provided sandbox-directory or takes the executor-id->sandbox-directory from the agent json to
   determine the sandbox directory.
   sandbox-directory will be populated in the instance by the cook executor, else it may be nil when the query to
   the agent needs to me made.
   Hardcodes fun stuff like the port we run the agent on. Users will need to add the file path & offset to their query.
   Refer to the 'Using the output_url' section in docs/scheduler-rest-api.asc for further details."
  [framework-id agent-hostname executor-id agent-query-cache sandbox-directory]
  (try
    (when-let [directory (or sandbox-directory
                             (get (get-executor-id->sandbox-directory-cache-entry framework-id agent-hostname agent-query-cache)
                                  executor-id))]
      (str "http://" agent-hostname ":5051" "/files/read.json?path="
           (URLEncoder/encode directory "UTF-8")))
    (catch Exception e
      (log/debug e "Unable to retrieve directory path for" executor-id "on agent" agent-hostname)
      nil)))

(defn fetch-instance-map
  "Converts the instance entity to a map representing the instance fields."
  [db framework-id agent-query-cache instance]
  (let [hostname (:instance/hostname instance)
        executor-id (:instance/executor-id instance)
        sandbox-directory (:instance/sandbox-directory instance)
        url-path (retrieve-url-path framework-id hostname executor-id agent-query-cache sandbox-directory)
        start (:instance/start-time instance)
        mesos-start (:instance/mesos-start-time instance)
        end (:instance/end-time instance)
        cancelled (:instance/cancelled instance)
        reason (reason/instance-entity->reason-entity db instance)
        exit-code (:instance/exit-code instance)
        progress (:instance/progress instance)
        progress-message (:instance/progress-message instance)]
    (cond-> {:backfilled false ;; Backfill has been deprecated
             :executor_id executor-id
             :hostname hostname
             :ports (vec (:instance/ports instance))
             :preempted (:instance/preempted? instance false)
             :slave_id (:instance/slave-id instance)
             :status (name (:instance/status instance))
             :task_id (:instance/task-id instance)}
            start (assoc :start_time (.getTime start))
            mesos-start (assoc :mesos_start_time (.getTime mesos-start))
            end (assoc :end_time (.getTime end))
            cancelled (assoc :cancelled cancelled)
            exit-code (assoc :exit_code exit-code)
            url-path (assoc :output_url url-path)
            reason (assoc :reason_code (:reason/code reason)
                          :reason_string (:reason/string reason))
            progress (assoc :progress progress)
            progress-message (assoc :progress_message progress-message)
            sandbox-directory (assoc :sandbox_directory sandbox-directory))))

(defn fetch-job-map
  [db framework-id agent-query-cache job-uuid]
  (let [job (d/entity db [:job/uuid job-uuid])
        resources (util/job-ent->resources job)
        groups (:group/_job job)
        application (:job/application job)
        expected-runtime (:job/expected-runtime job)
        state (util/job-ent->state job)
        constraints (->> job
                         :job/constraint
                         (map util/remove-datomic-namespacing)
                         (map (fn [{:keys [attribute operator pattern]}]
                                (->> [attribute (str/upper-case (name operator)) pattern]
                                     (map str)))))
        instances (map #(fetch-instance-map db framework-id agent-query-cache %1) (:job/instance job))
        submit-time (when (:job/submit-time job) ; due to a bug, submit time may not exist for some jobs
                (.getTime (:job/submit-time job)))
        job-map {:command (:job/command job)
                 :constraints constraints
                 :cpus (:cpus resources)
                 :disable_mea_culpa_retries (:job/disable-mea-culpa-retries job false)
                 :env (util/job-ent->env job)
                 :framework_id framework-id
                 :gpus (int (:gpus resources 0))
                 :instances instances
                 :labels (util/job-ent->label job)
                 :max_retries (:job/max-retries job) ; consistent with input
                 :max_runtime (:job/max-runtime job Long/MAX_VALUE) ; consistent with input
                 :mem (:mem resources)
                 :name (:job/name job "cookjob")
                 :ports (:job/ports job 0)
                 :priority (:job/priority job util/default-job-priority)
                 :retries_remaining (- (:job/max-retries job) (util/job-ent->attempts-consumed db job))
                 :state state
                 :status (name (:job/state job))
                 :submit_time submit-time
                 :uris (:uris resources)
                 :user (:job/user job)
                 :uuid (:job/uuid job)}]
     (cond-> job-map
            groups (assoc :groups (map #(str (:group/uuid %)) groups))
            application (assoc :application (util/remove-datomic-namespacing application))
            expected-runtime (assoc :expected-runtime expected-runtime))))

(defn fetch-group-job-details
  [db guuid]
  (let [group (d/entity db [:group/uuid guuid])
        jobs (:group/job group)
        jobs-by-state (group-by :job/state jobs)]
    {:completed (count (:job.state/completed jobs-by-state))
     :running (count (:job.state/running jobs-by-state))
     :waiting (count (:job.state/waiting jobs-by-state))}))

(defn fetch-group-map
  [db guuid]
  (let [group (d/entity db [:group/uuid guuid])
        jobs (:group/job group)]
    (-> (into {} group)
        ;; Remove job because we don't want to walk entire job list
        (dissoc :group/job)
        util/remove-datomic-namespacing
        (assoc :jobs (map :job/uuid jobs))
        (update-in [:host-placement :type] util/without-ns)
        (update-in [:straggler-handling :type] util/without-ns))))

(defn instance-uuid->job-uuid
  "Queries for the job uuid from an instance uuid.
   Returns nil if the instance uuid doesn't correspond
   a job"
  [db instance-uuid]
  (->> (d/entity db [:instance/task-id (str instance-uuid)])
       :job/_instance
       :job/uuid))

(defn- wrap-seq
  "Returns:
   - [v] if v is not nil and not sequential
   - v otherwise"
  [v]
  (if (or (nil? v) (sequential? v))
    v
    [v]))

(defn retrieve-jobs
  "Returns a tuple that either has the shape:

    [true {::jobs ...
           ::jobs-requested ...
           ::instances-requested ...}]

  or:

    [false {::error ...}]

  Given a collection of job and/or instance uuids, attempts to return the
  corresponding set of existing job uuids. By default (or if the 'partial'
  query parameter is false), the function returns an ::error if any of the
  provided job or instance uuids cannot be found. If 'partial' is true, the
  function will return the subset of job uuids that were found, assuming at
  least one was found. This 'partial' flag allows a client to query a
  particular cook cluster for a set of uuids, where some of them may not belong
  to that cluster, and get back the data for those that do match."
  [conn ctx]
  (let [jobs (wrap-seq (get-in ctx [:request :query-params :job]))
        instances (wrap-seq (get-in ctx [:request :query-params :instance]))
        allow-partial-results (get-in ctx [:request :query-params :partial])]
    (let [instance-uuid->job-uuid #(instance-uuid->job-uuid (d/db conn) %)
          instance-jobs (mapv instance-uuid->job-uuid instances)
          exists? #(job-exists? (db conn) %)
          existing-jobs (filter exists? jobs)]
      (cond
        (and (= (count existing-jobs) (count jobs))
             (every? some? instance-jobs))
        [true {::jobs (into jobs instance-jobs)
               ::jobs-requested jobs
               ::instances-requested instances}]

        (and allow-partial-results
             (or (pos? (count existing-jobs))
                 (some some? instance-jobs)))
        [true {::jobs (into existing-jobs (filter some? instance-jobs))
               ::jobs-requested jobs
               ::instances-requested instances}]

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
                               (set/difference (set jobs) (set existing-jobs)))
                             " didn't correspond to a job")}]))))

(defn check-job-params-present
  [ctx]
  (let [jobs (get-in ctx [:request :query-params :job])
        instances (get-in ctx [:request :query-params :instance])]
    (if (or jobs instances)
      false
      [true {::error "must supply at least one job or instance query param"}])))

(defn user-authorized-for-job?
  [conn is-authorized-fn ctx job-uuid]
  (let [request-user (get-in ctx [:request :authorization/user])
        job-user (:job/user (d/entity (db conn) [:job/uuid job-uuid]))
        request-method (get-in ctx [:request :request-method])]
    (is-authorized-fn request-user request-method {:owner job-user :item :job})))

(defn job-request-allowed?
  [conn is-authorized-fn ctx]
  (let [uuids (or (::jobs ctx)
                  (::jobs (second (retrieve-jobs conn ctx))))
        authorized? (partial user-authorized-for-job? conn is-authorized-fn ctx)]
    (if (every? authorized? uuids)
      true
      [false {::error (str "You are not authorized to view access the following jobs "
                           (str/join \space (remove authorized? uuids)))}])))

(defn render-jobs-for-response
  [conn framework-id agent-query-cache ctx]
  (mapv (partial fetch-job-map (db conn) framework-id agent-query-cache) (::jobs ctx)))


;;; On GET; use repeated job argument
(defn read-jobs-handler
  [conn framework-id task-constraints gpu-enabled? is-authorized-fn agent-query-cache]
  (base-cook-handler
    {:allowed-methods [:get]
     :malformed? check-job-params-present
     :allowed? (partial job-request-allowed? conn is-authorized-fn)
     :exists? (partial retrieve-jobs conn)
     :handle-ok (partial render-jobs-for-response conn framework-id agent-query-cache)}))


;;; On DELETE; use repeated job argument
(defn destroy-jobs-handler
  [conn framework-id is-authorized-fn agent-query-cache]
  (base-cook-handler
    {:allowed-methods [:delete]
     :malformed? check-job-params-present
     :allowed? (partial job-request-allowed? conn is-authorized-fn)
     :exists? (partial retrieve-jobs conn)
     :delete! (fn [ctx]
                (cook.mesos/kill-job conn (::jobs-requested ctx))
                (cook.mesos/kill-instances conn (::instances-requested ctx)))
     :handle-ok (partial render-jobs-for-response conn framework-id agent-query-cache)}))

(defn vectorize
  "If x is not a vector (or nil), turns it into a vector"
  [x]
  (if (or (nil? x) (vector? x))
    x
    [x]))

(def cook-coercer
  "This coercer adds to compojure-api's default-coercion-matchers by
  converting keys from snake_case to kebab-case for requests and from
  kebab-case to snake_case for responses. For example, this allows clients to
  pass 'max_retries' when submitting a job, even though the job schema has
  :max-retries (with a dash instead of an underscore). For more information on
  coercion in compojure-api, see:

    https://github.com/metosin/compojure-api/wiki/Validation-and-coercion"
  (constantly
    (-> c-mw/default-coercion-matchers
      (update :body
              (fn [their-matchers]
                (fn [s]
                  (or (their-matchers s)
                      (get {;; can't use form->kebab-case because env and label
                            ;; accept arbitrary kvs
                            JobRequestMap (partial map-keys ->kebab-case)
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


(defn create-jobs!
  "Based on the context, persists the specified jobs, along with their groups,
  to Datomic.
  Preconditions:  The context must already have been populated with both
  ::jobs and ::groups, which specify the jobs and job groups."
  [conn {:keys [::groups ::jobs] :as ctx}]
  (try
    (log/info "Submitting jobs through raw api:" jobs)
    (let [group-uuids (set (map :uuid groups))
          group-asserts (map (fn [guuid] [:entity/ensure-not-exists [:group/uuid guuid]])
                             group-uuids)
          ;; Create new implicit groups (with all default settings)
          implicit-groups (->> jobs
                               (map :group)
                               (remove nil?)
                               distinct
                               (remove #(contains? group-uuids %))
                               (map make-default-group))
          groups (into (vec implicit-groups) groups)
          job-asserts (map (fn [j] [:entity/ensure-not-exists [:job/uuid (:uuid j)]]) jobs)
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
          group-txns (map #(make-group-txn % (get group-uuid->job-dbids
                                                  (:uuid %)
                                                  []))
                          groups)]
      @(d/transact
         conn
         (-> (vec group-asserts)
             (into job-asserts)
             (into job-txns)
             (into group-txns)))

      {::results (str/join
                   \space (concat ["submitted jobs"]
                                  (map (comp str :uuid) jobs)
                                  (if (not (empty? groups))
                                    (concat ["submitted groups"]
                                            (map (comp str :uuid) groups)))))})
    (catch Exception e
      (log/error e "Error submitting jobs through raw api")
      (throw e))))

(defn job-create-allowed?
  "Returns true if the user is authorized to :create jobs."
  [is-authorized-fn {:keys [request]}]
  (let [request-user (:authorization/user request)]
    (if (is-authorized-fn request-user :create {:owner request-user :item :job})
      true
      [false {::error "You are not authorized to create jobs"}])))

;;; On POST; JSON blob that looks like:
;;; {"jobs": [{"command": "echo hello world",
;;;            "uuid": "123898485298459823985",
;;;            "max_retries": 3
;;;            "cpus": 1.5,
;;;            "mem": 1000}]}
;;;
(defn create-jobs-handler
  [conn task-constraints gpu-enabled? is-authorized-fn]
  (base-cook-handler
    {:allowed-methods [:post]
     :malformed? (fn [ctx]
                   (let [params (get-in ctx [:request :body-params])
                         jobs (get params :jobs)
                         groups (get params :groups)
                         user (get-in ctx [:request :authorization/user])
                         override-group-immutability? (boolean (get params :override-group-immutability))]
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
     :allowed? (partial job-create-allowed? is-authorized-fn)
     :exists? (fn [ctx]
                (let [db (d/db conn)
                      existing (filter (partial job-exists? db) (map :uuid (::jobs ctx)))]
                  [(seq existing) {::existing existing}]))

     ;; To ensure compatibility with existing clients,
     ;; we need to return 409 (conflict) when a client POSTs a Job UUID that already exists.
     ;; Liberator normally only supports 409 responses to PUT requests, so we need to override
     ;; the specific decisions that will lead to a 409 response on a POST request as well.
     ;; (see https://clojure-liberator.github.io/liberator/tutorial/decision-graph.html)
     :post-to-existing? (fn [ctx] false)
     :put-to-existing? (fn [ctx] true)
     ;; conflict? will only be invoked if exists? was true, and if so, we always want
     ;; to indicate a conflict.
     :conflict? (fn [ctx] true)
     :handle-conflict (fn [ctx] {:error (str "The following job UUIDs were already used: "
                                             (str/join ", " (::existing ctx)))})
     ;; Implementing put! doesn't mean that PUT requests are actually supported
     ;; (see :allowed-methods above). It is simply what liberator eventually calls to persist
     ;; resource changes when conflict? has returned false.
     :put! (partial create-jobs! conn)

     :post! (partial create-jobs! conn)
     :handle-exception (fn [{:keys [exception]}]
                         {:error (str "Exception occurred while creating job - " (.getMessage exception))})
     :handle-created (fn [ctx] (::results ctx))}))

(defn retrieve-groups
  "Returns a tuple that either has the shape:

    [true {::guuids ...}]

  or:

    [false {::error ...}]

  Given a collection of group uuids, attempts to return the corresponding
  set of existing group uuids. By default (or if the 'partial' query
  parameter is false), the function returns an ::error if any of the
  provided group uuids cannot be found. If 'partial' is true, the function
  will return the subset of group uuids that were found, assuming at least
  one was found. This 'partial' flag allows a client to query a particular
  cook cluster for a set of uuids, where some of them may not belong to that
  cluster, and get back the data for those that do match."
  [conn ctx]
  (try
    (let [requested-guuids (->> (get-in ctx [:request :query-params "uuid"])
                                vectorize
                                (mapv #(UUID/fromString %)))
          allow-partial-results (get-in ctx [:request :query-params :partial])
          exists? #(group-exists? (db conn) %)
          existing-groups (filter exists? requested-guuids)]
      (if (or (= (count existing-groups) (count requested-guuids))
              (and allow-partial-results (pos? (count existing-groups))))
        [true {::guuids existing-groups}]
        [false {::error (str "UUID "
                             (str/join
                               \space
                               (set/difference (set requested-guuids) (set existing-groups)))
                             " didn't correspond to a group")}]))
    (catch Exception e
      [false {::error e}])))

(defn read-groups-handler
  [conn task-constraints is-authorized-fn]
  (base-cook-handler
    {:allowed-methods [:get]
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
                                          (str/join \space unauthorized-guuids))}])))
     :exists? (partial retrieve-groups conn)
     :handle-ok (fn [ctx]
                  (if (get-in ctx [:request :query-params "detailed"])
                    (mapv #(merge (fetch-group-map (db conn) %)
                                  (fetch-group-job-details (db conn) %))
                          (::guuids ctx))
                    (mapv #(fetch-group-map (db conn) %) (::guuids ctx))))}))

;;
;; /queue

(def leader-hostname-regex #"^([^#]*)#([0-9]*)#.*")

(defn waiting-jobs
  [mesos-pending-jobs-fn is-authorized-fn mesos-leadership-atom leader-selector]
  (liberator/resource
   :available-media-types ["application/json"]
   :allowed-methods [:get]
   :as-response (fn [data ctx] {:body data})
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
   :exists? (fn [_] [@mesos-leadership-atom {}])
   :existed? (fn [_] [true {}])
   :moved-temporarily? (fn [ctx]
                         (if @mesos-leadership-atom
                           [false {}]
                           (let [leader-id (-> leader-selector .getLeader .getId)
                                 leader-match (re-matcher leader-hostname-regex leader-id)]
                             (if (.matches leader-match)
                               (let [leader-hostname (.group leader-match 1)
                                     leader-port (.group leader-match 2)]
                                 [true {:location (str "http://" leader-hostname ":" leader-port "/queue")}])
                               (throw (IllegalStateException.
                                       (str "Unable to parse leader id: " leader-id)))))))
   :handle-forbidden (fn [ctx]
                       (log/info (get-in ctx [:request :authorization/user]) " is not authorized to access queue")
                       (render-error ctx))
   :handle-malformed render-error
   :handle-ok (fn [ctx]
                (map-vals (fn [queue]
                            (take (::limit ctx) queue))
                          (mesos-pending-jobs-fn)))))

;;
;; /running
;;

(defn running-jobs
  [conn is-authorized-fn]
  (liberator/resource
   :available-media-types ["application/json"]
   :allowed-methods [:get]
   :as-response (fn [data ctx] {:body data})
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
                     (map d/touch)))))

;;
;; /retry
;;

(def ReadRetriesRequest
  {:job [s/Uuid]})

(def UpdateRetriesRequest
  {(s/optional-key :job) s/Uuid
   (s/optional-key :jobs) [s/Uuid]
   (s/optional-key :retries) PosNum
   (s/optional-key :increment) PosNum})

(defn display-retries
  [conn ctx]
  (:job/max-retries (d/entity (db conn) [:job/uuid (-> ctx ::jobs first)])))

(defn jobs-from-request
  "Reads a set of job UUIDs from the request, supporting \"job\" in the query,
  and either \"job\" or \"jobs\" in the body, while accommodating some aspects of
  liberator and compojure-api. For example, compojure-api doesn't support
  applying a schema to a combination of query parameters and body parameters."
  [ctx]
  (or (vectorize (get-in ctx [:request :query-params :job]))
      ;; if the query params are coerceable, both :job and "job" keys will be
      ;; present, but in that case we only want to read the coerced version
      (when-let [uncoerced-query-params (get-in ctx [:request :query-params "job"])]
        (mapv #(UUID/fromString %) (vectorize uncoerced-query-params)))
      (get-in ctx [:request :body-params :jobs])
      [(get-in ctx [:request :body-params :job])]))

(defn check-jobs-exist
  [conn ctx]
  (let [jobs (jobs-from-request ctx)
        jobs-not-existing (remove (partial job-exists? (d/db conn)) jobs)]
    (if (seq jobs-not-existing)
      [false {::error (->> jobs-not-existing
                           (map #(str "UUID " % " does not correspond to a job."))
                           (str/join \space))}]
      [true {::jobs jobs}])))

(defn validate-retries
  [conn task-constraints ctx]
  (let [retries (get-in ctx [:request :body-params :retries])
        increment (get-in ctx [:request :body-params :increment])
        jobs (jobs-from-request ctx)
        retry-limit (:retry-limit task-constraints)]
    (cond
      (empty? jobs)
      [true {:error "Need to specify at least 1 job."}]

      (and (get-in ctx [:request :body-params :job])
           (get-in ctx [:request :body-params :jobs]))
      [true {:error "Can't specify both \"job\" and \"jobs\"."}]

      (and (get-in ctx [:request :query-params :job])
           (get-in ctx [:request :body-params :job]))
      [true {:error "Can't specify \"job\" in both query and body."}]

      (and (get-in ctx [:request :query-params :job])
           (get-in ctx [:request :body-params :jobs]))
      [true {:error "Can't specify both \"job\" in query and \"jobs\" in body."}]

      (and (nil? retries) (nil? increment))
      [true {::error "Need to specify either retries or increment."}]

      (and retries increment)
      [true {::error "Can't specify both retries and increment."}]

      (and retries (> retries retry-limit))
      [true {::error (str "'retries' exceeds the maximum retry limit of " retry-limit)}]

      (and increment (let [db (d/db conn)]
                       (some (fn [job]
                               (> (+ (:job/max-retries (d/entity db [:job/uuid job]))
                                     increment)
                                  retry-limit))
                             jobs)))
      [true {::error (str "Increment would exceed the maximum retry limit of " retry-limit)}]

      :else
      [false {::retries retries
              ::increment increment
              ::jobs jobs}])))

(defn user-can-retry-job?
  [conn is-authorized-fn request-user job]
  (let [job-owner (:job/user (d/entity (db conn) [:job/uuid job]))]
    (is-authorized-fn request-user :retry {:owner job-owner :item :job})))

(defn check-retry-allowed
  [conn is-authorized-fn ctx]
  (let [request-user (get-in ctx [:request :authorization/user])
        unauthorized-jobs (remove (partial user-can-retry-job?
                                           conn is-authorized-fn request-user)
                                  (::jobs ctx))]
    (if (seq unauthorized-jobs)
      [false {::error (->> unauthorized-jobs
                           (map #(str "You are not authorized to retry job " % "."))
                           (str/join \space))}]
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
     :exists? (partial check-jobs-exist conn)
     :handle-ok (partial display-retries conn)}))

(defn retry-jobs!
  [conn ctx]
  (let [db (d/db conn)]
    (doseq [job (distinct (::jobs ctx))]
      (let [new-retries (or (::retries ctx)
                            (+ (:job/max-retries (d/entity db [:job/uuid job]))
                               (::increment ctx)))]
        (util/retry-job! conn job new-retries)))))

(defn post-retries-handler
  [conn is-authorized-fn task-constraints]
  (base-retries-handler
    conn is-authorized-fn
    {:allowed-methods [:post]
     ;; we need to check if it's a valid job UUID in malformed? here,
     ;; because this endpoint currently isn't restful (POST used for what is
     ;; actually an idempotent update; it should be PUT).
     :exists? (partial check-jobs-exist conn)
     :malformed? (partial validate-retries conn task-constraints)
     :handle-created (partial display-retries conn)
     :post! (partial retry-jobs! conn)}))

(defn put-retries-handler
  [conn is-authorized-fn task-constraints]
  (base-retries-handler
    conn is-authorized-fn
    {:allowed-methods [:put]
     :exists? (partial check-jobs-exist conn)
     :malformed? (partial validate-retries conn task-constraints)
     :handle-created (partial display-retries conn)
     :put! (partial retry-jobs! conn)}))

;; /share and /quota
(def UserParam {:user s/Str})

(def UserLimitsResponse
  {String s/Num})

(defn set-limit-params
  [limit-type]
  {:body-params (merge UserParam {limit-type {s/Keyword s/Num}
                                  :reason s/Str})})

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
     :delete! (fn [ctx]
                (retract-limit-fn conn
                                  (get-in ctx [:request :query-params :user])
                                  (get-in ctx [:request :query-params :reason])))}))

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
              (apply set-limit-fn
                     conn
                     (get-in ctx [:request :body-params :user])
                     (get-in ctx [:request :body-params :reason])
                     (reduce into [] (::limits ctx))))}))


;; /failure-reasons

(s/defschema FailureReasonsResponse
  [{:code s/Int
    :name s/Str
    :description s/Str
    :mea_culpa s/Bool
    (s/optional-key :failure_limit) s/Int}])

(defn reason-entity->consumable-map
  [default-failure-limit e]
  (cond-> {:code (:reason/code e)
           :name (name (:reason/name e))
           :description (:reason/string e)
           :mea_culpa (:reason/mea-culpa? e)}
          (:reason/mea-culpa? e)
          (assoc :failure_limit (or (:reason/failure-limit e) default-failure-limit))))

(defn failure-reasons-handler
  [conn is-authorized-fn]
  (base-cook-handler
    {:allowed-methods [:get]
     :handle-ok (fn [_]
                  (let [db (d/db conn)
                        default-limit (reason/default-failure-limit db)]
                    (->> (reason/all-known-reasons db)
                         (mapv (partial reason-entity->consumable-map
                                        default-limit)))))}))

;; /settings

(defn stringify
  "Converts values to strings as needed for conversion to JSON"
  [v]
  (cond
    (fn? v) (str v)
    (map? v) (map-vals stringify v)
    (instance? Atom v) (stringify (deref v))
    (instance? TestingServer v) (str v)
    (instance? Minutes v) (str v)
    (instance? ServerSocket v) (str v)
    (instance? Var v) (str v)
    (instance? RiemannReporter v) (str v)
    :else v))

(defn settings-handler
  [settings]
  (base-cook-handler
    {:allowed-methods [:get]
     :handle-ok (fn [_] (stringify settings))}))

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

(defn normalize-list-states
  "Given a set, states, returns a new set that only contains one of the
  'terminal' states of completed, success, and failed. We take completed
  to mean both success and failed."
  [states]
  (if (contains? states "completed")
    (set/difference states util/instance-states)
    (if (set/superset? states util/instance-states)
      (-> states (set/difference util/instance-states) (conj "completed"))
      states)))

(defn valid-name-filter?
  "Returns true if the provided name filter is either nil or satisfies the JobNameListFilter schema"
  [name]
  (or (nil? name)
      (nil? (s/check JobNameListFilter name))))

(defn name-filter-str->name-filter-pattern
  "Generates a regex pattern corresponding to a user-provided name filter string"
  [name]
  (re-pattern (-> name
                  (str/replace #"\." "\\\\.")
                  (str/replace #"\*+" ".*"))))

(defn name-filter-str->name-filter-fn
  "Returns a name-filtering function (or nil) given a user-provided name filter string"
  [name]
  (if name
    (let [pattern (name-filter-str->name-filter-pattern name)]
      #(re-matches pattern %))
    nil))

(defn list-resource
  [db framework-id is-authorized-fn agent-query-cache]
  (liberator/resource
    :available-media-types ["application/json"]
    :allowed-methods [:get]
    :as-response (fn [data ctx] {:body data})
    :malformed? (fn [ctx]
                  ;; since-hours-ago is included for backwards compatibility but is deprecated
                  ;; please use start-ms and end-ms instead
                  (let [{:keys [state user since-hours-ago start-ms end-ms limit name]
                         :as params}
                        (keywordize-keys (or (get-in ctx [:request :query-params])
                                             (get-in ctx [:request :body-params])))
                        states (if state (set (clojure.string/split state #"\+")) nil)
                        allowed-list-states (set/union util/job-states util/instance-states)]
                    (cond
                      (not (and state user))
                      [true {::error "must supply the state and the user name"}]

                      (not (set/superset? allowed-list-states states))
                      [true {::error (str "unsupported state in " state ", must be one of: " allowed-list-states)}]

                      (not (valid-name-filter? name))
                      [true {::error
                             (str "unsupported name filter " name
                                  ", can only contain alphanumeric characters, '.', '-', '_', and '*' as a wildcard")}]

                      :else
                      (try
                        [false {::states (normalize-list-states states)
                                ::user user
                                ::since-hours-ago (parse-int-default since-hours-ago 24)
                                ::start-ms (parse-long-default start-ms nil)
                                ::limit (parse-int-default limit Integer/MAX_VALUE)
                                ::end-ms (parse-long-default end-ms (System/currentTimeMillis))
                                ::name-filter-fn (name-filter-str->name-filter-fn name)}]
                        (catch NumberFormatException e
                          [true {::error (.toString e)}])))))
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
                        limit ::limit
                        name-filter-fn ::name-filter-fn} ctx
                       start (if start-ms
                               (Date. start-ms)
                               (Date. (- end-ms (-> since-hours-ago t/hours t/in-millis))))
                       end (Date. end-ms)
                       job-uuids (->> (timers/time!
                                       fetch-jobs
                                       ;; Get valid timings
                                       (util/get-jobs-by-user-and-states db user states start end limit name-filter-fn))
                                      (sort-by :job/submit-time)
                                      reverse
                                      (map :job/uuid))
                       job-uuids (if (nil? limit)
                                   job-uuids
                                   (take limit job-uuids))]
                    (mapv (partial fetch-job-map db framework-id agent-query-cache) job-uuids))))))

;;
;; /unscheduled_jobs
;;

(s/defschema UnscheduledJobResponse [{:uuid s/Uuid
                                      :reasons [{:reason s/Str
                                                 :data {s/Any s/Any}}]}])

(defn job-reasons
  "Given a job, load the unscheduled reasons from the database and massage them
  into a presentation-friendly structure. For example:
  [{:reason \"reason1\" :data {:attr1 1 :attr2 2}}}
   {:reason \"reason2\" :data {:attra 3 :attrb 4}}}]"
  [conn job-uuid]
  (let [db (d/db conn)
        job (d/entity db [:job/uuid job-uuid])
        reasons (unscheduled/reasons conn job)
        representation (map (fn [[reason data]] {:reason reason :data data})
                            reasons)]
    representation))

(defn read-unscheduled-handler
  [conn is-authorized-fn]
  (base-cook-handler
    {:allowed-methods [:get]
     :exists? (partial check-jobs-exist conn)
     :allowed? (partial job-request-allowed? conn is-authorized-fn)
     :handle-ok (fn [ctx]
                  (map (fn [job] {:uuid job
                                  :reasons (job-reasons conn job)})
                       (::jobs ctx)))}))

(defn- streaming-json-encoder
  "Takes as input the response body which can be converted into JSON,
  and returns a function which takes a ServletResponse and writes the JSON
  encoded response data. This is suitable for use with jet's server API."
  [data]
  (fn [^ServletResponse resp]
    (cheshire/generate-stream data
                              (OutputStreamWriter. (.getOutputStream resp)))))

(defn- streaming-json-middleware
  "Ring middleware which encodes JSON responses with streaming-json-encoder.
  All other response types are unaffected."
  [handler]
  (fn streaming-json-handler [req]
    (let [{:keys [headers body] :as resp} (handler req)
          json-response (or (= "application/json" (get headers "Content-Type"))
                            (coll? body))]
      (cond-> resp
        json-response (res/content-type "application/json")
        (and json-response body) (assoc :body (streaming-json-encoder body))))))

;;
;; "main" - the entry point that routes to other handlers
;;
(defn main-handler
  [conn framework-id mesos-pending-jobs-fn mesos-agent-query-cache
   {:keys [is-authorized-fn task-constraints]
    gpu-enabled? :mesos-gpu-enabled
    :as settings}
   leader-selector
   mesos-leadership-atom]
  (->
   (routes
    (c-api/api
     {:swagger {:ui "/swagger-ui"
                :spec "/swagger-docs"
                :data {:info {:title "Cook API"
                              :description "How to Cook things"}
                       :tags [{:name "api", :description "some apis"}]}}
      :format nil
      :coercion cook-coercer}

     (c-api/context
      "/rawscheduler" []
      (c-api/resource
       {:get {:summary "Returns info about a set of Jobs"
              :parameters {:query-params QueryJobsParams}
              :responses {200 {:schema [JobResponse]
                               :description "The jobs and their instances were returned."}
                          400 {:description "Non-UUID values were passed as jobs."}
                          403 {:description "The supplied UUIDs don't correspond to valid jobs."}}
              :handler (read-jobs-handler conn framework-id task-constraints gpu-enabled? is-authorized-fn mesos-agent-query-cache)}
        :post {:summary "Schedules one or more jobs."
               :parameters {:body-params RawSchedulerRequest}
               :responses {201 {:description "The jobs were successfully scheduled."}
                           400 {:description "One or more of the jobs were incorrectly specified."}
                           409 {:description "One or more of the jobs UUIDs are already in use."}}
               :handler (create-jobs-handler conn task-constraints gpu-enabled? is-authorized-fn)}
        :delete {:summary "Cancels jobs, halting execution when possible."
                 :responses {204 {:description "The jobs have been marked for termination."}
                             400 {:description "Non-UUID values were passed as jobs."}
                             403 {:description "The supplied UUIDs don't correspond to valid jobs."}}
                 :parameters {:query-params JobOrInstanceIds}
                 :handler (destroy-jobs-handler conn framework-id is-authorized-fn mesos-agent-query-cache)}}))

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
                          :description "The number of retries for the jobs."}
                     400 {:description "Invalid request format."}
                     401 {:description "Request user not authorized to access those jobs."}
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
              :parameters {:query-params QueryGroupsParams}
              :responses {200 {:schema [GroupResponse]
                               :description "The groups were returned."}
                          400 {:description "Non-UUID values were passed."}
                          403 {:description "The supplied UUIDs don't correspond to valid groups."}}
              :handler (read-groups-handler conn task-constraints is-authorized-fn)}}))

     (c-api/context
      "/failure_reasons" []
      (c-api/resource
       {:get {:summary "Returns a description of all possible task failure reasons"
              :responses {200 {:schema FailureReasonsResponse
                               :description "The failure reasons were returned."}}
              :handler (failure-reasons-handler conn is-authorized-fn)}}))

     (c-api/context
      "/settings" []
      (c-api/resource
       {:get {:summary "Returns the settings that cook is configured with"
              :responses {200 {:description "The settings were returned."}}
              :handler (settings-handler settings)}}))

     (c-api/context
      "/unscheduled_jobs" []
      (c-api/resource
       {:produces ["application/json"],
        :get {:summary "Read reasons why a job isn't being scheduled."
              :parameters {:query-params {:job s/Uuid}}
              :handler (read-unscheduled-handler conn is-authorized-fn)
              :responses {200 {:schema UnscheduledJobResponse
                               :description "Reasons the job isn't being scheduled."}
                          400 {:description "Invalid request format."}
                          404 {:description "The UUID doesn't correspond to a job."}}}})))
    (ANY "/queue" []
         (waiting-jobs mesos-pending-jobs-fn is-authorized-fn mesos-leadership-atom leader-selector))
    (ANY "/running" []
         (running-jobs conn is-authorized-fn))
    (ANY "/list" []
         (list-resource (db conn) framework-id is-authorized-fn mesos-agent-query-cache)))
   (format-params/wrap-restful-params {:formats [:json-kw]
                                       :handle-error c-mw/handle-req-error})
   (streaming-json-middleware)))

