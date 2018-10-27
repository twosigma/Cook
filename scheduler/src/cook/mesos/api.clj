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
            [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clj-time.format :as tf]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk :refer [keywordize-keys]]
            [compojure.api.middleware :as c-mw]
            [compojure.api.sweet :as c-api]
            [compojure.core :refer [ANY GET POST routes]]
            [cook.config :as config]
            [cook.cors :as cors]
            [cook.datomic :as datomic]
            [cook.mesos.data-locality :as dl]
            [cook.mesos.pool :as pool]
            [cook.mesos.quota :as quota]
            [cook.mesos.reason :as reason]
            [cook.mesos.schema :refer [constraint-operators host-placement-types straggler-handling-types]]
            [cook.mesos.share :as share]
            [cook.mesos.task-stats :as task-stats]
            [cook.mesos.unscheduled :as unscheduled]
            [cook.mesos.util :as util]
            [cook.mesos]
            [cook.rate-limit :as rate-limit]
            [cook.util :refer [ZeroInt PosNum NonNegNum PosInt NonNegInt PosDouble UserName NonEmptyString]]
            [datomic.api :as d :refer [q]]
            [liberator.core :as liberator]
            [liberator.util :refer [combine]]
            [me.raynes.conch :as sh]
            [mesomatic.scheduler]
            [metrics.histograms :as histograms]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [ring.middleware.format-params :as format-params]
            [ring.util.response :as res]
            [schema.core :as s]
            [swiss.arrows :refer :all])
  (:import (clojure.lang Atom Var)
           com.codahale.metrics.ScheduledReporter
           com.netflix.fenzo.VMTaskFitnessCalculator
           (java.io OutputStreamWriter)
           (java.net ServerSocket URLEncoder)
           (java.util Date UUID)
           javax.servlet.ServletResponse
           org.apache.curator.test.TestingServer
           (org.joda.time DateTime Minutes)
           schema.core.OptionalKey))


;; We use Liberator to handle requests on our REST endpoints.
;; The control flow among Liberator's handler functions is described here:
;; https://clojure-liberator.github.io/liberator/doc/decisions.html

;; This is necessary to prevent a user from requesting a uid:gid
;; pair other than their own (for example, root)
(sh/let-programs
  [_id "/usr/bin/id"]
  (defn uid [user-name]
    (str/trim (_id "-u" user-name)))
  (defn gid [user-name]
    (str/trim (_id "-g" user-name))))

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
   :handle-conflict render-error
   :handle-unprocessable-entity render-error})

(defn base-cook-handler
  [resource-attrs]
  (pc/mapply liberator/resource (merge cook-liberator-attrs resource-attrs)))

;;
;; /rawscheduler
;;

(defn prepare-schema-response
  "Takes a schema and outputs a new schema which conforms to our responses.

   Modifications:
   1. changes keys to snake_case"
  [schema]
  (pc/map-keys (fn [k]
                 (if (instance? OptionalKey k)
                   (update k :k ->snake_case)
                   (->snake_case k)))
               schema))

(def iso-8601-format (:date-time tf/formatters))

(def partition-date-format (:basic-date tf/formatters))

(s/defschema CookInfo
  "Schema for the /info endpoint response"
  {:authentication-scheme s/Str
   :commit s/Str
   :start-time s/Inst
   :version s/Str})

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
   (s/optional-key :reason_mea_culpa) s/Bool
   (s/optional-key :executor) s/Str
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

(defn non-empty-max-128-characters
  "String of between 1 and 128 characters"
  [s]
  (<= 1 (.length s) 128))

(def non-empty-max-128-characters-str
  (s/constrained s/Str non-empty-max-128-characters))

(defn valid-date-str?
  "yyyyMMdd"
  [s]
  (try
    (tf/parse partition-date-format s)
    true
    (catch Exception e false)))

(def Application
  "Schema for the application a job corresponds to"
  {:name (s/constrained s/Str non-empty-max-128-characters-and-alphanum?)
   :version (s/constrained s/Str non-empty-max-128-characters-and-alphanum?)})

; Datasets represent the data dependencies of jobs, which can be used by the scheduler to schedule jobs
; on hosts to take advantage of data locality.
; A dataset is decribed by two fields:
; - dataset: a dictionary from string->string describing the dataset
;   e.g. {"kind": "trades", "market": "NYSE"}
; - partitions (optional): a description of a subset of the dataset required
;   e.g. [{"begin": "20180101", "end": "20180201"}, {"begin": "20180301", "end": "20180401"}]
; If partitions are specified, the dataset field must contain the key "partition-type" to describe the type of partition scheme.
;   e.g. "partition-type": "date"
(def Dataset
  "Schema for a job dataset"
  {:dataset {non-empty-max-128-characters-str non-empty-max-128-characters-str}
   (s/optional-key :partitions) #{{non-empty-max-128-characters-str non-empty-max-128-characters-str}}})

(def DatePartition
  "Schema for a date partition"
  {(s/required-key "begin") (s/constrained s/Str valid-date-str?)
   (s/required-key "end") (s/constrained s/Str valid-date-str?)})

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
   (s/optional-key :executor) (s/enum "cook" "mesos")
   (s/optional-key :progress-output-file) NonEmptyString
   (s/optional-key :progress-regex-string) NonEmptyString
   (s/optional-key :group) s/Uuid
   (s/optional-key :disable-mea-culpa-retries) s/Bool
   :cpus PosDouble
   :mem PosDouble
   (s/optional-key :gpus) (s/both s/Int (s/pred pos? 'pos?))
   ;; Make sure the user name is valid. It must begin with a lower case character, end with
   ;; a lower case character or a digit, and has length between 2 to (62 + 2).
   :user UserName
   (s/optional-key :application) Application
   (s/optional-key :expected-runtime) PosInt
   (s/optional-key :datasets) #{Dataset}})

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
             (s/optional-key :datasets) [{:dataset {s/Keyword s/Str}
                                          (s/optional-key :partitions) [{s/Keyword s/Str}]}]
             ;; The Java job client used to send the
             ;; status field on job submission. At some
             ;; point, that changed, but we will keep
             ;; allowing it here for backwards compat.
             (s/optional-key :status) s/Str
             (s/optional-key :uris) [UriRequest])))

(def JobRequest
  "Schema for the part of a request that launches a single job."
  (s/constrained JobRequestMap valid-runtimes?))

(def JobResponseBase
  "Schema for a description of a job (as returned by the API).
  The structure is similar to JobRequest, but has some differences.
  For example, it can include descriptions of instances for the job."
  (-> JobRequestMap
      (dissoc (s/optional-key :group))
      (dissoc (s/optional-key :status))
      (merge {:framework-id (s/maybe s/Str)
              :retries-remaining NonNegInt
              :status s/Str
              :state s/Str
              :submit-time (s/maybe PosInt)
              :user UserName
              (s/optional-key :gpus) s/Int
              (s/optional-key :groups) [s/Uuid]
              (s/optional-key :instances) [Instance]
              (s/optional-key :pool) s/Str
              (s/optional-key :datasets) #{{:dataset {s/Str s/Str}
                                            (s/optional-key :partitions) #{{s/Str s/Str}}}}})
      prepare-schema-response))

(def JobResponseDeprecated
  "Deprecated. New fields should be added only to the JobResponse schema."
  JobResponseBase)

(def JobResponse
  "Schema for a description of a job (as returned by the API)."
  (let [ParentGroup {:uuid s/Uuid, :name s/Str}]
    (-> JobResponseBase
        (assoc (s/optional-key :groups) [ParentGroup]))))

(def InstanceResponse
  "Schema for a description of a job instance (as returned by the API)."
  (let [ParentJob {:uuid s/Uuid
                   :name s/Str
                   :status s/Str
                   :state s/Str
                   :user UserName}]
    (-> Instance
        (assoc :job ParentJob))))

(def JobOrInstanceIds
  "Schema for any number of job and/or instance uuids"
  {(s/optional-key :job) [s/Uuid]
   (s/optional-key :instance) [s/Uuid]})

(def QueryJobsParamsDeprecated
  "Schema for querying for jobs by job and/or instance uuid, allowing optionally
  for 'partial' results, meaning that some uuids can be valid and others not"
  (-> JobOrInstanceIds
      (assoc (s/optional-key :partial) s/Bool)))

(def allowed-list-states (set/union util/job-states util/instance-states))

(def QueryJobsParams
  "Schema for querying for jobs by job uuid, allowing optionally for
  'partial' results, meaning that some uuids can be valid and others not"
  {(s/optional-key :uuid) [s/Uuid]
   (s/optional-key :partial) s/Bool
   (s/optional-key :user) UserName
   (s/optional-key :state) [(apply s/enum allowed-list-states)]
   (s/optional-key :start) s/Str
   (s/optional-key :end) s/Str
   (s/optional-key :limit) NonNegInt
   (s/optional-key :name) JobNameListFilter
   (s/optional-key :pool) s/Str})

(def QueryInstancesParams
  "Schema for querying for instances by instance uuid, allowing optionally for
  'partial' results, meaning that some uuids can be valid and others not"
  {:uuid [s/Uuid]
   (s/optional-key :partial) s/Bool})

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

(def KillGroupsParams
  "Schema for killing groups by uuid"
  {:uuid [s/Uuid]})

(def GroupResponse
  "A schema for a group http response"
  (-> Group
      (merge
        {:jobs [s/Uuid]
         (s/optional-key :waiting) s/Int
         (s/optional-key :running) s/Int
         (s/optional-key :completed) s/Int})
      prepare-schema-response))

(def JobSubmission
  "Schema for a request to submit one or more jobs."
  {:jobs [JobRequest]
   (s/optional-key :override-group-immutability) s/Bool
   (s/optional-key :groups) [Group]})

(def RawSchedulerRequestDeprecated
  "Schema for a request to the raw scheduler endpoint."
  JobSubmission)

(def JobSubmissionRequest
  "Schema for a POST request to the /jobs endpoint."
  (assoc JobSubmission
    (s/optional-key :pool) s/Str))

(defn- mk-container-params
  "Helper for build-container.  Transforms parameters into the datomic schema."
  [cid params]
  (when (seq params)
    {:docker/parameters (mapv (fn [{:keys [key value]}]
                                {:docker.param/key key
                                 :docker.param/value value})
                              params)}))

(defn- mkvolumes
  "Helper for build-container.  Transforms volumes into the datomic schema."
  [cid vols]
  (when (seq vols)
    (let [vol-maps (mapv (fn [{:keys [container-path host-path mode]}]
                           (merge (when container-path
                                    {:container.volume/container-path container-path})
                                  (when host-path
                                    {:container.volume/host-path host-path})
                                  (when mode
                                    {:container.volume/mode (str/upper-case mode)})))
                         vols)]
      {:container/volumes vol-maps})))

(defn- mk-docker-ports
  "Helper for build-container.  Transforms port-mappings into the datomic schema."
  [cid ports]
  (when (seq ports)
    (let [port-maps (mapv (fn [{:keys [container-port host-port protocol]}]
                            (merge (when container-port
                                     {:docker.portmap/container-port container-port})
                                   (when host-port
                                     {:docker.portmap/host-port host-port})
                                   (when protocol
                                     {:docker.portmap/protocol (str/upper-case protocol)})))
                          ports)]
      {:docker/port-mapping port-maps})))

(defn- build-container
  "Helper for submit-jobs, deal with container structure."
  [user id container]
  (let [container-id (d/tempid :db.part/user)
        docker-id (d/tempid :db.part/user)
        ctype (:type container)
        volumes (or (:volumes container) [])]
    (if (= (str/lower-case ctype) "docker")
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

(defn- str->executor-enum
  "Converts an executor string to the corresponding executor option enum.
   Throws an IllegalArgumentException if the string is non-nil and not supported."
  [executor]
  (when executor
    (case executor
      "cook" :executor/cook
      "mesos" :executor/mesos
      (throw (IllegalArgumentException. (str "Unsupported executor type: " executor))))))

(defn- make-commit-latch
  "Makes a new commit-latch.
   Returns a pair of (commit-latch-id, commit-latch)"
  []
  (let [commit-latch-id (d/tempid :db.part/user)
        commit-latch {:db/id commit-latch-id
                      :commit-latch/committed? true
                      :commit-latch/uuid (d/squuid)}]
    [commit-latch-id commit-latch]))

(defn make-partition-ent
  "Makes a datomic entity for a partition"
  [partition-type partition]
  (let [coerce-date (fn coerce-date [date-str]
                      (tc/to-date (tf/parse partition-date-format date-str)))]
    (case partition-type
      "date"
      {:dataset.partition/begin (coerce-date (partition "begin"))
       :dataset.partition/end (coerce-date (partition "end"))}
      :default (throw (IllegalArgumentException. (str "Unsupported partition type " partition-type))))))

(s/defn make-job-txn
  "Creates the necessary txn data to insert a job into the database"
  [pool commit-latch-id job :- Job]
  (let [{:keys [uuid command max-retries max-runtime expected-runtime priority cpus mem gpus
                user name ports uris env labels container group application disable-mea-culpa-retries
                constraints executor progress-output-file progress-regex-string datasets]
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
        executor (str->executor-enum executor)
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
        datasets (map (fn [{:keys [dataset partitions]}]
                        (let [parameters (map (fn [[k v]] {:db/id (d/tempid :db.part/user)
                                                           :dataset.parameter/key k
                                                           :dataset.parameter/value v})
                                              dataset)]
                          (if (contains? dataset "partition-type")
                            (let [partition-type (get dataset "partition-type")
                                  partitions (map (partial make-partition-ent partition-type) partitions)]
                              {:db/id (d/tempid :db.part/user)
                               :dataset/parameters parameters
                               :dataset/partition-type partition-type
                               :dataset/partitions partitions})
                            {:db/id (d/tempid :db.part/user)
                             :dataset/parameters parameters})))
                      datasets)
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
                    expected-runtime (assoc :job/expected-runtime expected-runtime)
                    executor (assoc :job/executor executor)
                    progress-output-file (assoc :job/progress-output-file progress-output-file)
                    progress-regex-string (assoc :job/progress-regex-string progress-regex-string)
                    pool (assoc :job/pool (:db/id pool))
                    (seq datasets) (assoc :job/datasets datasets))]

    ;; TODO batch these transactions to improve performance
    (-> ports
        (into uris)
        (into env)
        (into constraints)
        (into labels)
        (into container)
        (into maybe-datoms)
        (conj txn))))

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
        {(namespace-fn :parameters) (pc/map-keys (partial namespace-fn type)
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

(defn validate-partitions
  "Ensures that the given partitions are valid.
   Currently, we only support date partitions which have a begin and end value, specified as a yyyyMMdd date."
  [{:keys [dataset partitions] :as in}]
  (let [partition-type (get dataset "partition-type" nil)]
    (cond
      (nil? partition-type)
      (when-not (empty? partitions)
        (throw (ex-info "Dataset with partitions must supply partition-type"
                        {:dataset dataset})))
      (= "date" partition-type)
      (run! (partial s/validate DatePartition) partitions)
      :else
      (throw (ex-info "Dataset with unsupported partition type"
                      {:dataset dataset}))))
  in)

(defn munge-datasets
  "Converts dataset and partition keys (keywords) into strings"
  [datasets]
  (->> datasets
       (map (fn [{:keys [dataset partitions]}]
              (cond-> {:dataset (walk/stringify-keys dataset)}
                partitions
                (assoc :partitions (into #{} (walk/stringify-keys partitions))))))
       (into #{})))

(defn validate-and-munge-job
  "Takes the user, the parsed json from the job and a list of the uuids of
   new-groups (submitted in the same request as the job). Returns proper Job
   objects, or else throws an exception"
  [db user task-constraints gpu-enabled? new-group-uuids
   {:keys [cpus mem gpus uuid command priority max-retries max-runtime expected-runtime name
           uris ports env labels container group application disable-mea-culpa-retries
           constraints executor progress-output-file progress-regex-string datasets]
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
                 (when gpus {:gpus (int gpus)})
                 (when env {:env (walk/stringify-keys env)})
                 (when uris {:uris (map (fn [{:keys [value executable cache extract]}]
                                          (merge {:value value}
                                                 (when executable {:executable? executable})
                                                 (when cache {:cache? cache})
                                                 (when extract {:extract? extract})))
                                        uris)})
                 (when labels {:labels (walk/stringify-keys labels)})
                 ;; Rest framework keywordifies all keys, we want these to be strings!
                 (when constraints {:constraints constraints})
                 (when group-uuid {:group group-uuid})
                 (when container {:container container})
                 (when expected-runtime {:expected-runtime expected-runtime})
                 (when executor {:executor executor})
                 (when progress-output-file {:progress-output-file progress-output-file})
                 (when progress-regex-string {:progress-regex-string progress-regex-string})
                 (when application {:application application})
                 (when datasets {:datasets (munge-datasets datasets)}))]
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
    (doseq [dataset (:datasets munged)]
      (validate-partitions dataset))
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

(defn retrieve-url-path
  "Constructs a URL to query the sandbox directory of the task.
   Uses the provided sandbox-directory to determine the sandbox directory.
   Hard codes fun stuff like the port we run the agent on.
   Users will need to add the file path & offset to their query.
   Refer to the 'Using the output_url' section in docs/scheduler-rest-api.adoc for further details."
  [agent-hostname task-id sandbox-directory]
  (try
    (when sandbox-directory
      (str "http://" agent-hostname ":5051" "/files/read.json?path="
           (URLEncoder/encode sandbox-directory "UTF-8")))
    (catch Exception e
      (log/debug e "Unable to retrieve directory path for" task-id "on agent" agent-hostname)
      nil)))

(defn fetch-instance-map
  "Converts the instance entity to a map representing the instance fields."
  [db instance]
  (let [hostname (:instance/hostname instance)
        task-id (:instance/task-id instance)
        executor (:instance/executor instance)
        sandbox-directory (:instance/sandbox-directory instance)
        url-path (retrieve-url-path hostname task-id sandbox-directory)
        start (:instance/start-time instance)
        mesos-start (:instance/mesos-start-time instance)
        end (:instance/end-time instance)
        cancelled (:instance/cancelled instance)
        reason (reason/instance-entity->reason-entity db instance)
        exit-code (:instance/exit-code instance)
        progress (:instance/progress instance)
        progress-message (:instance/progress-message instance)]
    (cond-> {:backfilled false ;; Backfill has been deprecated
             :executor_id (:instance/executor-id instance)
             :hostname hostname
             :ports (vec (sort (:instance/ports instance)))
             :preempted (:instance/preempted? instance false)
             :slave_id (:instance/slave-id instance)
             :status (name (:instance/status instance))
             :task_id task-id}
            executor (assoc :executor (name executor))
            start (assoc :start_time (.getTime start))
            mesos-start (assoc :mesos_start_time (.getTime mesos-start))
            end (assoc :end_time (.getTime end))
            cancelled (assoc :cancelled cancelled)
            exit-code (assoc :exit_code exit-code)
            url-path (assoc :output_url url-path)
            reason (assoc :reason_code (:reason/code reason)
                          :reason_string (:reason/string reason)
                          :reason_mea_culpa (:reason/mea-culpa? reason))
            progress (assoc :progress progress)
            progress-message (assoc :progress_message progress-message)
            sandbox-directory (assoc :sandbox_directory sandbox-directory))))

(defn- docker-parameter->response-map
  [{:keys [docker.param/key docker.param/value]}]
  {:key key
   :value value})

(defn- docker->response-map
  [{:keys [docker/image docker/parameters docker/network docker/force-pull-image]}]
  (cond-> {:image image}
          parameters (assoc :parameters (map docker-parameter->response-map parameters))
          network (assoc :network network)
          (some? force-pull-image) (assoc :force-pull-image force-pull-image)))

(defn- container-volume->response-map
  [{:keys [container.volume/mode container.volume/host-path container.volume/container-path]}]
  (cond-> {:host-path host-path}
          mode (assoc :mode mode)
          container-path (assoc :container-path container-path)))

(defn- container->response-map
  [{:keys [container/type container/docker container/volumes]}]
  (cond-> {:type type}
          docker (assoc :docker (docker->response-map docker))
          volumes (assoc :volumes (map container-volume->response-map volumes))))

(defn fetch-job-map
  [db framework-id job-uuid]
  (timers/time!
    (timers/timer ["cook-mesos" "internal" "fetch-job-map"])
    (let [job (d/entity db [:job/uuid job-uuid])
          resources (util/job-ent->resources job)
          groups (:group/_job job)
          application (:job/application job)
          expected-runtime (:job/expected-runtime job)
          executor (:job/executor job)
          progress-output-file (:job/progress-output-file job)
          progress-regex-string (:job/progress-regex-string job)
          pool (:job/pool job)
          container (:job/container job)
          state (util/job-ent->state job)
          constraints (->> job
                           :job/constraint
                           (map util/remove-datomic-namespacing)
                           (map (fn [{:keys [attribute operator pattern]}]
                                  (->> [attribute (str/upper-case (name operator)) pattern]
                                       (map str)))))
          instances (map #(fetch-instance-map db %1) (:job/instance job))
          submit-time (when (:job/submit-time job) ; due to a bug, submit time may not exist for some jobs
                        (.getTime (:job/submit-time job)))
          datasets (when (seq (:job/datasets job))
                     (dl/get-dataset-maps job))
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
              expected-runtime (assoc :expected-runtime expected-runtime)
              executor (assoc :executor (name executor))
              progress-output-file (assoc :progress-output-file progress-output-file)
              progress-regex-string (assoc :progress-regex-string progress-regex-string)
              pool (assoc :pool (:pool/name pool))
              container (assoc :container (container->response-map container))
              datasets (assoc :datasets datasets)))))

(defn fetch-group-live-jobs
  "Get all jobs from a group that are currently running or waiting (not complete)"
  [db guuid]
  (let [group (d/entity db [:group/uuid guuid])
        jobs (:group/job group)
        completed? #(-> % :job/state (= :job.state/completed))]
    (remove completed? jobs)))

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
        allow-partial-results (get-in ctx [:request :query-params :partial])
        instance-uuid->job-uuid #(instance-uuid->job-uuid (d/db conn) %)
        instance-jobs (mapv instance-uuid->job-uuid instances)
        exists? #(job-exists? (d/db conn) %)
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
                           " didn't correspond to a job")}])))

(defn instances-exist?
  [conn ctx]
  "Returns a tuple that either has the shape:

    [true {::jobs ...}]

  or:

    [false {::non-existing-uuids ...}]

  Given a collection of instance uuids, attempts to return the corresponding set of *existing*
  parent job uuids. By default (or if the 'partial' query parameter is false), the function
  returns an ::error if any of the provided instance uuids cannot be found. If
  'partial' is true, the function will return the subset of job uuids that were found,
  assuming at least one was found. This 'partial' flag allows a client to query a
  particular cook cluster for a set of uuids, where some of them may not belong to that
  cluster, and get back the data for those that do match."
  (let [uuids (wrap-seq (::instances ctx))
        allow-partial-results? (::allow-partial-results? ctx)
        instance-uuid->job-uuid #(instance-uuid->job-uuid (d/db conn) %)
        job-uuids (mapv instance-uuid->job-uuid uuids)]
    (cond
      (every? some? job-uuids)
      [true {::jobs job-uuids}]

      (and allow-partial-results? (some some? job-uuids))
      [true {::jobs (filter some? job-uuids)}]

      :else
      [false {::non-existing-uuids (filter (comp nil? instance-uuid->job-uuid) uuids)}])))

(defn jobs-exist?
  "Returns a tuple that either has the shape:

    [true {::jobs ...}]

  or:

    [false {::error ...}]

  Given a collection of job uuids, attempts to return the corresponding set of *existing*
  job uuids. By default (or if the 'partial' query parameter is false), the function
  returns an ::error if any of the provided job uuids cannot be found. If
  'partial' is true, the function will return the subset of job uuids that were found,
  assuming at least one was found. This 'partial' flag allows a client to query a
  particular cook cluster for a set of uuids, where some of them may not belong to that
  cluster, and get back the data for those that do match."
  [conn ctx]
  (let [uuids (::jobs ctx)
        allow-partial-results? (::allow-partial-results? ctx)
        exists? #(job-exists? (d/db conn) %)
        existing-uuids (filter exists? uuids)]
    (cond
      (= (count existing-uuids) (count uuids))
      [true {::jobs uuids}]

      (and allow-partial-results? (pos? (count existing-uuids)))
      [true {::jobs existing-uuids}]

      :else
      (let [non-existing-uuids (apply disj (set uuids) existing-uuids)
            message (str "The following UUIDs don't correspond to a job:\n" (str/join \newline non-existing-uuids))]
        [false {::error message}]))))

(defn check-job-params-present
  [ctx]
  (let [jobs (get-in ctx [:request :query-params :job])
        instances (get-in ctx [:request :query-params :instance])]
    (if (or jobs instances)
      false
      [true {::error "must supply at least one job or instance query param"}])))

(defn job-request-malformed?
  [ctx]
  (let [uuids (wrap-seq (get-in ctx [:request :params :uuid]))
        allow-partial-results? (get-in ctx [:request :query-params :partial])]
    [false {::jobs (map #(UUID/fromString %) uuids)
            ::allow-partial-results? allow-partial-results?}]))

(defn instance-request-parse-params
  [ctx]
  (let [uuids (wrap-seq (get-in ctx [:request :params :uuid]))
        allow-partial-results? (get-in ctx [:request :query-params :partial])]
    [false {::instances (mapv #(UUID/fromString %) uuids)
            ::allow-partial-results? allow-partial-results?}]))

(defn user-authorized-for-job?
  [conn is-authorized-fn ctx job-uuid]
  (let [request-user (get-in ctx [:request :authorization/user])
        job-user (:job/user (d/entity (d/db conn) [:job/uuid job-uuid]))
        impersonator (get-in ctx [:request :authorization/impersonator])
        request-method (get-in ctx [:request :request-method])]
    (is-authorized-fn request-user request-method impersonator {:owner job-user :item :job})))

(defn- job-uuids-from-context
  [conn ctx]
  (or (::jobs ctx)
      (::jobs (second (retrieve-jobs conn ctx)))))

(defn job-request-allowed?
  ([conn is-authorized-fn ctx uuids action]
   (let [authorized? (partial user-authorized-for-job? conn is-authorized-fn ctx)]
     (if (every? authorized? uuids)
       [true {}]
       [false {::error (str "You are not authorized to " action " the following jobs: "
                            (str/join \space (remove authorized? uuids)))}])))
  ([conn is-authorized-fn ctx uuids]
    (job-request-allowed? conn is-authorized-fn ctx uuids "view / access"))
  ([conn is-authorized-fn ctx]
   (let [uuids (job-uuids-from-context conn ctx)]
     (job-request-allowed? conn is-authorized-fn ctx uuids))))

(defn- job-kill-allowed?
  [conn is-authorized-fn ctx]
  (let [uuids (job-uuids-from-context conn ctx)]
    (job-request-allowed? conn is-authorized-fn ctx uuids "kill")))

(defn instance-request-allowed?
  [conn is-authorized-fn ctx]
  (let [[exist? exist-data] (instances-exist? conn ctx)]
    (if exist?
      (let [job-uuids (::jobs exist-data)
            [allowed? allowed-data] (job-request-allowed? conn is-authorized-fn ctx job-uuids)]
        (if allowed?
          [true {::jobs job-uuids}]
          [false allowed-data]))
      [true exist-data])))

(defn render-jobs-for-response-deprecated
  [conn framework-id ctx]
  (mapv (partial fetch-job-map (d/db conn) framework-id) (::jobs ctx)))

(defn render-jobs-for-response
  [conn framework-id ctx]
  (let [db (d/db conn)

        fetch-group
        (fn fetch-group [group-uuid]
          (let [group (d/entity db [:group/uuid (UUID/fromString group-uuid)])]
            {:uuid group-uuid
             :name (:group/name group)}))

        fetch-job
        (fn fetch-job [job-uuid]
          (let [job (fetch-job-map db framework-id job-uuid)
                groups (mapv fetch-group (:groups job))]
            (assoc job :groups groups)))]

    (mapv fetch-job (::jobs ctx))))

(defn render-instances-for-response
  [conn framework-id ctx]
  (let [db (d/db conn)
        fetch-job (partial fetch-job-map db framework-id)
        job-uuids (::jobs ctx)
        jobs (mapv fetch-job job-uuids)
        instance-uuids (set (::instances ctx))]
    (for [job jobs
          instance (:instances job)
          :let [instance-uuid (-> instance :task_id UUID/fromString)]
          :when (contains? instance-uuids instance-uuid)]
      (-> instance
          (assoc :job (select-keys job [:uuid :name :status :state :user]))))))

;; We store the start-up time (ISO-8601) for reporting on the /info endpoint
(def start-up-time (tf/unparse iso-8601-format (t/now)))

(defn cook-info-handler
  "Handler for the /info endpoint"
  [settings]
  (let [auth-middleware (:authorization-middleware settings)
        auth-scheme (str (or (-> auth-middleware meta :json-value) auth-middleware))]
    (base-cook-handler
      {:allowed-methods [:get]
       :handle-ok (fn get-info-handler [_]
                    {:authentication-scheme auth-scheme
                     :commit @cook.util/commit
                     :start-time start-up-time
                     :version @cook.util/version})})))

;;; On GET; use repeated job argument
(defn read-jobs-handler-deprecated
  [conn framework-id is-authorized-fn]
  (base-cook-handler
    {:allowed-methods [:get]
     :malformed? check-job-params-present
     :allowed? (partial job-request-allowed? conn is-authorized-fn)
     :exists? (partial retrieve-jobs conn)
     :handle-ok (fn [ctx] (render-jobs-for-response-deprecated conn framework-id ctx))}))

(defn valid-name-filter?
  "Returns true if the provided name filter is either nil or satisfies the JobNameListFilter schema"
  [name]
  (or (nil? name)
      (nil? (s/check JobNameListFilter name))))

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

(defn name-filter-str->name-filter-pattern
  "Generates a regex pattern corresponding to a user-provided name filter string"
  [name]
  (re-pattern (-> name
                  (str/replace #"\." "\\\\.")
                  (str/replace #"\*+" ".*"))))

(defn name-filter-str->name-filter-fn
  "Returns a name-filtering function (or nil) given a user-provided name filter string"
  [name]
  (when name
    (let [pattern (name-filter-str->name-filter-pattern name)]
      #(re-matches pattern %))))

(defn job-list-request-malformed?
  "Returns a [true {::error ...}] pair if the request is malformed, and
  otherwise [false m], where m represents data that gets merged into the ctx"
  [ctx]
  (let [{:strs [state user start end limit name]} (get-in ctx [:request :query-params])
        pool (or (get-in ctx [:request :query-params "pool"])
                 (get-in ctx [:request :headers "x-cook-pool"]))
        states (wrap-seq state)]
    (cond
      (not (and states user start end))
      [true {::error "must supply the state, user name, start time, and end time"}]

      (not (set/superset? allowed-list-states states))
      [true {::error (str "unsupported state in " states ", must be one of: " allowed-list-states)}]

      (not (valid-name-filter? name))
      [true {::error
             (str "unsupported name filter " name
                  ", can only contain alphanumeric characters, '.', '-', '_', and '*' as a wildcard")}]

      :else
      (try
        [false {::states (normalize-list-states states)
                ::user user
                ::start-ms (-> start ^DateTime util/parse-time .getMillis)
                ::end-ms (-> end ^DateTime util/parse-time .getMillis)
                ::limit (util/parse-int-default limit 150)
                ::name-filter-fn (name-filter-str->name-filter-fn name)
                ::pool-name pool}]
        (catch NumberFormatException e
          [true {::error (.toString e)}])))))

(defn job-list-request-allowed?
  [is-authorized-fn ctx]
  (let [{limit ::limit, user ::user, start-ms ::start-ms, end-ms ::end-ms} ctx
        request-user (get-in ctx [:request :authorization/user])
        impersonator (get-in ctx [:request :authorization/impersonator])]
    (cond
      (not (is-authorized-fn request-user :get impersonator {:owner user :item :job}))
      [false {::error (str "You are not authorized to list jobs for " user)}]

      (not (pos? limit))
      [false {::error (str "limit must be positive")}]

      (and start-ms (> start-ms end-ms))
      [false {::error (str "start-ms (" start-ms ") must be before end-ms (" end-ms ")")}]

      :else true)))

(timers/deftimer [cook-scheduler handler fetch-jobs])
(timers/deftimer [cook-scheduler handler list-endpoint])
(histograms/defhistogram [cook-mesos api list-request-param-time-range-ms])
(histograms/defhistogram [cook-mesos api list-request-param-limit])
(histograms/defhistogram [cook-mesos api list-response-job-count])

(defn list-jobs
  "Queries using the params from ctx and returns the job uuids that were found"
  [db include-custom-executor? ctx]
  (timers/time!
    list-endpoint
    (let [{states ::states
           user ::user
           start-ms ::start-ms
           end-ms ::end-ms
           since-hours-ago ::since-hours-ago
           limit ::limit
           name-filter-fn ::name-filter-fn
           pool-name ::pool-name} ctx
          start-ms' (or start-ms (- end-ms (-> since-hours-ago t/hours t/in-millis)))
          start (Date. ^long start-ms')
          end (Date. ^long end-ms)
          job-uuids (->> (timers/time!
                           fetch-jobs
                           (util/get-jobs-by-user-and-states db user states start end limit
                                                             name-filter-fn include-custom-executor? pool-name))
                         (sort-by :job/submit-time)
                         reverse
                         (map :job/uuid))
          job-uuids (if (nil? limit)
                      job-uuids
                      (take limit job-uuids))]
      (histograms/update! list-request-param-time-range-ms (- end-ms start-ms'))
      (histograms/update! list-request-param-limit limit)
      (histograms/update! list-response-job-count (count job-uuids))
      job-uuids)))

(defn jobs-list-exist?
  [conn ctx]
  [true {::jobs (list-jobs (d/db conn) true ctx)}])

(defn read-jobs-handler
  [conn is-authorized-fn resource-attrs]
  (base-cook-handler
    (merge {:allowed-methods [:get]
            :malformed? (fn [ctx]
                          (if (get-in ctx [:request :params :uuid])
                            (job-request-malformed? ctx)
                            (job-list-request-malformed? ctx)))
            :allowed? (fn [ctx]
                        (if (get-in ctx [:request :params :uuid])
                          (job-request-allowed? conn is-authorized-fn ctx)
                          (job-list-request-allowed? is-authorized-fn ctx)))
            :exists? (fn [ctx]
                       (if (get-in ctx [:request :params :uuid])
                         (jobs-exist? conn ctx)
                         (jobs-list-exist? conn ctx)))}
           resource-attrs)))

(defn read-jobs-handler-multiple
  [conn framework-id is-authorized-fn]
  (let [handle-ok (partial render-jobs-for-response conn framework-id)]
    (read-jobs-handler conn is-authorized-fn {:handle-ok handle-ok})))

(defn read-jobs-handler-single
  [conn framework-id is-authorized-fn]
  (let [handle-ok
        (fn handle-ok [ctx]
          (first
            (render-jobs-for-response conn framework-id ctx)))]
    (read-jobs-handler conn is-authorized-fn {:handle-ok handle-ok})))

(defn instance-request-exists?
  [ctx]
  (if-let [non-existing-uuids (::non-existing-uuids ctx)]
    [false {::error (str "The following UUIDs don't correspond to a job instance:\n"
                         (str/join \newline non-existing-uuids))}]
    true))

(defn base-read-instances-handler
  [conn is-authorized-fn resource-attrs]
  (base-cook-handler
    (merge {:allowed-methods [:get]
            :malformed? instance-request-parse-params
            :allowed? (partial instance-request-allowed? conn is-authorized-fn)
            :exists? instance-request-exists?}
           resource-attrs)))

(defn read-instances-handler-multiple
  [conn framework-id is-authorized-fn]
  (let [handle-ok (partial render-instances-for-response conn framework-id)]
    (base-read-instances-handler conn is-authorized-fn {:handle-ok handle-ok})))

(defn read-instances-handler-single
  [conn framework-id is-authorized-fn]
  (let [handle-ok (->> (partial render-instances-for-response conn framework-id)
                       (comp first))]
    (base-read-instances-handler conn is-authorized-fn {:handle-ok handle-ok})))

;;; On DELETE; use repeated job argument
(defn destroy-jobs-handler
  [conn is-authorized-fn]
  (base-cook-handler
    {:allowed-methods [:delete]
     :malformed? check-job-params-present
     :allowed? (partial job-kill-allowed? conn is-authorized-fn)
     :exists? (partial retrieve-jobs conn)
     :delete! (fn [ctx]
                (cook.mesos/kill-job conn (::jobs-requested ctx))
                (cook.mesos/kill-instances conn (::instances-requested ctx)))}))

(defn vectorize
  "If x is not a vector (or nil), turns it into a vector"
  [x]
  (if (or (nil? x) (vector? x))
    x
    [x]))

(defn create-jobs!
  "Based on the context, persists the specified jobs, along with their groups,
  to Datomic.
  Preconditions:  The context must already have been populated with both
  ::jobs and ::groups, which specify the jobs and job groups."
  [conn {:keys [::groups ::jobs ::pool] :as ctx}]
  (try
    (log/info "Submitting jobs through raw api:" (map #(dissoc % :command) jobs))
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
          [commit-latch-id commit-latch] (make-commit-latch)
          job-txns (mapcat (partial make-job-txn pool commit-latch-id) jobs)
          job-uuids->dbids (->> job-txns
                                ;; Not all txns are for the top level job
                                (filter :job/uuid)
                                (map (juxt :job/uuid :db/id))
                                (into {}))
          group-uuid->job-dbids (->> jobs
                                     (group-by :group)
                                     (pc/map-vals (fn [jobs]
                                                    (map #(job-uuids->dbids (:uuid %))
                                                         jobs))))
          group-txns (map #(make-group-txn % (get group-uuid->job-dbids
                                                  (:uuid %)
                                                  []))
                          groups)]

      (let [user (get-in ctx [:request :authorization/user])]
        (rate-limit/spend! rate-limit/job-submission-rate-limiter user (count jobs)))

      @(d/transact
         conn
         (-> (vec group-asserts)
             (into job-asserts)
             (conj commit-latch)
             (into job-txns)
             (into group-txns)))

      (meters/mark! (meters/meter ["cook-mesos" "scheduler" "jobs-created"
                                   (str "pool-" (pool/pool-name-or-default (:pool/name pool)))])
                    (count jobs))
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
  (let [request-user (:authorization/user request)
        impersonator (:authorization/impersonator request)]
    (if (is-authorized-fn request-user :create impersonator {:owner request-user :item :job})
      true
      [false {::error "You are not authorized to create jobs"}])))

(defn no-job-exceeds-quota?
  "Check if any of the submitted jobs exceed the user's total quota,
   in which case the job would never be schedulable (unless the quota is increased).

   If none of the jobs individually seem to exceed the user's quota, returns true;
   otherwise, the following error structure is returned:

     [false {::error \"...\"}]

  where \"...\" is a detailed error string describing the quota bounds exceeded."
  [conn {:keys [::jobs ::pool] :as ctx}]
  (let [db (d/db conn)
        resource-keys [:cpus :mem :gpus]
        user (get-in ctx [:request :authorization/user])
        user-quota (quota/get-quota db user (:pool/name pool))
        errors (for [job jobs
                     resource resource-keys
                     :let [job-usage (-> job (get resource 0) double)
                           quota-val (-> user-quota (get resource) double)]
                     :when (> job-usage quota-val)]
                 (format "Job %s exceeds quota for %s: %f > %f"
                         (:uuid job) (name resource) job-usage quota-val))]
    (cond
      (zero? (:count user-quota)) [false {::error "User quota is set to zero jobs."}]
      (seq errors) [false {::error (str/join "\n" errors)}]
      :else true)))

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
                         headers (get-in ctx [:request :headers])
                         jobs (get params :jobs)
                         groups (get params :groups)
                         user (get-in ctx [:request :authorization/user])
                         override-group-immutability? (boolean (get params :override-group-immutability))
                         pool-name (or (get params :pool) (get headers "x-cook-pool"))
                         pool (when pool-name (d/entity (d/db conn) [:pool/name pool-name]))
                         uuid->count (pc/map-vals count (group-by :uuid jobs))
                         time-until-out-of-debt (rate-limit/time-until-out-of-debt-millis! rate-limit/job-submission-rate-limiter user)
                         in-debt? (not (zero? time-until-out-of-debt))]
                     (try
                       (when in-debt?
                         (log/info (str "User " user " is inserting too quickly (will be out of debt in "
                                        (/ time-until-out-of-debt 1000.0) " seconds).")))
                       (cond
                         (and in-debt? (rate-limit/enforce? rate-limit/job-submission-rate-limiter))
                         [true {::error (str "User " user " is inserting too quickly. Not allowed to insert for "
                                             (/ time-until-out-of-debt 1000.0) " seconds.")}]

                         (empty? params)
                         [true {::error (str "Must supply at least one job or group to start."
                                             "Are you specifying that this is application/json?")}]

                         (and pool-name (not pool))
                         [true {::error (str pool-name " is not a valid pool name.")}]

                         (and pool (not (pool/accepts-submissions? pool)))
                         [true {::error (str pool-name " is not accepting job submissions.")}]

                         (some true? (map (fn [[uuid count]] (< 1 count)) uuid->count))
                         [true {::error (str "Duplicate job uuids: " (->> uuid->count
                                                                          (filter (fn [[uuid count]] (< 1 count)))
                                                                          (map first)
                                                                          (map str)
                                                                          (into [])))}]
                         :else
                         (let [groups (mapv #(validate-and-munge-group (d/db conn) %) groups)
                               jobs (mapv #(validate-and-munge-job
                                             (d/db conn)
                                             user
                                             task-constraints
                                             gpu-enabled?
                                             (set (map :uuid groups))
                                             %
                                             :override-group-immutability?
                                             override-group-immutability?) jobs)]
                           [false {::groups groups
                                   ::jobs jobs
                                   ::pool pool}]))
                       (catch Exception e
                         (log/warn e "Malformed raw api request")
                         [true {::error (.getMessage e)}]))))
     :allowed? (partial job-create-allowed? is-authorized-fn)
     :exists? (fn [ctx]
                (let [db (d/db conn)
                      existing (filter (partial job-exists? db) (map :uuid (::jobs ctx)))]
                  [(seq existing) {::existing existing}]))
     ;; We want to return a 422 (unprocessable entity) if the requested resources
     ;; for a single job exceed the user's total resource quota.
     :processable? (partial no-job-exceeds-quota? conn)
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
                         (if (datomic/transaction-timeout? exception)
                           {:error (str "Transaction timed out."
                                        " Your jobs may not have been created successfully."
                                        " Please query your jobs and check whether they were created successfully.")}
                           {:error (str "Exception occurred while creating job - " (.getMessage exception))}))
     :handle-created (fn [ctx] (::results ctx))}))

(defn retrieve-groups
  "Returns a tuple that either has the shape:

    [false {::guuids ...}]

  or:

    [true {::error ...}]

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
          exists? #(group-exists? (d/db conn) %)
          {existing-groups true missing-groups false} (group-by exists? requested-guuids)]
      [false {::allow-partial-results? allow-partial-results
              ::guuids existing-groups
              ::non-existing-guuids missing-groups}])
    (catch Exception e
      [true {::error e}])))

(defn groups-action-handler
  [conn task-constraints is-authorized-fn]
  (base-cook-handler
    {:allowed-methods [:get :delete]
     :malformed? (partial retrieve-groups conn)
     :allowed? (fn [ctx]
                 (let [user (get-in ctx [:request :authorization/user])
                       guuids (::guuids ctx)
                       group-user (fn [guuid] (-> (d/entity (d/db conn) [:group/uuid guuid])
                                                  :group/job first :job/user))
                       request-method (get-in ctx [:request :request-method])
                       impersonator (get-in ctx [:request :authorization/impersonator])
                       authorized? (fn [guuid] (is-authorized-fn user
                                                                 request-method
                                                                 impersonator
                                                                 {:owner (group-user guuid) :item :job}))
                       {authorized-guuids true unauthorized-guuids false} (group-by authorized? guuids)]
                   (if (or (empty? unauthorized-guuids) (::allow-partial-results? ctx))
                     [true {::guuids (with-meta
                                       (vec authorized-guuids)
                                       ; Liberator will concatenate vectors by default when composing context values,
                                       ; but we want to replace our vector with only the filtered entries.
                                       {:replace true})}]
                     [false {::error (str "You are not authorized to "
                                          (case request-method
                                            :get "view"
                                            :delete "kill")
                                          " the following groups "
                                          (str/join \space unauthorized-guuids))}])))
     :exists? (fn [ctx]
                (or (::allow-partial-results? ctx)
                    (empty? (::non-existing-guuids ctx))
                    [false {::error (str "The following UUIDs didn't correspond to a group: "
                                         (str/join \space (::non-existing-guuids ctx)))}]))
     :handle-ok (fn [ctx]
                  (if (Boolean/valueOf (get-in ctx [:request :query-params "detailed"]))
                    (mapv #(merge (fetch-group-map (d/db conn) %)
                                  (fetch-group-job-details (d/db conn) %))
                          (::guuids ctx))
                    (mapv #(fetch-group-map (d/db conn) %) (::guuids ctx))))
     :delete! (fn [ctx]
                (let [jobs (mapcat #(fetch-group-live-jobs (d/db conn) %)
                                   (::guuids ctx))
                      juuids (mapv :job/uuid jobs)]
                  (cook.mesos/kill-job conn juuids)))}))

;;
;; /queue

(def leader-hostname-regex #"^([^#]*)#([0-9]*)#([a-z]*)#.*")

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
               (let [user (get-in ctx [:request :authorization/user])
                     impersonator (get-in ctx [:request :authorization/impersonator])]
                 (if (is-authorized-fn user :read impersonator {:owner ::system :item :queue})
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
                                     leader-port (.group leader-match 2)
                                     leader-protocol (.group leader-match 3)]
                                 [true {:location (str leader-protocol "://" leader-hostname ":" leader-port "/queue")}])
                               (throw (IllegalStateException.
                                       (str "Unable to parse leader id: " leader-id)))))))
   :handle-forbidden (fn [ctx]
                       (log/info (get-in ctx [:request :authorization/user]) " is not authorized to access queue")
                       (render-error ctx))
   :handle-malformed render-error
   :handle-ok (fn [ctx]
                (pc/map-vals (fn [queue]
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
               (let [user (get-in ctx [:request :authorization/user])
                     impersonator (get-in ctx [:request :authorization/impersonator])]
                 (if (is-authorized-fn user :read impersonator {:owner ::system :item :running})
                   true
                   [false {::error "Unauthorized"}])))
   :handle-forbidden render-error
   :handle-malformed render-error
   :handle-ok (fn [ctx]
                (->> (util/get-running-task-ents (d/db conn))
                     (take (::limit ctx))
                     (map d/touch)))))

;;
;; /retry
;;

(def ReadRetriesRequest
  {:job [s/Uuid]})

;; Base retry options (used for both PUT and POST).
(def UpdateRetriesRequestBase
  {(s/optional-key :job) s/Uuid
   (s/optional-key :jobs) [s/Uuid]
   (s/optional-key :retries) PosNum
   (s/optional-key :increment) PosNum})

;; Since POST is deprecated, we don't want to add any more features
;; (just continue supporting the base options for a retry)
(def UpdateRetriesRequestDeprecated UpdateRetriesRequestBase)

;; The PUT verb is able to support newer options on the retry endpoint.
(def UpdateRetriesRequest
  (merge UpdateRetriesRequestBase
         {(s/optional-key :groups) [s/Uuid]
          (s/optional-key :failed-only) s/Bool}))

(defn display-retries
  [conn ctx]
  (:job/max-retries (d/entity (d/db conn) [:job/uuid (-> ctx ::jobs first)])))

(defn job-failed?
  "Checks if the specified job is in a failed state."
  [db juuid]
  (->> [:job/uuid juuid] (d/entity db) util/job-ent->state (= "failed")))

(defn jobs-from-request
  "Reads a set of job UUIDs from the request, supporting \"job\" in the query,
  and either \"job\" or \"jobs\" in the body, while accommodating some aspects of
  liberator and compojure-api. For example, compojure-api doesn't support
  applying a schema to a combination of query parameters and body parameters.
  Returns the cached value stored in the context under the ::jobs key if
  the jobs were previously stored there by an earlier liberator context update."
  [ctx]
  (or (::jobs ctx)
      (vectorize (get-in ctx [:request :query-params :job]))
      ;; if the query params are coerceable, both :job and "job" keys will be
      ;; present, but in that case we only want to read the coerced version
      (when-let [uncoerced-query-params (get-in ctx [:request :query-params "job"])]
        (mapv #(UUID/fromString %) (vectorize uncoerced-query-params)))
      (get-in ctx [:request :body-params :jobs])
      (vectorize (get-in ctx [:request :body-params :job]))))

(defn check-jobs-exist
  [conn ctx]
  (let [db (d/db conn)
        jobs (jobs-from-request ctx)
        jobs-not-existing (remove (partial job-exists? db) jobs)]
    (cond
      ; error case
      (seq jobs-not-existing)
      [false {::error (->> jobs-not-existing
                           (map #(str "UUID " % " does not correspond to a job."))
                           (str/join \space))}]

      ; remove non-failed jobs if necessary
      (::failed-only? ctx)
      [true {::jobs (with-meta
                      (filterv (partial job-failed? db) (::jobs ctx))
                      ; Liberator will concatenate vectors by default when composing context values,
                      ; but we want to replace our vector with only the filtered entries.
                      {:replace true})}]

      ; if the context doesn't already have ::jobs, add it
      (nil? (::jobs ctx))
      [true {::jobs jobs}]

      ; otherwise, no update needed
      :else true)))

(defn check-groups-exist
  "Check if all of the group UUIDs provided via ::groups are valid."
  [conn ctx]
  (let [guuids (::groups ctx)
        bad-guuids (remove #(d/entity (d/db conn) [:group/uuid %]) guuids)]
    (if (seq bad-guuids)
      [false {::error (->> bad-guuids
                           (map #(str "UUID " % " does not correspond to a group."))
                           (str/join \space))}]
      true)))

(defn check-jobs-and-groups-exist
  "Check if all of the ::jobs and ::groups in the context are valid."
  [conn ctx]
  (let [[jobs-ok? jobs-ctx' :as jobs-result] (vectorize (check-jobs-exist conn ctx))]
    (if-not jobs-ok?
      jobs-result
      (let [[groups-ok? groups-ctx' :as groups-result] (vectorize (check-groups-exist conn ctx))]
        (cond
          (not groups-ok?) groups-result
          (not (or jobs-ctx' groups-ctx')) true
          (and jobs-ctx' groups-ctx') [true (combine jobs-ctx' groups-ctx')]
          :else [true (or jobs-ctx' groups-ctx')])))))

(defn validate-retries
  [conn task-constraints ctx]
  (let [retries (get-in ctx [:request :body-params :retries])
        increment (get-in ctx [:request :body-params :increment])
        jobs (jobs-from-request ctx)
        groups (get-in ctx [:request :body-params :groups])
        has-groups? (-> groups seq boolean)
        failed-only? (let [fo? (get-in ctx [:request :body-params :failed-only])]
                       ;; Default to true if there are groups, false if only jobs.
                       ;; This maintains backwards-compatibility for old code,
                       ;; but gives a more sane default for code where groups are used.
                       (if (nil? fo?) has-groups? fo?))
        retry-limit (:retry-limit task-constraints)]
    (cond
      (and (empty? jobs) (empty? groups))
      [true {:error "Need to specify at least 1 job or group."}]

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

      (and retries (let [db (d/db conn)]
                     (some (fn [job]
                             (> (d/invoke db :job/attempts-consumed db (d/entity db [:job/uuid job]))
                                retries))
                           jobs)))
      [true {::error (str "Retries would be less than attempts-consumed")}]

      :else
      (let [db (d/db conn)
            group-jobs (for [guuid groups
                             :let [group (d/entity db [:group/uuid guuid])]
                             job (:group/job group)]
                         (:job/uuid job))
            all-jobs (vec (concat jobs group-jobs))
            ctx' {::retries retries
                  ::increment increment
                  ::failed-only? failed-only?
                  ::jobs all-jobs
                  ::non-group-jobs jobs
                  ::groups groups}]
        [false ctx']))))

(defn user-can-retry-job?
  [conn is-authorized-fn request-user impersonator job]
  (let [job-owner (:job/user (d/entity (d/db conn) [:job/uuid job]))]
    (is-authorized-fn request-user :retry impersonator {:owner job-owner :item :job})))

(defn check-retry-allowed
  [conn is-authorized-fn ctx]
  (let [request-user (get-in ctx [:request :authorization/user])
        impersonator (get-in ctx [:request :authorization/impersonator])
        unauthorized-job? #(not (user-can-retry-job? conn is-authorized-fn request-user impersonator %))]
    (or (first (for [guuid (::groups ctx)
                     :let [group (d/entity (d/db conn) [:group/uuid guuid])
                           job (-> group :group/job first)]
                     :when (some-> job :job/uuid unauthorized-job?)]
                 [false {::error (str "You are not authorized to retry jobs from group " guuid ".")}]))
        (when-let [unauthorized-jobs (->> ctx ::non-group-jobs (filter unauthorized-job?) seq)]
          [false {::error (->> unauthorized-jobs
                               (map #(str "You are not authorized to retry job " % "."))
                               (str/join \space))}])
        true)))

(defn check-retry-conflict
  "Checks whether a 409 conflict should be returned during a retry operation. This should occur when one of the jobs:
   - Is already completed and has already retried 'retries' times
   - Is not completed and already has 'retries' max-retries"
  [conn ctx]
  (let [jobs (::jobs ctx)
        retries (get-in ctx [:request :body-params :retries])
        db (d/db conn)]
    (if (not (nil? retries))
      (let [jobs-not-updated (filter (fn [uuid]
                                       (let [{:keys [job/state job/max-retries] :as job} (d/entity db [:job/uuid uuid])]
                                         (or (and (not (= :job.state/completed state))
                                                  (= retries max-retries))
                                             (and (= :job.state/completed state)
                                                  (<= retries (d/invoke db :job/attempts-consumed db job))))))
                                     jobs)]
        (if (not (empty? jobs-not-updated))
          [true {::error (str "Jobs will not retry: " (str/join ", " jobs-not-updated))}]
          false))
      false))) ; no conflict when incrementing

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
     :exists? (partial check-jobs-and-groups-exist conn)
     :malformed? (partial validate-retries conn task-constraints)
     :conflict? (partial check-retry-conflict conn)
     :handle-created (partial display-retries conn)
     :post! (partial retry-jobs! conn)}))

(defn put-retries-handler
  [conn is-authorized-fn task-constraints]
  (base-retries-handler
    conn is-authorized-fn
    {:allowed-methods [:put]
     :malformed? (partial validate-retries conn task-constraints)
     :exists? (partial check-jobs-and-groups-exist conn)
     :conflict? (partial check-retry-conflict conn)
     :put! (partial retry-jobs! conn)
     ;; :new? decides whether to respond with Created (true) or OK (false).
     :new? (comp seq ::jobs)
     :respond-with-entity? (constantly true)
     ;; :handle-ok and :handle-accepted both return the number of jobs to be retried,
     ;; but :handle-ok is only triggered when there are no failed jobs to retry.
     :handle-created (partial display-retries conn)
     :handle-ok (constantly 0)}))

;; /share and /quota
(def UserParam {:user s/Str})
(def ReasonParam {:reason s/Str})
(def UserLimitChangeParams (merge {(s/optional-key :pool) s/Str} UserParam ReasonParam))

(def UserLimitSchema
  {:cpus s/Num
   :gpus s/Num
   :mem s/Num
   (s/optional-key :count) s/Num})

(def UserLimitsResponse
  (assoc UserLimitSchema (s/optional-key :pools) {s/Str UserLimitSchema}))

(defn set-limit-params
  [limit-type]
  {:body-params (merge UserLimitChangeParams {limit-type {s/Keyword NonNegNum}})})

(defn- get-pool-limits [get-limit-fn db user]
  (pc/map-from-keys (fn [pool] (get-limit-fn db user pool))
                    (map :pool/name (pool/all-pools db))))

(defn retrieve-user-limit
  [get-limit-fn conn ctx]
  (let [user (or (get-in ctx [:request :query-params :user])
                 (get-in ctx [:request :body-params :user]))
        pool (or (get-in ctx [:request :query-params :pool])
                 (get-in ctx [:request :body-params :pool])
                 (get-in ctx [:request :headers "x-cook-pool"]))
        db (d/db conn)
        response (get-limit-fn db user pool)]
    (if (nil? pool)
      (let [pool-limits (get-pool-limits get-limit-fn db user)]
        (cond-> response
          (not (empty? pool-limits)) (assoc :pools pool-limits)))
      response)))

(defn check-limit-allowed
  [limit-type is-authorized-fn ctx]
  (let [request-user (get-in ctx [:request :authorization/user])
        impersonator (get-in ctx [:request :authorization/impersonator])
        request-method (get-in ctx [:request :request-method])]
    (if-not (is-authorized-fn request-user request-method impersonator {:owner ::system :item limit-type})
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
                                  (or (get-in ctx [:request :query-params :pool])
                                      (get-in ctx [:request :headers "x-cook-pool"]))
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
                     (or (get-in ctx [:request :body-params :pool])
                         (get-in ctx [:request :headers "x-cook-pool"]))
                     (get-in ctx [:request :body-params :reason])
                     (reduce into [] (::limits ctx))))}))

(def UserUsageParams
  "User usage endpoint query string parameters"
  (assoc UserParam
         (s/optional-key :group_breakdown) s/Bool))

(def UsageInfo
  "Schema for a dictionary of job resource usage info."
  {:cpus s/Num, :mem s/Num, :gpus s/Num, :jobs s/Num})

(def UsageGroupInfo
  "Helper-schema for :grouped jobs' group info in UserUsageResponse."
  {:uuid s/Uuid, :name s/Str, :running_jobs [s/Uuid]})

(def JobsUsageResponse
  "Helper-schema for :ungrouped jobs in UserUsageResponse."
  {:running_jobs [s/Uuid], :usage UsageInfo})

(def UserUsageInPool
  "Schema for a user's usage within a particular pool."
  {:total_usage UsageInfo
   (s/optional-key :grouped) [{:group UsageGroupInfo, :usage UsageInfo}]
   (s/optional-key :ungrouped) JobsUsageResponse})

(def UserUsageResponse
  "Schema for a usage response."
  (assoc UserUsageInPool
    (s/optional-key :pools) {s/Str UserUsageInPool}))

(def zero-usage
  "Resource usage map 'zero' value"
  (util/total-resources-of-jobs nil))

(defn user-usage
  "Given a collection of jobs, returns the usage information for those jobs."
  [with-group-breakdown? jobs]
  (merge
    ; basic user usage response
    {:total-usage (util/total-resources-of-jobs jobs)}
    ; (optional) user's total usage with breakdown by job groups
    (when with-group-breakdown?
      (let [breakdowns (->> jobs
                            (group-by util/job-ent->group-uuid)
                            (pc/map-vals (juxt #(mapv :job/uuid %)
                                            util/total-resources-of-jobs
                                            #(-> % first :group/_job first))))]
        {:grouped (for [[guuid [job-uuids usage group]] breakdowns
                        :when guuid]
                    {:group {:uuid (:group/uuid group)
                             :name (:group/name group)
                             :running-jobs job-uuids}
                     :usage usage})
         :ungrouped (let [[job-uuids usage] (get breakdowns nil)]
                      {:running-jobs job-uuids
                       :usage (or usage zero-usage)})}))))

(defn no-usage-map
  "Returns a resource usage map showing no usage"
  [with-group-breakdown?]
  (cond-> {:total-usage zero-usage}
          with-group-breakdown? (assoc :grouped []
                                       :ungrouped {:running-jobs []
                                                   :usage zero-usage})))

(defn get-user-usage
  "Query a user's current resource usage based on running jobs."
  [db ctx]
  (let [user (get-in ctx [:request :query-params :user])
        with-group-breakdown? (get-in ctx [:request :query-params :group_breakdown])
        pool (or (get-in ctx [:request :query-params :pool])
                 (get-in ctx [:request :headers "x-cook-pool"]))]
    (if pool
      (let [jobs (util/get-user-running-job-ents-in-pool db user pool)]
        (user-usage with-group-breakdown? jobs))
      (let [jobs (util/get-user-running-job-ents db user)
            pools (pool/all-pools db)]
        (if (pos? (count pools))
          (let [default-pool-name (config/default-pool)
                pool-name->jobs (group-by
                                  #(if-let [pool (:job/pool %)]
                                     (:pool/name pool)
                                     default-pool-name)
                                  jobs)
                no-usage (no-usage-map with-group-breakdown?)
                pool-name->usage (pc/map-vals (partial user-usage with-group-breakdown?) pool-name->jobs)
                pool-name->no-usage (into {} (map (fn [{:keys [pool/name]}] [name no-usage]) pools))
                total-usage (user-usage with-group-breakdown? jobs)]
            (assoc total-usage
              :pools (merge pool-name->no-usage pool-name->usage)))
          (user-usage with-group-breakdown? jobs))))))

(defn read-usage-handler
  "Handle GET requests for a user's current usage."
  [conn is-authorized-fn]
  (base-limit-handler
    :usage is-authorized-fn
    {:handle-ok (partial get-user-usage (d/db conn))}))


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
    (contains? (meta v) :json-value) (-> v meta :json-value str)
    (fn? v) (str v)
    (map? v) (pc/map-vals stringify v)
    (instance? Atom v) (stringify (deref v))
    (instance? TestingServer v) (str v)
    (instance? Minutes v) (str v)
    (instance? ServerSocket v) (str v)
    (instance? Var v) (str v)
    (instance? ScheduledReporter v) (str v)
    (instance? VMTaskFitnessCalculator v) (.getName v)
    :else v))

(defn settings-handler
  [settings]
  (base-cook-handler
    {:allowed-methods [:get]
     :handle-ok (fn [_] (stringify settings))}))

(defn- parse-long-default
  [s d]
  (if (nil? s)
    d
    (Long/parseLong s)))

(defn list-resource
  [db framework-id is-authorized-fn]
  (liberator/resource
    :available-media-types ["application/json"]
    :allowed-methods [:get]
    :as-response (fn [data _] {:body data})
    :malformed? (fn [ctx]
                  ;; since-hours-ago is included for backwards compatibility but is deprecated
                  ;; please use start-ms and end-ms instead
                  (let [{:keys [state user since-hours-ago start-ms end-ms limit name]}
                        (keywordize-keys (or (get-in ctx [:request :query-params])
                                             (get-in ctx [:request :body-params])))
                        states (when state (set (str/split state #"\+")))]
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
                                ::since-hours-ago (util/parse-int-default since-hours-ago 24)
                                ::start-ms (parse-long-default start-ms nil)
                                ::limit (util/parse-int-default limit Integer/MAX_VALUE)
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
                     request-user (get-in ctx [:request :authorization/user])
                     impersonator (get-in ctx [:request :authorization/impersonator])]
                 (cond
                   (not (is-authorized-fn request-user :get impersonator {:owner user :item :job}))
                   [false {::error (str "You are not authorized to list jobs for " user)}]

                   (not (<= 0 since-hours-ago 168))
                   [false {::error (str "since-hours-ago must be in the interval [0, 168], (168 hours = 7 days)")}]

                   (> 1 limit)
                   [false {::error (str "limit must be positive")}]

                   (and start-ms (> start-ms end-ms))
                   [false {::error (str "start-ms (" start-ms ") must be before end-ms (" end-ms ")")}]

                   :else true)))
    :handle-malformed ::error
    :handle-forbidden ::error
    :handle-ok (fn [ctx]
                 (timers/time!
                   (timers/timer ["cook-scheduler" "handler" "list-endpoint-duration"])
                   (let [job-uuids (list-jobs db false ctx)]
                     (mapv (partial fetch-job-map db framework-id) job-uuids))))))

;;
;; /unscheduled_jobs
;;

(def UnscheduledJobParams
  "Schema for getting the reason(s) for job(s) not being scheduled, allowing optionally
  for 'partial' results, meaning that some uuids can be valid and others not"
  {:job [s/Uuid]
   (s/optional-key :partial) s/Bool})

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
     :exists? (partial retrieve-jobs conn)
     :allowed? (partial job-request-allowed? conn is-authorized-fn)
     :handle-ok (fn [ctx]
                  (map (fn [job] {:uuid job
                                  :reasons (job-reasons conn job)})
                       (::jobs ctx)))}))

;;
;; /stats/instances
;;

(def HistogramStats
  {:percentiles {(s/required-key 50) s/Num
                 (s/required-key 75) s/Num
                 (s/required-key 95) s/Num
                 (s/required-key 99) s/Num
                 (s/required-key 100) s/Num}
   :total NonNegNum})

(def TaskStats
  {(s/optional-key :count) NonNegInt
   (s/optional-key :cpu-seconds) HistogramStats
   (s/optional-key :mem-seconds) HistogramStats
   (s/optional-key :run-time-seconds) HistogramStats})

(def UserLeaders
  {s/Str NonNegNum})

(def TaskStatsResponse
  {:by-reason {s/Any TaskStats}
   :by-user-and-reason {s/Str {s/Any TaskStats}}
   :leaders {:cpu-seconds UserLeaders
             :mem-seconds UserLeaders}
   :overall TaskStats})

(def TaskStatsParams
  {:status s/Str
   :start s/Str
   :end s/Str
   (s/optional-key :name) s/Str})

(timers/deftimer [cook-scheduler handler stats-instances-endpoint])

(defn task-stats-handler
  [conn is-authorized-fn]
  (base-cook-handler
    {:allowed-methods [:get]
     :allowed?
     (fn [ctx]
       (let [user (get-in ctx [:request :authorization/user])
             impersonator (get-in ctx [:request :authorization/impersonator])]
         (if (is-authorized-fn user :read impersonator {:owner ::system :item :stats})
           true
           [false {::error "Unauthorized"}])))
     :malformed?
     (fn [ctx]
       (let [{:keys [status start end name]} (-> ctx :request :params)
             allowed-instance-statuses #{"unknown" "running" "success" "failed"}
             start-time (util/parse-time start)
             end-time (util/parse-time end)]
         (cond
           (not (allowed-instance-statuses status))
           [true {::error (str "unsupported status " status ", must be one of: " allowed-instance-statuses)}]

           (not (valid-name-filter? name))
           [true {::error
                  (str "unsupported name filter " name
                       ", can only contain alphanumeric characters, '.', '-', '_', and '*' as a wildcard")}]

           (not (t/after? end-time start-time))
           [true {::error "end time must be after start time"}]

           (< 31 (t/in-days (t/interval start-time end-time)))
           [true {::error (str "time interval must be less than or equal to 31 days")}]

           :else
           (try
             [false {::status (keyword "instance.status" status)
                     ::start start-time
                     ::end end-time
                     ::name-filter-fn (name-filter-str->name-filter-fn name)}]
             (catch NumberFormatException e
               [true {::error (.toString e)}])))))
     :handle-ok
     (fn [ctx]
       (timers/time!
         stats-instances-endpoint
         (let [{status ::status, start ::start, end ::end, name-filter-fn ::name-filter-fn} ctx]
           (task-stats/get-stats conn status start end name-filter-fn))))}))

;;
;; /pools
;;

(def PoolsResponse
  [{:name s/Str
    :purpose s/Str
    :state s/Str}])

(defn pool-entity->consumable-map
  "Converts the given pool entity to a 'consumable' map"
  [e]
  {:name (:pool/name e)
   :purpose (:pool/purpose e)
   :state (name (:pool/state e))})

(defn pools-handler
  "Handler for retrieving pools"
  []
  (base-cook-handler
    {:allowed-methods [:get]
     :handle-ok (fn [_]
                  (let [db (d/db datomic/conn)]
                    (->> (pool/all-pools db)
                         (mapv pool-entity->consumable-map))))}))

(def DataLocalUpdateTimeResponse
  {s/Uuid (s/maybe s/Str)})

(def DataLocalCostResponse
  {s/Str s/Num})

(defn data-local-update-time-handler
  "Handler for return the last update time of data locality"
  [conn]
  (base-cook-handler
   {:allowed-methods [:get]
    ; TODO (pschorf) - cache this if it is a performance bottleneck
    :handle-ok (fn [_]
                 (let [uuid->datasets (->> (d/db conn)
                                          util/get-pending-job-ents 
                                          (filter (fn [j] (not (empty? (:job/datasets j)))))
                                          (map (fn [j] [(:job/uuid j) (dl/get-dataset-maps j)]))
                                          (into {}))
                       datasets->update-time (dl/get-last-update-time)]
                   (pc/map-vals (fn [datasets]
                                  (when-let [update-time (get datasets->update-time datasets nil)]
                                    (tf/unparse iso-8601-format update-time)))
                                uuid->datasets)))}))

(defn data-local-cost-handler
  "Handler which returns the data locality costs for a given job"
  [conn]
  (base-cook-handler
   {:allowed-methods [:get]
    :exists? (fn [ctx]
               (let [uuid (get-in ctx [:request :params :uuid])
                     job-ent (d/entity (d/db conn) [:job/uuid (UUID/fromString uuid)])
                     datasets (dl/get-dataset-maps job-ent)]
                 (if-let [costs (get (dl/get-data-local-costs) datasets nil)]
                   {:costs costs}
                   false)))
    :handle-ok (fn [{:keys [costs]}]
                 costs)}))

(defn- streaming-json-encoder
  "Takes as input the response body which can be converted into JSON,
  and returns a function which takes a ServletResponse and writes the JSON
  encoded response data. This is suitable for use with jet's server API."
  [data]
  (fn streaming-json-encoder-fn [^ServletResponse resp]
    (cheshire/generate-stream data
                              (OutputStreamWriter. (.getOutputStream resp)))))

(defn- streaming-json-middleware
  "Ring middleware which encodes JSON responses with streaming-json-encoder.
  All other response types are unaffected."
  [handler]
  (fn streaming-json-handler [req]
    (let [{:keys [headers body] :as resp} (handler req)
          json-response (or (and (= "application/json" (get headers "Content-Type"))
                                 (not (string? body)))
                            (coll? body))]
      (cond-> resp
        json-response (res/content-type "application/json")
        (and json-response body) (assoc :body (streaming-json-encoder body))))))

(def cook-coercer
  "This coercer adds to compojure-api's default-coercion-matchers by
  converting keys from snake_case to kebab-case for requests and from
  kebab-case to snake_case for responses. For example, this allows clients to
  pass 'max_retries' when submitting a job, even though the job schema has
  :max-retries (with a dash instead of an underscore). For more information on
  coercion in compojure-api, see:

    https://github.com/metosin/compojure-api/wiki/Validation-and-coercion"
  (let [merged-matchers (fn [our-matchers]
                          (fn [their-matchers]
                            (fn [s]
                              (or (their-matchers s) (our-matchers s)))))
        body-matchers (merged-matchers
                        {;; can't use form->kebab-case because env and label
                         ;; accept arbitrary kvs
                         JobRequestMap (partial pc/map-keys ->kebab-case)
                         Group (partial pc/map-keys ->kebab-case)
                         HostPlacement (fn [hp]
                                         (update hp :type keyword))
                         UpdateRetriesRequest (partial pc/map-keys ->kebab-case)
                         StragglerHandling (fn [sh]
                                             (update sh :type keyword))})
        resp-matchers (merged-matchers
                        {JobResponseDeprecated (partial pc/map-keys ->snake_case)
                         JobResponse (partial pc/map-keys ->snake_case)
                         GroupResponse (partial pc/map-keys ->snake_case)
                         UserUsageResponse (partial pc/map-keys ->snake_case)
                         UserUsageInPool (partial pc/map-keys ->snake_case)
                         UsageGroupInfo (partial pc/map-keys ->snake_case)
                         JobsUsageResponse (partial pc/map-keys ->snake_case)
                         s/Uuid str})]
    (constantly
      (-> c-mw/default-coercion-matchers
          (update :body body-matchers)
          (update :response resp-matchers)))))

;;
;; "main" - the entry point that routes to other handlers
;;
(defn main-handler
  [conn framework-id mesos-pending-jobs-fn
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
            {:get {:summary "Returns info about a set of Jobs (deprecated)"
                   :parameters {:query-params QueryJobsParamsDeprecated}
                   :responses {200 {:schema [JobResponseDeprecated]
                                    :description "The jobs and their instances were returned."}
                               400 {:description "Non-UUID values were passed as jobs."}
                               403 {:description "The supplied UUIDs don't correspond to valid jobs."}}
                   :handler (read-jobs-handler-deprecated conn framework-id is-authorized-fn)}
             :post {:summary "Schedules one or more jobs (deprecated)."
                    :parameters {:body-params RawSchedulerRequestDeprecated}
                    :responses {201 {:description "The jobs were successfully scheduled."}
                                400 {:description "One or more of the jobs were incorrectly specified."}
                                409 {:description "One or more of the jobs UUIDs are already in use."}}
                    :handler (create-jobs-handler conn task-constraints gpu-enabled? is-authorized-fn)}
             :delete {:summary "Cancels jobs, halting execution when possible."
                      :responses {204 {:description "The jobs have been marked for termination."}
                                  400 {:description "Non-UUID values were passed as jobs."}
                                  403 {:description "The supplied UUIDs don't correspond to valid jobs."}}
                      :parameters {:query-params JobOrInstanceIds}
                      :handler (destroy-jobs-handler conn is-authorized-fn)}}))

        (c-api/context
          "/jobs/:uuid" [uuid]
          :path-params [uuid :- s/Uuid]
          (c-api/resource
            {:get {:summary "Returns info about a single Job"
                   :responses {200 {:schema JobResponse
                                    :description "The job was returned."}
                               400 {:description "A non-UUID value was passed."}
                               403 {:description "The supplied UUID doesn't correspond to a valid job."}}
                   :handler (read-jobs-handler-single conn framework-id is-authorized-fn)}}))

        (c-api/context
          "/jobs" []
          (c-api/resource
            {:get {:summary "Returns info about a set of Jobs"
                   :parameters {:query-params QueryJobsParams}
                   :responses {200 {:schema [JobResponse]
                                    :description "The jobs were returned."}
                               400 {:description "Non-UUID values were passed."}
                               403 {:description "The supplied UUIDs don't correspond to valid jobs."}}
                   :handler (read-jobs-handler-multiple conn framework-id is-authorized-fn)}
             :post {:summary "Schedules one or more jobs."
                    :parameters {:body-params JobSubmissionRequest}
                    :responses {201 {:description "The jobs were successfully scheduled."}
                                400 {:description "One or more of the jobs were incorrectly specified."}
                                409 {:description "One or more of the jobs UUIDs are already in use."}}
                    :handler (create-jobs-handler conn task-constraints gpu-enabled? is-authorized-fn)}}))
        (c-api/context
          "/info" []
          (c-api/resource
            ;; NOTE: The authentication for this endpoint is disabled via cook.components/conditional-auth-bypass
            {:get {:summary "Returns info about this Cook Scheduler instance's setup. No authentication required."
                   :responses {200 {:schema CookInfo
                                    :description "The Cook Scheduler info was returned."}}
                   :handler (cook-info-handler settings)}}))

        (c-api/context
          "/instances/:uuid" [uuid]
          :path-params [uuid :- s/Uuid]
          (c-api/resource
            {:get {:summary "Returns info about a single Job Instance"
                   :responses {200 {:schema InstanceResponse
                                    :description "The job instance was returned."}
                               400 {:description "A non-UUID value was passed."}
                               403 {:description "The supplied UUID doesn't correspond to a valid job instance."}}
                   :handler (read-instances-handler-single conn framework-id is-authorized-fn)}}))

        (c-api/context
          "/instances" []
          (c-api/resource
            {:get {:summary "Returns info about a set of Job Instances"
                   :parameters {:query-params QueryInstancesParams}
                   :responses {200 {:schema [InstanceResponse]
                                    :description "The job instances were returned."}
                               400 {:description "Non-UUID values were passed."}
                               403 {:description "The supplied UUIDs don't correspond to valid job instances."}}
                   :handler (read-instances-handler-multiple conn framework-id is-authorized-fn)}}))

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
                      :parameters {:query-params UserLimitChangeParams}
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
                      :parameters {:query-params UserLimitChangeParams}
                      :handler (destroy-limit-handler :delete quota/retract-quota! conn is-authorized-fn)}}))

        (c-api/context
          "/usage" []
          (c-api/resource
            {:produces ["application/json"],
             :responses {200 {:schema UserUsageResponse
                              :description "User's usage calculated."}
                         401 {:description "Not authorized to read usage."}
                         403 {:description "Invalid request format."}}
             :get {:summary "Query a user's current resource usage."
                   :parameters {:query-params UserUsageParams}
                   :handler (read-usage-handler conn is-authorized-fn)}}))

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
              :responses {200 {:schema ZeroInt
                               :description "No failed jobs provided to retry."}
                          201 {:schema PosInt
                               :description "The number of retries for the jobs."}
                          400 {:description "Invalid request format."}
                          401 {:description "Request user not authorized to access those jobs."}
                          404 {:description "Unrecognized job UUID."}}}
             :post
             {:summary "Change a job's retry count (deprecated)"
              :parameters {:body-params UpdateRetriesRequestDeprecated}
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
                   :handler (groups-action-handler conn task-constraints is-authorized-fn)}
             :delete
             {:summary "Kill all jobs within a set of groups"
              :parameters {:query-params KillGroupsParams}
              :responses {204 {:description "The groups' jobs have been marked for termination."}
                          400 {:description "Non-UUID values were passed."}
                          403 {:description "The supplied UUIDs don't correspond to valid groups."}}
              :handler (groups-action-handler conn task-constraints is-authorized-fn)}}))

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
                   :parameters {:query-params UnscheduledJobParams}
                   :handler (read-unscheduled-handler conn is-authorized-fn)
                   :responses {200 {:schema UnscheduledJobResponse
                                    :description "Reasons the job isn't being scheduled."}
                               400 {:description "Invalid request format."}
                               404 {:description "The UUID doesn't correspond to a job."}}}}))

        (c-api/context
          "/stats/instances" []
          (c-api/resource
            {:produces ["application/json"],
             :responses {200 {:schema TaskStatsResponse
                              :description "Task stats calculated."}
                         401 {:description "Not authorized to read stats."}
                         403 {:description "Invalid request format."}}
             :get {:summary "Query statistics for instances started in a time range."
                   :parameters {:query-params TaskStatsParams}
                   :handler (task-stats-handler conn is-authorized-fn)}}))

        (c-api/context
          "/pools" []
          (c-api/resource
            {:produces ["application/json"],
             :responses {200 {:schema PoolsResponse
                              :description "The pools were returned."}}
             :get {:summary "Returns the pools."
                   :handler (pools-handler)}})))

      ; Data locality debug endpoints
      (c-api/context
       "/data-local/:uuid" []
       :path-params [uuid :- s/Uuid]
       (c-api/resource
        {:produces ["application/json"]
         :responses {200 {:schema DataLocalCostResponse}}
         :get {:summary "Returns summary information on the current data locality status"
               :handler (data-local-cost-handler conn)}}))

      (c-api/context
       "/data-local" []
       (c-api/resource
        {:produces ["application/json"]
         :responses {200 {:schema DataLocalUpdateTimeResponse}}
         :get {:summary "Returns summary information on the current data locality status"
               :handler (data-local-update-time-handler conn)}}))

      (ANY "/queue" []
        (waiting-jobs mesos-pending-jobs-fn is-authorized-fn mesos-leadership-atom leader-selector))
      (ANY "/running" []
        (running-jobs conn is-authorized-fn))
      (ANY "/list" []
        (list-resource (d/db conn) framework-id is-authorized-fn)))
    (format-params/wrap-restful-params {:formats [:json-kw]
                                        :handle-error c-mw/handle-req-error})
    (streaming-json-middleware)))
