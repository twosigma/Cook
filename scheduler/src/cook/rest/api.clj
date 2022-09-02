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
(ns cook.rest.api
  (:require [better-cond.core :as b]
            [camel-snake-kebab.core :refer [->kebab-case ->snake_case]]
            [cheshire.core :as cheshire]
            [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clj-time.format :as tf]
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk :refer [keywordize-keys]]
            [compojure.api.middleware :as c-mw]
            [compojure.api.sweet :as c-api]
            [compojure.core :refer [ANY GET POST routes]]
            [cook.cache :as ccache]
            [cook.caches :as caches]
            [cook.cached-queries :as cached-queries]
            [cook.compute-cluster :as cc]
            [cook.config :as config]
            [cook.config-incremental :as config-incremental]
            [cook.datomic :as datomic]
            [cook.mesos]
            [cook.mesos.reason :as reason]
            [cook.passport :as passport]
            [cook.plugins.adjustment :as adjustment]
            [cook.plugins.job-submission-modifier :as job-submission-plugin]
            [cook.plugins.definitions :as plugins]
            [cook.plugins.file :as file-plugin]
            [cook.plugins.submission :as submission-plugin]
            [cook.pool :as pool]
            [cook.progress :as progress]
            [cook.prometheus-metrics :as prom]
            [cook.regexp-tools :as regexp-tools]
            [cook.queries :as queries]
            [cook.queue-limit :as queue-limit]
            [cook.quota :as quota]
            [cook.rate-limit :as rate-limit]
            [cook.scheduler.share :as share]
            [cook.schema :refer [constraint-operators host-placement-types straggler-handling-types]]
            [cook.task :as task]
            [cook.task-stats :as task-stats]
            [cook.tools :as util]
            [cook.unscheduled :as unscheduled]
            [cook.util :refer [NonEmptyString NonNegInt NonNegNum PosDouble PosInt PosNum UserName ZeroInt]]
            [datomic.api :as d :refer [q]]
            [liberator.core :as liberator]
            [liberator.util :refer [combine]]
            [mesomatic.scheduler]
            [metatransaction.core :refer [db]]
            [metrics.histograms :as histograms]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [ring.middleware.format-params :as format-params]
            [ring.util.response :as res]
            [schema.core :as s]
            [schema.macros :as macros]
            [schema.spec.leaf :as leaf]
            [swiss.arrows :refer :all])
  (:import (clojure.lang Atom Var)
           (com.codahale.metrics ScheduledReporter)
           (com.netflix.fenzo VMTaskFitnessCalculator)
           (java.io OutputStreamWriter)
           (java.net ServerSocket)
           (java.util Date UUID)
           (javax.servlet ServletResponse)
           (org.apache.curator.framework.recipes.leader LeaderSelector)
           (org.apache.curator.test TestingServer)
           (org.joda.time DateTime Minutes)
           (schema.core OptionalKey Schema)))

;; We use Liberator to handle requests on our REST endpoints.
;; The control flow among Liberator's handler functions is described here:
;; https://clojure-liberator.github.io/liberator/doc/decisions.html

(defn render-error
  [{:keys [::error request]}]
  (try
    (let [{:keys [params remote-addr :authorization/user uri request-method]} request]
      (log/info "Handling error" {:error error
                                  :params params
                                  :remote-addr remote-addr
                                  :user user
                                  :uri uri
                                  :request-method request-method}))
    (catch Exception e
      (log/error e "Error while logging in render-error")))
  {:error error})

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

(s/defschema CookInfo
  "Schema for the /info endpoint response"
  {:authentication-scheme s/Str
   :commit s/Str
   :start-time s/Inst
   :version s/Str
   :leader-url s/Str})

(def PortMapping
  "Schema for Docker Portmapping"
  {:host-port (s/both s/Int (s/pred #(<= 0 % 65536) 'between-0-and-65536))
   :container-port (s/both s/Int (s/pred #(<= 0 % 65536) 'between-0-and-65536))
   (s/optional-key :protocol) s/Str})

(def DockerInfo
  "Schema for a DockerInfo"
  {(s/optional-key :image) s/Str
   (s/optional-key :network) s/Str
   (s/optional-key :force-pull-image) s/Bool
   (s/optional-key :parameters) [{:key s/Str :value s/Str}]
   (s/optional-key :port-mapping) [PortMapping]})

(def MesosInfo
  "Schema for mesos container info"
  {:image s/Str})

(def Volume
  "Schema for a Volume"
  {(s/optional-key :container-path) s/Str
   :host-path s/Str
   (s/optional-key :mode) s/Str})

(def Container
  "Schema for a Mesos Container"
  {:type s/Str
   (s/optional-key :docker) DockerInfo
   (s/optional-key :mesos) MesosInfo
   (s/optional-key :volumes) [Volume]})

(def CheckpointOptions
  "Schema for checkpointing options"
  {:preserve-paths #{s/Str}})

(def PeriodicCheckpointOptions
  "Schema for periodic checkpointing options"
  {:period-sec s/Int})

(def Checkpoint
  "Schema for a configuration to enable checkpointing"
  ; auto - checkpointing code will select the best method
  ; periodic - periodically create a checkpoint
  ; preemption - checkpoint is created on preemption before the VM is stopped
  {:mode (s/enum "auto" "periodic" "preemption")
   (s/optional-key :options) CheckpointOptions
   (s/optional-key :periodic-options) PeriodicCheckpointOptions})

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

(def MesosComputeCluster
  "Schema for a Mesos compute cluster"
  {:framework-id s/Str})

(def ComputeCluster
  "Schema for a compute cluster"
  {:name s/Str
   :type s/Keyword
   (s/optional-key :mesos) MesosComputeCluster})

(def Instance
  "Schema for a description of a single job instance."
  {:status s/Str
   :task_id s/Uuid
   :executor_id s/Uuid
   :preempted s/Bool
   :backfilled s/Bool
   :ports [s/Int]
   :compute-cluster ComputeCluster
   (s/optional-key :agent_id) s/Str
   (s/optional-key :slave_id) s/Str
   (s/optional-key :hostname) s/Str
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
   (s/optional-key :sandbox_directory) s/Str
   (s/optional-key :file_url) s/Str
   (s/optional-key :queue_time) s/Int})

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

(def valid-k8s-pod-label-value-regex
  #"([a-zA-Z0-9]{1,2}|[a-zA-Z0-9][\.a-zA-Z0-9_-]{0,61}[a-zA-Z0-9])?")

(defn valid-k8s-pod-label-value?
  "Returns true if s contains only '.', '_', '-' or any word or
  number characters, has length at least 2 and at most 63, and
  begins and ends with a word or number character. This is based
  on the validation for k8s pod label values:
  https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set"
  [s]
  (re-matches valid-k8s-pod-label-value-regex s))

(defn valid-job-label-value?
  "If the label has the (:add-job-label-to-pod-prefix (config/kubernetes)) prefix, then we check that
  the value is a valid-k8s-pod-label-value?"
  [[label value]]
  (if-let [add-job-label-to-pod-prefix (:add-job-label-to-pod-prefix (config/kubernetes))]
    (or
      (not (str/starts-with? label add-job-label-to-pod-prefix))
      (valid-k8s-pod-label-value? value))
    true))

(defn validate-job-labels
  "Given job labels, return an error message if they are not valid"
  [job-labels]
  (if-not (map? job-labels)
    (str job-labels " is not a map")
    (let [invalid-job-labels (vec (filter #(not (valid-job-label-value? %)) job-labels))]
      (if-not (empty? invalid-job-labels)
        (str "The following job labels have the pod prefix `" (:add-job-label-to-pod-prefix (config/kubernetes))
             "` but don't match the regex " valid-k8s-pod-label-value-regex ": " invalid-job-labels)))))

(defrecord JobLabelsSchema []
  Schema
  (spec [this] (leaf/leaf-spec
                 (fn [value]
                   (when-let [error (macros/try-catchall (validate-job-labels value) (catch e# 'throws?))]
                     (macros/validation-error this value nil error)))))
  (explain [this] (str "Labels with keys starting with " (:add-job-label-to-pod-prefix (config/kubernetes))
                       " must have values that match " valid-k8s-pod-label-value-regex)))

(def Application
  "Schema for the application a job corresponds to"
  {:name (s/constrained s/Str non-empty-max-128-characters-and-alphanum?)
   :version (s/constrained s/Str non-empty-max-128-characters-and-alphanum?)
   (s/optional-key :workload-class) (s/constrained s/Str valid-k8s-pod-label-value?)
   (s/optional-key :workload-id) (s/constrained s/Str valid-k8s-pod-label-value?)
   (s/optional-key :workload-details) (s/constrained s/Str valid-k8s-pod-label-value?)})

(def Disk
  "Schema for disk limit specifications"
  {:request (s/pred #(> % 1.0) 'greater-than-one)
   (s/optional-key :limit) (s/pred #(> % 1.0) 'greater-than-one)
   (s/optional-key :type) s/Str})

(def Constraint
  "Schema for user defined job host constraint"
  [(s/one (s/constrained s/Str valid-k8s-pod-label-value? 'valid-k8s-pod-label-value?) "attribute")
   (s/one (s/pred #(contains? (set (map name constraint-operators))
                              (str/lower-case %))
                  'constraint-operator-exists?)
          "operator")
   (s/one (s/constrained s/Str valid-k8s-pod-label-value? 'valid-k8s-pod-label-value?) "pattern")])

(s/defschema JobName
  (s/both s/Str (s/both s/Str (s/pred max-128-characters-and-alphanum? 'max-128-characters-and-alphanum?))))

(s/defschema JobNameListFilter
  (s/both s/Str (s/pred (fn [s]
                          (re-matches #"[\.a-zA-Z0-9_\-\*]*" s)))))

(s/defschema JobPriority
  (s/both s/Int (s/pred #(<= 0 % 16000000) 'between-0-and-16000000)))

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
   (s/optional-key :labels) (JobLabelsSchema.)
   (s/optional-key :constraints) [Constraint]
   (s/optional-key :checkpoint) Checkpoint
   (s/optional-key :container) Container
   (s/optional-key :executor) (s/enum "cook" "mesos")
   (s/optional-key :progress-output-file) NonEmptyString
   (s/optional-key :progress-regex-string) NonEmptyString
   (s/optional-key :group) s/Uuid
   (s/optional-key :disable-mea-culpa-retries) s/Bool
   :cpus PosDouble
   :mem PosDouble
   (s/optional-key :disk) Disk
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
              :retries-remaining s/Int
              :status s/Str
              :state s/Str
              :submit-time (s/maybe PosInt)
              :user UserName
              (s/optional-key :disk) Disk
              (s/optional-key :gpus) s/Int
              (s/optional-key :groups) [s/Uuid]
              (s/optional-key :instances) [Instance]
              (s/optional-key :pool) s/Str
              (s/optional-key :submit-pool) s/Str})
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

(def JobInstanceProgressRequest
  "Schema for a POST request to the /progress/:uuid endpoint."
  (s/both
    (s/pred (some-fn :progress-message :progress-percent) 'message-or-percent-required)
    {:progress-sequence s/Int
     (s/optional-key :progress-message) s/Str
     (s/optional-key :progress-percent) s/Int}))

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

(defn- ensure-user-parameter
  "Ensures the presence of the user parameter by attaching it when missing in params."
  [user id params]
  (if-let [user-id (util/user->user-id user)]
    (if-let [group-id (util/user->group-id user)]
      (let [expected-user-param (str user-id ":" group-id)]
        (if-let [user-params (seq (filter #(= (:key %) "user") params))]
          (do
            ;; Validate the user parameter if it was provided in the job.
            ;; Prevents a user from requesting a user-id:group-id pair other than their own.
            (when (some #(not= expected-user-param (:value %)) user-params)
              (throw (ex-info "user parameter must match uid and gid of user submitting"
                              {:expected-user-param expected-user-param
                               :user-params-submitted user-params})))
            params)
          (do
            (log/info "attaching user" expected-user-param "to parameters for job" id)
            (conj params {:key "user" :value expected-user-param}))))
      (throw (ex-info "The gid is unavailable for the user" {:job-id id :user user})))
    (throw (ex-info "The uid is unavailable for the user" {:job-id id :user user}))))

(defn- build-docker-container
  [user id container {:keys [uuid name]} pool-name default-container]
  (let [container-id (d/tempid :db.part/user)
        docker-id (d/tempid :db.part/user)
        volumes (or (:volumes container) [])
        docker (:docker container)
        default-docker (:docker default-container)
        params (->> (or (:parameters docker) [])
                    (ensure-user-parameter user id))
        port-mappings (or (:port-mapping docker) [])
        user-image (:image docker)
        ; A user can specify their own container but use the image of the default container. This allows the
        ; user to set other container properties such as ports but not have to provide the actual image themselves.
        ; To do this, a user can omit the image field. The default container image is then used.
        image-config (if-not user-image
                       (:image default-docker)
                       user-image)
        image (if (string? image-config)
                image-config
                (let [[resolved-config reason]
                      (config-incremental/resolve-incremental-config
                        uuid
                        image-config
                        (or (:image-fallback docker) (:image-fallback default-docker)))]
                  (passport/log-event {:event-type passport/default-image-selected
                                       :image-config image-config
                                       :job-name name
                                       :job-uuid (str uuid)
                                       :pool pool-name
                                       :reason reason
                                       :resolved-config resolved-config
                                       :user user})
                  resolved-config))]
    [[:db/add id :job/container container-id]
     (merge {:db/id container-id
             :container/type "DOCKER"}
            (mkvolumes container-id volumes))
     [:db/add container-id :container/docker docker-id]
     (merge {:db/id docker-id
             :docker/image image
             :docker/force-pull-image (:force-pull-image docker false)}
            (when (:network docker) {:docker/network (:network docker)})
            (mk-container-params docker-id params)
            (mk-docker-ports docker-id port-mappings))]))

(defn- build-mesos-container
  [user id container]
  (let [container-id (d/tempid :db.part/user)
        mesos-id (d/tempid :db.part/user)
        mesos (:mesos container)
        volumes (or (:volumes container) [])]
    [[:db/add id :job/container container-id]
     (merge {:db/id container-id
             :container/type "MESOS"}
            (mkvolumes container-id volumes))
     [:db/add container-id :container/mesos mesos-id]
     {:db/id mesos-id
      :mesos/image (:image mesos)}]))

(defn- build-container
  "Helper for submit-jobs, deal with container structure."
  [user id {:keys [type] :as container} job pool-name default-container]
  (case (str/lower-case type)
    "docker" (build-docker-container user id container job pool-name default-container)
    "mesos" (build-mesos-container user id container)
    {}))

(defn- build-checkpoint
  "Helper for submit-jobs, deal with checkpoint config."
  [{:keys [mode options periodic-options]}]
  (cond-> {:checkpoint/mode mode}
    options
    (assoc :checkpoint/options
           (let [{:keys [preserve-paths]} options]
             {:checkpoint-options/preserve-paths preserve-paths}))
    periodic-options
    (assoc :checkpoint/periodic-options
           (let [{:keys [period-sec]} periodic-options]
             {:checkpoint-periodic-options/period-sec period-sec}))))

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

(defn get-default-container-for-pool
  "Given a pool name, determine a default container that should be run on it."
  [default-containers effective-pool-name]
  (regexp-tools/match-based-on-pool-name default-containers effective-pool-name :container))

(defn get-gpu-models-on-pool
   "Given a pool name, determine the supported GPU models on that pool."
   [valid-gpu-models effective-pool-name]
   (regexp-tools/match-based-on-pool-name valid-gpu-models effective-pool-name :valid-models))

(defn get-disk-types-on-pool
  "Given a pool name, determine the supported disk types on that pool."
  [disk effective-pool-name]
  (regexp-tools/match-based-on-pool-name disk effective-pool-name :valid-types))

(defn get-max-disk-size-on-pool
  "Given a pool name, determine the max requestable disk size on that pool."
  [disk effective-pool-name]
  (regexp-tools/match-based-on-pool-name disk effective-pool-name :max-size))

(defn lookup-cache-pool-name!
  "Looks up the given pool name in the given pool-name->result cache"
  [cache db pool->value-fn pool-name]
  (let [pool-name->pool
        (fn [pool-name]
          (when pool-name (d/entity db [:pool/name pool-name])))
        miss-fn
        (fn [pool-name]
          (-> pool-name pool-name->pool pool->value-fn))]
    (ccache/lookup-cache! cache identity miss-fn pool-name)))

(s/defn make-job-txn
  "Creates the necessary txn data to insert a job into the database"
  [{:keys [job pool-name pool-name-from-submission]} :- {:job Job} commit-latch-id db]
  (let [{:keys [uuid command max-retries max-runtime expected-runtime priority cpus mem disk gpus
                user name ports uris env labels container group application disable-mea-culpa-retries
                constraints executor progress-output-file progress-regex-string checkpoint]
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
        default-containers (get-in config/config [:settings :pools :default-containers])
        default-container (get-default-container-for-pool default-containers pool-name)
        container (if (nil? container)
                    (if pool-name
                      (if default-container
                        (build-container user db-id default-container job pool-name default-container)
                        [])
                      [])
                    (build-container user db-id container job pool-name default-container))
        executor (str->executor-enum executor)
        ;; These are optionally set datoms w/ default values
        maybe-datoms (reduce into
                             []
                             [(when (and priority (not= util/default-job-priority priority))
                                [[:db/add db-id :job/priority priority]])
                              (when (and max-runtime (not= Long/MAX_VALUE max-runtime))
                                [[:db/add db-id :job/max-runtime max-runtime]])
                              (when disk
                                (let [disk-id (d/tempid :db.part/user)
                                      params {:db/id disk-id
                                              :resource/type :resource.type/disk
                                              :resource.disk/request (some-> disk :request double)}]
                                  [[:db/add db-id :job/resource disk-id]
                                   (reduce-kv
                                     ;; This only adds the optional params to the DB if they were explicitly set
                                     (fn [txn-map k v]
                                       (if-not (nil? v)
                                         (assoc txn-map k v)
                                         txn-map))
                                     params
                                     {:resource.disk/limit (some-> disk :limit double)
                                      :resource.disk/type (:type disk)})]))
                              (when (and gpus (not (zero? gpus)))
                                (let [gpus-id (d/tempid :db.part/user)]
                                  [[:db/add db-id :job/resource gpus-id]
                                   {:db/id gpus-id
                                    :resource/type :resource.type/gpus
                                    :resource/amount (double gpus)}]))])
        job-submit-time (Date.)
        txn (cond-> {:db/id db-id
                     :job/command command
                     :job/command-length (count command)
                     :job/commit-latch commit-latch-id
                     :job/custom-executor false
                     :job/disable-mea-culpa-retries disable-mea-culpa-retries
                     :job/last-waiting-start-time job-submit-time
                     :job/max-retries max-retries
                     :job/name (or name "cookjob") ; set the default job name if not provided.
                     :job/resource [{:resource/type :resource.type/cpus
                                     :resource/amount cpus}
                                    {:resource/type :resource.type/mem
                                     :resource/amount mem}]
                     :job/state :job.state/waiting
                     :job/submit-time job-submit-time
                     :job/user user
                     :job/uuid uuid}
                    application (assoc :job/application
                                       (cond->
                                         {:application/name (:name application)
                                          :application/version (:version application)}
                                         (:workload-class application)
                                         (assoc
                                           :application/workload-class
                                           (:workload-class application))
                                         (:workload-id application)
                                         (assoc
                                           :application/workload-id
                                           (:workload-id application))
                                         (:workload-details application)
                                         (assoc
                                           :application/workload-details
                                           (:workload-details application))))
                    expected-runtime (assoc :job/expected-runtime expected-runtime)
                    executor (assoc :job/executor executor)
                    progress-output-file (assoc :job/progress-output-file progress-output-file)
                    progress-regex-string (assoc :job/progress-regex-string progress-regex-string)
                    ; This is a wart. We want to set a pool unconditionally, but would
                    ; need to fix unnumerable unit tests before then; you can't store
                    ; a nil pool in datomic, and most of our unit tests pre-date
                    ; pools and don't specify a config with a default pool
                    pool-name
                    (assoc :job/pool
                           (lookup-cache-pool-name!
                             caches/pool-name->db-id-cache
                             db
                             :db/id
                             pool-name))
                    pool-name-from-submission
                    (assoc :job/submit-pool-name
                           pool-name-from-submission)
                    checkpoint (assoc :job/checkpoint (build-checkpoint checkpoint)))
        txn (plugins/adjust-job adjustment/plugin txn db)]

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

(defn validate-gpu-job
  "Validates that a job requesting GPUs is supported on the pool"
  [gpu-enabled? pool-name {:keys [gpus env]}]
  (let [requested-gpu-model (get env "COOK_GPU_MODEL")
        gpus' (or gpus 0)]
    (when (and (pos? gpus') (not gpu-enabled?))
      (throw (ex-info (str "GPU support is not enabled") {})))
    (when (and (pos? gpus') (not (get-gpu-models-on-pool (config/valid-gpu-models) pool-name)))
      (throw (ex-info (str "Job requested GPUs but pool " pool-name " does not have any valid GPU models") {})))
    (when (and requested-gpu-model
               (not (contains? (get-gpu-models-on-pool (config/valid-gpu-models) pool-name) requested-gpu-model)))
      (throw (ex-info (str "The following GPU model is not supported: " requested-gpu-model) {})))))

(defn validate-job-disk
  "Validates that a job requesting disk is satisfying the following conditions:
    - Disk specifications cannot be made on a pool that does not support disks
    - Disk limit and disk type are optional
    - Disk request cannot exceed disk limit, and both request and limit cannot exceed the max size in config
    - Requested type must be a valid type in config"
  [pool-name {:keys [disk]}]
  (let [{disk-request :request disk-limit :limit requested-disk-type :type} disk
        max-size (get-max-disk-size-on-pool (config/disk) pool-name)
        disk-types-on-pool (get-disk-types-on-pool (config/disk) pool-name)]
    (when-not disk-types-on-pool
      (throw (ex-info (str "Disk specifications are not supported on pool " pool-name) disk)))
    (when disk-limit
      (when-not (<= disk-request disk-limit max-size)
        (throw (ex-info (str "Disk resource setting error. We must have disk-request <= disk-limit <= max-size.")
                        {:disk-request disk-request :disk-limit disk-limit :max-size max-size}))))
    (when (> disk-request max-size)
      (throw (ex-info (str "Disk request specified is greater than max disk size on pool") disk)))
    (when (and requested-disk-type
               (not (contains? disk-types-on-pool requested-disk-type)))
      (throw (ex-info (str "The following disk type is not supported: " requested-disk-type " for pool " pool-name " with disks " disk-types-on-pool) disk)))))

(defn validate-and-munge-job
  "Takes the user, the parsed json from the job and a list of the uuids of
   new-groups (submitted in the same request as the job). Returns proper Job
   objects, or else throws an exception"
  [db pool-name user task-constraints gpu-enabled? new-group-uuids
   {:keys [cpus mem disk gpus uuid command priority max-retries max-runtime expected-runtime name
           uris ports env labels container group application disable-mea-culpa-retries
           constraints executor progress-output-file progress-regex-string checkpoint]
    :or {group nil
         disable-mea-culpa-retries false}
    :as job}
   & {:keys [commit-latch-id override-group-immutability?]
      :or {commit-latch-id nil
           override-group-immutability? false}}]
  (let [{:keys [docker-parameters-allowed max-ports]} (config/task-constraints)
        group-uuid (when group group)
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
                 (when disk {:disk disk})
                 (when gpus {:gpus (int gpus)})
                 (when env {:env (walk/stringify-keys env)})
                 (when uris {:uris (map (fn [{:keys [value executable cache extract]}]
                                          (merge {:value value}
                                                 (when executable {:executable? executable})
                                                 (when cache {:cache? cache})
                                                 (when extract {:extract? extract})))
                                        uris)})
                 ; We need to convert job label keys to strings, but
                 ; walk/stringify-keys removes the namespace, which would mean
                 ; that jobs submitted with a label key like "platform/thing"
                 ; would lose the "platform/" part. Instead we just call str and
                 ; remove the leading ':'.
                 (when labels {:labels (pc/map-keys #(-> % str (subs 1)) labels)})
                 ;; Rest framework keywordifies all keys, we want these to be strings!
                 (when constraints {:constraints constraints})
                 (when group-uuid {:group group-uuid})
                 (when container {:container container})
                 (when expected-runtime {:expected-runtime expected-runtime})
                 (when executor {:executor executor})
                 (when progress-output-file {:progress-output-file progress-output-file})
                 (when progress-regex-string {:progress-regex-string progress-regex-string})
                 (when application {:application application})
                 (when checkpoint {:checkpoint (if (-> checkpoint :options :preserve-paths)
                                                 (update-in checkpoint [:options :preserve-paths] set)
                                                 checkpoint)}))
        params (get-in munged [:container :docker :parameters])]
    (s/validate Job munged)
    ; Note that we are passing munged here because the function expects stringified env
    (validate-gpu-job gpu-enabled? pool-name munged)
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
    (when disk (validate-job-disk pool-name munged))
    (when (> (:ports munged) max-ports)
      (throw (ex-info (str "Requested " ports " ports, but only allowed to use " max-ports)
                      {:constraints task-constraints
                       :job job})))
    (when (and (:retry-limit task-constraints)
               (> max-retries (:retry-limit task-constraints)))
      (throw (ex-info (str "Requested " max-retries " exceeds the maximum retry limit")
                      {:constraints task-constraints
                       :job job})))
    (when (and (:command-length-limit task-constraints)
               (> (.length ^String command) (:command-length-limit task-constraints)))
      (throw (ex-info (str "Job command length of " (.length command) " is greater than the maximum command length ("
                           (:command-length-limit task-constraints) ")")
                      {:command-length-limit (:command-length-limit task-constraints)
                       :job job})))

    (when (and params docker-parameters-allowed)
      (let [disallowed-params (into [] (filter (fn [{:keys [key]}]
                                                 (not (contains? docker-parameters-allowed key)))
                                               params))]
        (when (seq disallowed-params)
          (throw (ex-info (str "The following parameters are not supported: " disallowed-params)
                          {:params params})))))
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

(defn retrieve-sandbox-url-path
  "Gets a URL to query the sandbox directory of the task.
   Users will need to add the file path & offset to their query.
   Refer to the 'Using the output_url' section in docs/scheduler-rest-api.adoc for further details.
   Delegates to the compute cluster implementation."
  [instance-entity]
  (if-let [sandbox-url (:instance/sandbox-url instance-entity)]
    sandbox-url
    (when-let [compute-cluster (task/get-compute-cluster-for-task-ent-if-present instance-entity)]
      (cc/retrieve-sandbox-url-path compute-cluster instance-entity))))

(defn compute-cluster-entity->map
  "Attached to the the instance object when we send it in API responses"
  [entity]
  (cond-> {:name (:compute-cluster/cluster-name entity)}
    (= :compute-cluster.type/mesos (:compute-cluster/type entity))
    (-> (assoc :mesos {:framework-id (:compute-cluster/mesos-framework-id entity)})
        (assoc :type :mesos))
    (= :compute-cluster.type/kubernetes (:compute-cluster/type entity))
    (assoc :type :kubernetes)))

(defn fetch-compute-cluster-map
  "Converts a compute cluster entity as a map representing the fields. For legacy instances
  that predate cook-cluster, inline the default cluster, pulled from config."
  [db entity]
  (if entity
    (compute-cluster-entity->map entity)
    (->> (cc/get-default-cluster-for-legacy)  ; Get the default cluster.
         cc/db-id
         (d/entity db)
         (fetch-compute-cluster-map db))))

(defn fetch-instance-map
  "Converts the instance entity to a map representing the instance fields."
  [db instance]
  (prom/with-duration
    prom/fetch-instance-map-duration {}
    (timers/time!
      (timers/timer ["cook-mesos" "internal" "fetch-instance-map"])
      (let [hostname (:instance/hostname instance)
            slave-id (:instance/slave-id instance)
            task-id (:instance/task-id instance)
            executor (:instance/executor instance)
            sandbox-directory (:instance/sandbox-directory instance)
            url-path (retrieve-sandbox-url-path instance)
            start (:instance/start-time instance)
            mesos-start (:instance/mesos-start-time instance)
            end (:instance/end-time instance)
            cancelled (:instance/cancelled instance)
            reason (reason/instance-entity->reason-entity db instance)
            exit-code (:instance/exit-code instance)
            progress (:instance/progress instance)
            progress-message (:instance/progress-message instance)
            file-url (plugins/file-url file-plugin/plugin instance)
            queue-time (:instance/queue-time instance)]
        (cond-> {:backfilled false ;; Backfill has been deprecated
                 :compute-cluster (fetch-compute-cluster-map db (:instance/compute-cluster instance))
                 :executor_id (:instance/executor-id instance)
                 ; NOTE: the CLI requires hostname, slave-id, and agent-id to be non-nil. An instance's
                 ; pod which is scheduled by Kubernetes instead of Fenzo, does not initially have a
                 ; hostname, for example. We avoid setting fake data in this case on the instance in
                 ; Datomic and choose to return it at the API layer.
                 ;
                 ; TODO: stop this workaround once the CLI supports these as optional.
                 :hostname (or hostname "Unknown")
                 :slave_id (or slave-id "Unknown")
                 :agent_id (or slave-id "Unknown")

                 :ports (vec (sort (:instance/ports instance)))
                 :preempted (:instance/preempted? instance false)
                 :status (name (:instance/status instance))
                 :task_id task-id}
          executor (assoc :executor (name executor))
          file-url (assoc :file_url file-url)
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
          sandbox-directory (assoc :sandbox_directory sandbox-directory)
          queue-time (assoc :queue_time queue-time))))))

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

(defn- mesos->response-map
  [{:keys [mesos/image]}]
  {:image image})

(defn- container-volume->response-map
  [{:keys [container.volume/mode container.volume/host-path container.volume/container-path]}]
  (cond-> {:host-path host-path}
          mode (assoc :mode mode)
          container-path (assoc :container-path container-path)))

(defn- container->response-map
  [{:keys [container/type container/docker container/mesos container/volumes]}]
  (cond-> {:type type}
          docker (assoc :docker (docker->response-map docker))
          mesos (assoc :mesos (mesos->response-map mesos))
          volumes (assoc :volumes (map container-volume->response-map volumes))))

(defn guess-framework-id
  "If there is only one mesos compute cluster, returns it's framework id. Otherwise, returns \"unsupported\""
  []
  (let [compute-clusters (config/compute-clusters)]
    (if (and (= 1 (count compute-clusters))
             (-> compute-clusters first :config :framework-id))
      (-> compute-clusters first :config :framework-id)
      "unsupported")))

(defn fetch-job-map-from-entity
  [db job]
  (prom/with-duration
    prom/fetch-job-map-duration {}
    (timers/time!
      (timers/timer ["cook-mesos" "internal" "fetch-job-map"])
      (let [resources (util/job-ent->resources job)
            groups (:group/_job job)
            application (:job/application job)
            disk (:disk resources)
            expected-runtime (:job/expected-runtime job)
            executor (:job/executor job)
            progress-output-file (:job/progress-output-file job)
            progress-regex-string (:job/progress-regex-string job)
            pool (:job/pool job)
            container (:job/container job)
            checkpoint (:job/checkpoint job)
            state (util/job-ent->state job)
            constraints (->> job
                             :job/constraint
                             (map util/remove-datomic-namespacing)
                             (map (fn [{:keys [attribute operator pattern]}]
                                    (->> [attribute (str/upper-case (name operator)) pattern]
                                         (map str)))))
            instances (map #(fetch-instance-map db %1) (:job/instance job))
            submit-time (util/job->submit-time job)
            attempts-consumed (util/job-ent->attempts-consumed db job)
            retries-remaining (- (:job/max-retries job) attempts-consumed)
            disable-mea-culpa-retries (:job/disable-mea-culpa-retries job false)
            submit-pool-name (:job/submit-pool-name job)
            job-map {:command (:job/command job)
                     :constraints constraints
                     :cpus (:cpus resources)
                     :disable_mea_culpa_retries disable-mea-culpa-retries
                     :env (util/job-ent->env job)
                     ; TODO(pschorf): Remove field
                     :framework_id (guess-framework-id)
                     :gpus (int (:gpus resources 0))
                     :instances instances
                     :labels (util/job-ent->label job)
                     :max_retries (:job/max-retries job) ; consistent with input
                     :max_runtime (:job/max-runtime job Long/MAX_VALUE) ; consistent with input
                     :mem (:mem resources)
                     :name (:job/name job "cookjob")
                     :ports (:job/ports job 0)
                     :priority (:job/priority job util/default-job-priority)
                     :retries_remaining retries-remaining
                     :state state
                     :status (name (:job/state job))
                     :submit_time submit-time
                     :uris (:uris resources)
                     :user (:job/user job)
                     :uuid (:job/uuid job)}]
        (when (neg? retries-remaining)
          ; TODO:
          ; There's a bug in the retries remaining logic that
          ; causes this number to sometimes be negative
          (log/warn "Job" (:job/uuid job) "has negative retries remaining"
                    {:attempts-consumed attempts-consumed
                     :disable-mea-culpa-retries disable-mea-culpa-retries
                     :max-retries (:job/max-retries job)
                     :retries-remaining retries-remaining}))
        (cond-> job-map
          groups (assoc :groups (map #(str (:group/uuid %)) groups))
          application (assoc :application (util/remove-datomic-namespacing application))
          disk (assoc :disk disk)
          expected-runtime (assoc :expected-runtime expected-runtime)
          executor (assoc :executor (name executor))
          progress-output-file (assoc :progress-output-file progress-output-file)
          progress-regex-string (assoc :progress-regex-string progress-regex-string)
          pool (assoc :pool (:pool/name pool))
          container (assoc :container (container->response-map container))
          checkpoint (assoc :checkpoint (util/job-ent->checkpoint job))
          submit-pool-name (assoc :submit-pool submit-pool-name))))))

(defn fetch-job-map
  [db job-uuid]
  (fetch-job-map-from-entity db (d/entity db [:job/uuid job-uuid])))

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
        instance-uuid->job-uuid #(cached-queries/instance-uuid->job-uuid-datomic-query (d/db conn) %)
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
        instance-uuid->job-uuid #(cached-queries/instance-uuid->job-uuid-datomic-query (d/db conn) %)
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
        exists? #(job-exists? (db conn) %)
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
        job-user (:job/user (d/entity (db conn) [:job/uuid job-uuid]))
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
  [conn ctx]
  (mapv (partial fetch-job-map (db conn)) (::jobs ctx)))

(defn render-jobs-for-response
  [conn ctx]
  (let [db (db conn)

        fetch-group
        (fn fetch-group [group-uuid]
          (let [group (d/entity db [:group/uuid (UUID/fromString group-uuid)])]
            {:uuid group-uuid
             :name (:group/name group)}))

        fetch-job
        (fn fetch-job [job-uuid]
          (let [job (fetch-job-map db job-uuid)
                groups (mapv fetch-group (:groups job))]
            (assoc job :groups groups)))]

    (mapv fetch-job (::jobs ctx))))

(defn render-instances-for-response
  [conn ctx]
  (let [db (db conn)
        fetch-job (partial fetch-job-map db)
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

(def leader-hostname-regex #"^([^#]*)#([0-9]*)#([a-z]*)#.*")

(defn leader-selector->leader-id
  "Get the current leader node's id from a leader-selector object.
   Throws if there is currently no leader available."
  [^LeaderSelector leader-selector]
  (let [leader (.getLeader leader-selector)]
    ;; NOTE: .getLeader returns a dummy object when no leader is available,
    ;; but the dummy object always returns false for the .isLeader predicate.
    (when-not (.isLeader leader)
      (throw (IllegalStateException. "Leader is temporarily unavailable.")))
    (.getId leader)))

(defn leader-selector->leader-url
  "Get the URL for the current Cook leader node.
   This is useful for building redirects."
  [leader-selector]
  (let [leader-id (leader-selector->leader-id leader-selector)
        leader-match (re-matcher leader-hostname-regex leader-id)]
    (if (.matches leader-match)
      (let [leader-hostname (.group leader-match 1)
            leader-port (.group leader-match 2)
            leader-protocol (.group leader-match 3)]
        (str leader-protocol "://" leader-hostname ":" leader-port))
      (throw (IllegalStateException.
               (str "Unable to parse leader id: " leader-id))))))

(defn cook-info-handler
  "Handler for the /info endpoint"
  [settings leader-selector]
  (let [auth-middleware (:authorization-middleware settings)
        auth-scheme (str (or (-> auth-middleware meta :json-value) auth-middleware))]
    (base-cook-handler
      {:allowed-methods [:get]
       :handle-ok (fn get-info-handler [_]
                    {:authentication-scheme auth-scheme
                     :commit @cook.util/commit
                     :start-time start-up-time
                     :version @cook.util/version
                     :leader-url (leader-selector->leader-url leader-selector)})})))

;;; On GET; use repeated job argument
(defn read-jobs-handler-deprecated
  [conn is-authorized-fn]
  (base-cook-handler
    {:allowed-methods [:get]
     :malformed? check-job-params-present
     :allowed? (partial job-request-allowed? conn is-authorized-fn)
     :exists? (partial retrieve-jobs conn)
     :handle-ok (fn [ctx] (render-jobs-for-response-deprecated conn ctx))}))

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

(defn list-job-ents
  "Queries using the params from ctx and returns the jobs that were found as datomic entities."
  [db include-custom-executor? ctx]
  (prom/with-duration
    prom/list-jobs-duration {}
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
            time-range-ms (- end-ms start-ms')
            start (Date. ^long start-ms')
            end (Date. ^long end-ms)
            job-ents (->> (prom/with-duration
                            prom/fetch-jobs-duration {}
                            (timers/time!
                              fetch-jobs
                              (util/get-jobs-by-user-and-states db user states start end limit
                                                                name-filter-fn include-custom-executor? pool-name)))
                          (sort-by :job/submit-time)
                          reverse)
            job-ents (if (nil? limit)
                       job-ents
                       (take limit job-ents))
            job-ents-count (count job-ents)]
        (histograms/update! list-request-param-time-range-ms time-range-ms)
        (histograms/update! list-request-param-limit limit)
        (histograms/update! list-response-job-count job-ents-count)
        (prom/observe prom/list-request-param-time-range time-range-ms)
        (prom/observe prom/list-request-param-limit limit)
        (prom/observe prom/list-response-job-count job-ents-count)
        job-ents))))

(defn list-jobs
  "Queries using the params from ctx and returns the job uuids that were found"
  [db include-custom-executor? ctx]
  (map :job/uuid (list-job-ents db include-custom-executor? ctx)))

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
  [conn is-authorized-fn]
  (let [handle-ok (partial render-jobs-for-response conn)]
    (read-jobs-handler conn is-authorized-fn {:handle-ok handle-ok})))

(defn read-jobs-handler-single
  [conn is-authorized-fn]
  (let [handle-ok
        (fn handle-ok [ctx]
          (first
            (render-jobs-for-response conn ctx)))]
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
  [conn is-authorized-fn]
  (let [handle-ok (partial render-instances-for-response conn)]
    (base-read-instances-handler conn is-authorized-fn {:handle-ok handle-ok})))

(defn read-instances-handler-single
  [conn is-authorized-fn]
  (let [handle-ok (->> (partial render-instances-for-response conn)
                       (comp first))]
    (base-read-instances-handler conn is-authorized-fn {:handle-ok handle-ok})))

(defn redirect-to-leader
  "Returns a map of Liberator resource attribute functions to
   be used when only the Cook leader should handle the request.

   Note: If you use this function, you need special logic in the client to follow the
   resulting redirects. Specifically, python needs to use cook.util.request_with_redirects
   to work correctly, and curl needs command line option --location / --location-trusted."
  [leadership-atom leader-selector]
  {:can-post-to-gone?
   (constantly true)

   ; Needs to be false when we're the leader or else the response code for when :exists?
   ; is false is 410 GONE instead of 404 not-found.
   ; Needs to be true when we're not the leader to trigger the redirection path.
   :existed?
   (fn [_] (not @leadership-atom))

   ;; Being false when not the leader triggers path for moved-temporarily?
   :exists? (fn [_] @leadership-atom)

   :handle-moved-temporarily
   (fn redirect-to-leader-handle-moved-temporarily
     [ctx]
     {:location (:location ctx)
      :message "redirecting to leader"})

   :handle-service-not-available
   (fn redirect-to-leader-handle-service-not-available
     [{:keys [::message]}]
     {:message message})

   :moved-temporarily?
   (fn redirect-to-leader-moved-temporarily?
     [ctx]
     ;; the client is expected to cache the redirect location
     (if-let [leader-url (::leader-url ctx)]
       (let [request-path (get-in ctx [:request :uri])
             query-string (get-in ctx [:request :query-string])]
         ; Include the request parameters in the location header response, if needed.
         [true {:location (str leader-url request-path (when query-string (str "?" query-string)))}])
       [false {}]))

   :service-available?
   (fn redirect-to-leader-service-available?
     [_]
     (if @leadership-atom
       [true {}]
       (try
         ;; recording target leader-url for redirect
         [true {::leader-url (leader-selector->leader-url leader-selector)}]
         ;; handle leader-not-found errors by responding 503
         (catch IllegalStateException e
           [false {::message (.getMessage e)}]))))})

(defn update-instance-progress-handler
  [conn is-authorized-fn leadership-atom leader-selector progress-aggregator-chan]
  (base-cook-handler
    (merge
      ;; only the leader handles progress updates
      (redirect-to-leader leadership-atom leader-selector)
      {:allowed-methods [:post]
       :initialize-context (fn [ctx]
                             ;; injecting ::instances into ctx for later handlers
                             {::instances [(get-in ctx [:request :params :uuid])]})
       :allowed? (partial instance-request-allowed? conn is-authorized-fn)
       :exists? (constantly false)  ;; triggers path for moved-temporarily?
       :existed? instance-request-exists?
       :can-post-to-missing? (constantly false)
       :post! (fn [ctx]
                (let [progress-message-map (get-in ctx [:request :body-params])
                      task-id (-> ctx ::instances first)]
                  (progress/handle-progress-message!
                    (d/db conn) task-id progress-aggregator-chan progress-message-map)))
       :post-enacted? (constantly false)  ;; triggers http 202 "accepted" response
       :handle-accepted (fn [ctx]
                          (let [instance (-> ctx ::instances first)
                                job (-> ctx ::jobs first)]
                            {:instance instance :job job :message "progress update accepted"}))})))

(defn metrics-handler
  []
  (base-cook-handler
    {:allowed-methods [:get]
     :available-media-types ["text/plain"]
     :handle-ok (fn [_]
                  (prom/export))}))

;;; On DELETE; use repeated job argument
(defn destroy-jobs-handler
  [conn is-authorized-fn]
  (base-cook-handler
    {:allowed-methods [:delete]
     :malformed? check-job-params-present
     :allowed? (partial job-kill-allowed? conn is-authorized-fn)
     :exists? (partial retrieve-jobs conn)
     :delete! (fn [ctx]
                (let [user (get-in ctx [:request :authorization/user])]
                  (log/info "Killing jobs for user" user {:jobs (::jobs-requested ctx)
                                                          :instances (::instances-requested ctx)}))
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
  [conn {:keys [::groups ::job-pool-name-maps] :as ctx}]
  (try
    (log/info "Submitting jobs through raw api:"
              (map (fn [job-pool-name-map]
                     (update job-pool-name-map :job #(dissoc % :command)))
                   job-pool-name-maps))
    (doseq [{{:keys [uuid, user, name]} :job pool :pool-name} job-pool-name-maps]
      (passport/log-event {:event-type passport/job-created
                           :job-name name
                           :job-uuid (str uuid)
                           :pool pool
                           :user user}))
    (let [jobs (map :job job-pool-name-maps)
          group-uuids (set (map :uuid groups))
          group-asserts (map (fn [guuid] [:entity/ensure-not-exists [:group/uuid guuid]])
                             group-uuids)
          db (d/db conn)
          ;; Create new implicit groups (with all default settings)
          implicit-groups (->> jobs
                               (map :group)
                               (remove nil?)
                               distinct
                               (remove #(contains? group-uuids %))
                               (map (fn [guuid] (if (group-exists? db guuid)
                                                  (dissoc (fetch-group-map db guuid) :jobs)
                                                  (make-default-group guuid)))))
          groups (into (vec implicit-groups) groups)
          job-asserts (map (fn [j] [:entity/ensure-not-exists [:job/uuid (:uuid j)]]) jobs)
          [commit-latch-id commit-latch] (make-commit-latch)
          job-txns (mapcat
                     (fn [job-pool-name-map]
                       (make-job-txn job-pool-name-map commit-latch-id db))
                     job-pool-name-maps)
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
        (rate-limit/spend! rate-limit/job-submission-rate-limiter user (count jobs))
        @(d/transact
           conn
           (-> (vec group-asserts)
               (into job-asserts)
               (conj commit-latch)
               (into job-txns)
               (into group-txns)))
        (doseq [[pool-name num-jobs-in-pool] (->> job-pool-name-maps (map :pool-name) frequencies)]
          (let [pool-name (pool/pool-name-or-default pool-name)]
            (queue-limit/inc-queue-length! pool-name user num-jobs-in-pool)
            (prom/inc prom/jobs-created {:pool pool-name} num-jobs-in-pool)
            (meters/mark! (meters/meter ["cook-mesos" "scheduler" "jobs-created"
                                         (str "pool-" pool-name)])
                          num-jobs-in-pool))))
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
  [conn {:keys [::job-pool-name-maps] :as ctx}]
  (let [db (db conn)
        ; We cache quota by (user, pool) here because JobRouting plugins can result in
        ; different jobs in a single batch submission getting routed to different pools,
        ; and we don't want to query the database for quota for every single job.
        get-quota
        (fn [db user pool-name]
          (let [miss-fn
                (fn [{:keys [pool-name user]}]
                  (let [quota (quota/get-quota db user pool-name)]
                    (log/debug "In" pool-name "pool, queried user quota" user ":" quota)
                    {:cache-expires-at (-> 30 t/seconds t/from-now)
                     :quota quota}))]
            (:quota
              (ccache/lookup-cache-with-expiration!
                caches/user-and-pool-name->quota
                identity
                miss-fn
                {:pool-name pool-name :user user}))))
        resource-keys [:cpus :mem :gpus]
        user (get-in ctx [:request :authorization/user])
        errors (for [{:keys [job pool-name]} job-pool-name-maps
                     resource resource-keys
                     :let [job-usage (-> job (get resource 0) double)
                           user-quota (get-quota db user pool-name)
                           quota-val (-> user-quota (get resource) double)
                           zero-jobs? (-> user-quota :count zero?)]
                     :when (or (> job-usage quota-val) zero-jobs?)]
                 (if zero-jobs?
                   "User quota is set to zero jobs."
                   (format "Job %s exceeds quota for %s: %f > %f"
                           (:uuid job) (name resource) job-usage quota-val)))]
    (if (seq errors)
      [false {::error (str/join "\n" errors)}]
      true)))

(defn user-queue-length-within-limit?
  "Check if the job submission would cause the user to have more jobs queued than they're
  allowed. We refresh queue lengths from the database on a configurable interval. Note that
  this check does not take into account any of the following in real-time, and therefore is
  only 100% accurate as of the last refresh from the database:

  - Jobs will get removed from the queue when they launch. This is going to be minor as
    launch rate is going to be much smaller than insert rate.

  - Jobs will get added to the queue if they fail and have retries. This is going to be
    minor as failure rate is going to be much smaller than insert rate.

  - If there are multiple API hosts in the cluster, each one is maintaining a separate
    count of the queue, without knowing about the other API hosts' job submissions or
    job kills. We hope that locality (a user tends to talk to the same host) mitigates
    this.

  In summary, we take into account submitted and killed jobs on the same API host but
  ignore launched and retried jobs. This check is meant to prevent individual users from
  flooding the queue with hundreds of thousands of jobs, so if it lets a few hundred too
  many or too few in until the next refresh from the database, that's acceptable."
  [{:keys [::jobs ::pool] :as ctx}]
  (let [pool-name (or (:pool/name pool) (config/default-pool))
        user (get-in ctx [:request :authorization/user])
        user-queue-length (queue-limit/user-queue-length pool-name user)
        user-queue-length-limit (queue-limit/user-queue-limit pool-name)]
    (if (> (-> jobs count (+ user-queue-length))
           user-queue-length-limit)
      (do
        (log/info "In" pool-name "pool, rejecting job submission due to queue length"
                  {:number-jobs (count jobs)
                   :user user
                   :user-queue-length user-queue-length
                   :user-queue-length-limit user-queue-length-limit})
        [false {::error (format "User has too many jobs queued (the per-user queue limit is %d)"
                                user-queue-length-limit)}])
      true)))

(defn job-create-processable?
  "We want to return a 422 (unprocessable entity) if the requested
   resources for a single job exceed the user's total resource quota,
   or if the job submission would cause the user to have more jobs
   queued than they're allowed."
  [conn ctx]
  (let [no-job-exceeds-quota-result
        (no-job-exceeds-quota? conn ctx)]
    (if (true? no-job-exceeds-quota-result)
      (user-queue-length-within-limit? ctx)
      no-job-exceeds-quota-result)))

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
                         pool-param (get params :pool)
                         pool-header (get headers "x-cook-pool")
                         pool-name (or pool-param pool-header)
                         uuid->count (pc/map-vals count (group-by :uuid jobs))
                         time-until-out-of-debt (rate-limit/time-until-out-of-debt-millis! rate-limit/job-submission-rate-limiter user)
                         in-debt? (not (zero? time-until-out-of-debt))]
                     (try
                       (doseq [{:keys [cpus gpus mem name uuid]} jobs]
                         (passport/log-event
                           {:cpus cpus
                            :event-type passport/job-submitted
                            :gpus (or gpus 0)
                            :job-name name
                            :job-uuid (str uuid)
                            :mem mem
                            :pool pool-name
                            :pool-header pool-header
                            :pool-param pool-param
                            :pool-source
                            (cond
                              pool-param :request-parameter
                              pool-header :x-cook-pool-header
                              :else :unspecified)
                            :user user}))
                       (when in-debt?
                         (log/info (str "User " user " is inserting too quickly (will be out of debt in "
                                        (/ time-until-out-of-debt 1000.0) " seconds).")))
                       (b/cond
                         (and in-debt? (rate-limit/enforce? rate-limit/job-submission-rate-limiter))
                         [true {::error (str "User " user " is inserting too quickly. Not allowed to insert for "
                                             (/ time-until-out-of-debt 1000.0) " seconds.")}]

                         (empty? params)
                         [true {::error (str "Must supply at least one job or group to start."
                                             "Are you specifying that this is application/json?")}]

                         ; We reject jobs unless one of the following is true:
                         ; - the submission did not explicitly specify a pool name
                         ; - the submission specified a job-routing pool name
                         ; - the submission specified a pool name that exists and
                         ;   that is accepting submissions

                         :let [db (db conn)
                               skip-pool-name-checks?
                               (or (not pool-name)
                                   (config/job-routing-pool-name? pool-name))
                               pool-exists?
                               (or skip-pool-name-checks?
                                   ; Values cached in pool-name->exists?-cache
                                   ; are always either true or false
                                   (lookup-cache-pool-name!
                                     caches/pool-name->exists?-cache
                                     db
                                     some?
                                     pool-name))]
                         (not pool-exists?)
                         [true {::error (str pool-name " is not a valid pool name.")}]

                         :let [pool-accepts-submissions?
                               (or skip-pool-name-checks?
                                   ; Values cached in pool-name->accepts-submissions?-cache
                                   ; are always either true or false
                                   (lookup-cache-pool-name!
                                     caches/pool-name->accepts-submissions?-cache
                                     db
                                     pool/accepts-submissions?
                                     pool-name))]
                         (not pool-accepts-submissions?)
                         [true {::error (str pool-name " is not accepting job submissions.")}]

                         (some true? (map (fn [[uuid count]] (< 1 count)) uuid->count))
                         [true {::error (str "Duplicate job uuids: " (->> uuid->count
                                                                          (filter (fn [[uuid count]] (< 1 count)))
                                                                          (map first)
                                                                          (map str)
                                                                          (into [])))}]
                         :else
                         (let [groups (mapv #(validate-and-munge-group db %) groups)
                               job-pool-name-maps
                               (mapv
                                 (fn [raw-job]
                                   (let [effective-job
                                         (job-submission-plugin/apply-job-submission-modifier-plugins raw-job pool-name)
                                         effective-pool-name (get effective-job :pool)
                                         validated-and-munged-job
                                         (validate-and-munge-job
                                           db
                                           effective-pool-name
                                           user
                                           task-constraints
                                           gpu-enabled?
                                           (set (map :uuid groups))
                                           effective-job
                                           :override-group-immutability?
                                           override-group-immutability?)]
                                     {:job validated-and-munged-job
                                      :pool-name effective-pool-name
                                      :pool-name-from-submission pool-name}))
                                 jobs)
                               {:keys [status message]}
                               (submission-plugin/plugin-jobs-submission job-pool-name-maps)]
                           ; Does the plugin accept the submission?
                           (if (= :accepted status)
                             [false {::groups groups
                                     ::job-pool-name-maps job-pool-name-maps}]
                             [true {::error message}])))
                       (catch Exception e
                         (log/warn e "Malformed raw api request")
                         [true {::error (.getMessage e)}]))))
     :allowed? (partial job-create-allowed? is-authorized-fn)
     :exists? (fn [ctx]
                (let [db (d/db conn)
                      existing (filter (partial job-exists? db) (->> ctx ::job-pool-name-maps (map :job) (map :uuid)))]
                  [(seq existing) {::existing existing}]))
     :processable? (partial job-create-processable? conn)
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
                         (log/warn exception "Exception occurred while creating jobs" (.getMessage exception))
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
          exists? #(group-exists? (db conn) %)
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
                       group-user (fn [guuid] (-> (d/entity (db conn) [:group/uuid guuid])
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
                    (mapv #(merge (fetch-group-map (db conn) %)
                                  (fetch-group-job-details (db conn) %))
                          (::guuids ctx))
                    (mapv #(fetch-group-map (db conn) %) (::guuids ctx))))
     :delete! (fn [ctx]
                (let [jobs (mapcat #(fetch-group-live-jobs (db conn) %)
                                   (::guuids ctx))
                      juuids (mapv :job/uuid jobs)
                      user (get-in ctx [:request :authorization/user])]
                  (log/info "Killing job group for user" user {:groups (::guuids ctx)
                                                               :jobs juuids})
                  (cook.mesos/kill-job conn juuids)))}))

;;
;; /queue

(timers/deftimer [cook-scheduler handler queue-endpoint])
(defn waiting-jobs
  [conn mesos-pending-jobs-fn is-authorized-fn leadership-atom leader-selector]
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
    :exists? (fn [_] [@leadership-atom {}])
    :existed? (fn [_] [true {}])
    :moved-temporarily? (fn [_]
                          (if @leadership-atom
                            [false {}]
                            [true {:location (str (leader-selector->leader-url leader-selector) "/queue")}]))
    :handle-forbidden (fn [ctx]
                        (log/info (get-in ctx [:request :authorization/user]) " is not authorized to access queue")
                        (render-error ctx))
    :handle-malformed render-error
    :handle-ok (fn [ctx]
                 (prom/with-duration
                   prom/endpoint-duration {:endpoint "/queue"}
                   (timers/time!
                     queue-endpoint
                     (let [db (d/db conn)
                           pool->queue (mesos-pending-jobs-fn)
                           pool->user->quota (quota/create-pool->user->quota-fn db)
                           pool->user->usage (util/pool->user->usage db)]
                       (pc/for-map [[pool-name queue] pool->queue]
                         pool-name (->> queue
                                        (util/filter-pending-jobs-for-quota
                                          pool-name (atom {}) (atom {}) (pool->user->quota pool-name)
                                          (pool->user->usage pool-name)
                                          (util/global-pool-quota pool-name))
                                        (take (::limit ctx))))))))))

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
                 (->> (util/get-running-task-ents (db conn))
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
  (:job/max-retries (d/entity (db conn) [:job/uuid (-> ctx ::jobs first)])))

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
  (let [job-owner (:job/user (d/entity (db conn) [:job/uuid job]))]
    (is-authorized-fn request-user :retry impersonator {:owner job-owner :item :job})))

(defn check-retry-allowed
  [conn is-authorized-fn ctx]
  (let [request-user (get-in ctx [:request :authorization/user])
        impersonator (get-in ctx [:request :authorization/impersonator])
        unauthorized-job? #(not (user-can-retry-job? conn is-authorized-fn request-user impersonator %))]
    (or (first (for [guuid (::groups ctx)
                     :let [group (d/entity (db conn) [:group/uuid guuid])
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
                               (::increment ctx)))
            user (get-in ctx [:request :authorization/user])]
        (log/info "Updating retry count for job" job "to" new-retries "for user" user)
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
     ;; :handle-ok and :handle-created both return the number of jobs to be retried,
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
   (s/optional-key :count) s/Num
   (s/optional-key :launch-rate-saved) s/Num
   (s/optional-key :launch-rate-per-minute) s/Num})

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
  [limit-type retract-limit-fn conn is-authorized-fn leadership-atom leader-selector]
  (base-limit-handler
    limit-type is-authorized-fn
    (merge
      (redirect-to-leader leadership-atom leader-selector)
      {:allowed-methods [:delete]
       :delete! (fn [ctx]
                  (let [request-user (get-in ctx [:request :authorization/user])
                        limit-user (get-in ctx [:request :query-params :user])
                        pool (or (get-in ctx [:request :query-params :pool])
                                 (get-in ctx [:request :headers "x-cook-pool"]))
                        reason (get-in ctx [:request :query-params :reason])]
                    (log/info "Retracting limit type" limit-type {:request-user request-user
                                                                  :limit-user limit-user
                                                                  :pool pool
                                                                  :reason reason})
                    (retract-limit-fn conn
                                      limit-user
                                      pool
                                      reason)))})))

(defn coerce-limit-values
  [limits-from-request]
  (into {} (map (fn [[k v]] [k (if (= k :count) (int v) (double v))])
                limits-from-request)))

(defn update-limit-handler
  [limit-type extra-resource-types get-limit-fn set-limit-fn conn is-authorized-fn leadership-atom leader-selector]
  (base-limit-handler
    limit-type is-authorized-fn
    (merge
      (redirect-to-leader leadership-atom leader-selector)
      {:allowed-methods [:post]
       :handle-created (partial retrieve-user-limit get-limit-fn conn)
       :malformed? (fn [ctx]
                     (let [resource-types
                           (set (apply conj (queries/get-all-resource-types (d/db conn))
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
                (let [request-user (get-in ctx [:request :authorization/user])
                      limit-user (get-in ctx [:request :body-params :user])
                      pool (or (get-in ctx [:request :body-params :pool])
                               (get-in ctx [:request :headers "x-cook-pool"]))
                      reason (get-in ctx [:request :body-params :reason])
                      limits (reduce into [] (::limits ctx))]
                  (log/info "Updating limit" limit-type {:request-user request-user
                                                         :limit-user limit-user
                                                         :pool pool
                                                         :reason reason
                                                         :limits limits})
                  (apply set-limit-fn
                         conn
                         limit-user
                         pool
                         reason
                         limits)))})))

(def UserUsageParams
  "User usage endpoint query string parameters"
  {(s/optional-key :user) s/Str
   (s/optional-key :group_breakdown) s/Bool})

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
  {(s/optional-key :total_usage) UsageInfo
   (s/optional-key :grouped) [{:group UsageGroupInfo, :usage UsageInfo}]
   (s/optional-key :ungrouped) JobsUsageResponse})

(def UserUsageResponse
  "Schema for a usage response."
  (assoc UserUsageInPool
    (s/optional-key :pools) {s/Str UserUsageInPool}))

(def UsageResponse
  "Schema for a usage response."
  (assoc UserUsageResponse
    (s/optional-key :users) {s/Str UserUsageResponse}))

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

(defn usage
  "Given a collection of jobs, returns the usage information for those jobs."
  [db pool with-group-breakdown? jobs]
  (if pool
    (user-usage with-group-breakdown? jobs)
    (let [pools (pool/all-pools db)]
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
        (user-usage with-group-breakdown? jobs)))))

(defn get-user-usage
  "Query a user's current resource usage based on running jobs."
  [db ctx]
  (let [user (get-in ctx [:request :query-params :user])
        with-group-breakdown? (get-in ctx [:request :query-params :group_breakdown])
        pool (or (get-in ctx [:request :query-params :pool])
                 (get-in ctx [:request :headers "x-cook-pool"]))
        running-jobs
        (if user
          (if pool
            (util/get-user-running-job-ents-in-pool db user pool)
            (util/get-user-running-job-ents db user))
          (cond->>
            (util/get-running-job-ents db)
            pool (filter
                   #(= pool
                       (cached-queries/job->pool-name %)))))]
    (if user
      (usage db pool with-group-breakdown? running-jobs)
      {:users
       (->> running-jobs
            (group-by cached-queries/job-ent->user)
            (pc/map-vals
              #(usage db pool with-group-breakdown? %)))})))

(defn read-usage-handler
  "Handle GET requests for a user's current usage."
  [conn is-authorized-fn]
  (base-limit-handler
    :usage is-authorized-fn
    {:handle-ok (partial get-user-usage (db conn))}))


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
  [db is-authorized-fn]
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
                 (prom/with-duration
                   prom/endpoint-duration {:endpoint "/list"}
                   (timers/time!
                     (timers/timer ["cook-scheduler" "handler" "list-endpoint-duration"])
                     (let [job-ents (list-job-ents db false ctx)]
                       (doall (mapv (partial fetch-job-map-from-entity db) job-ents))))))))

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
  [conn is-authorized-fn leadership-atom leader-selector]
  (base-cook-handler
    (merge
      ;; only the leader handles unscheduled reasons
      (redirect-to-leader leadership-atom leader-selector)
      {:allowed-methods [:get]
       ; liberator will not route requests to moved temporarily if exists is true, so make sure its
       ; false if not leader.
       :exists? (fn [ctx] (and @leadership-atom (retrieve-jobs conn ctx)))
       :allowed? (partial job-request-allowed? conn is-authorized-fn)
       :handle-ok (fn [ctx]
                    (map (fn [job] {:uuid job
                                    :reasons (job-reasons conn job)})
                         (::jobs ctx)))})))

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
       (prom/with-duration
         prom/endpoint-duration {:endpoint "/stats/instances"}
         (timers/time!
           stats-instances-endpoint
           (let [{status ::status, start ::start, end ::end, name-filter-fn ::name-filter-fn} ctx]
             (task-stats/get-stats conn status start end name-filter-fn)))))}))

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

;;
;; /compute-clusters
;;

(def InsertComputeClusterRequest
  {; IP address / URL of the cluster
   :base-path NonEmptyString
   ; base-64-encoded certificate
   :ca-cert NonEmptyString
   ; Unique name of the cluster
   :name NonEmptyString
   ; State of the cluster
   :state (s/enum
            ; Jobs will get scheduled on this cluster
            "running"
            ; No new jobs / synthetic pods will
            ; get scheduled on this cluster
            "draining"
            ; The cluster is gone
            "deleted")
   ; The state can't be modified when it's locked
   (s/optional-key :state-locked?) s/Bool
   ; Key used to look up the configuration template
   :template NonEmptyString
   (s/optional-key :location) (s/maybe s/Str)
   (s/optional-key :features) [{:key s/Str :value s/Str}]})

(defn compute-cluster-exists?
  [conn name]
  (let [compute-cluster-names (->> (cc/get-compute-clusters conn)
                                   :db-configs
                                   (map :name)
                                   set)]
    (contains? compute-cluster-names name)))

(defn create-or-update-compute-cluster!
  [conn body-params]
  (let [{:keys [name template base-path ca-cert state state-locked?]}
        body-params
        result (cc/update-compute-cluster
                 conn
                 {:name name
                  :template template
                  :base-path base-path
                  :ca-cert ca-cert
                  :state (keyword state)
                  :state-locked? (boolean state-locked?)}
                 true)]
    {:result result :succeeded? (every? #(-> % :update-result :update-succeeded) result)}))

(defn validate-compute-cluster-definition-template
  [ctx]
  (let [template-name (-> ctx :request :body-params :template)
        cluster-definition-template ((config/compute-cluster-templates) template-name)
        {:keys [reason valid?]} (cc/validate-template template-name cluster-definition-template)]
    {:cluster-definition-template cluster-definition-template :reason reason :valid? valid?}))

(defn create-compute-cluster!
  [conn leadership-atom ctx]
  (if @leadership-atom
    (let [cluster-name (-> ctx :request :body-params :name)
          exists? (compute-cluster-exists? conn cluster-name)]
      (if exists?
        [false {::error {:message (str "Compute cluster with name " cluster-name " already exists")}}]
        (let [{:keys [cluster-definition-template reason valid?]} (validate-compute-cluster-definition-template ctx)]
          (if valid?
            (let [body-params (-> ctx :request :body-params)
                  {:keys [result succeeded?]} (create-or-update-compute-cluster! conn body-params)]
              (log/info "Result of create-compute-cluster! REST API call"
                        {:input body-params
                         :result result
                         :succeeded? succeeded?
                         :cluster-definition-template cluster-definition-template})
              (if succeeded?
                [true {:response result}]
                [false {::error {:message "Cluster creation was not successful"
                                 :details result}}]))
            [false {::error {:message reason}}]))))
    ; When we're not the leader, we need processable? to go down
    ; the "true" branch in order to trigger the redirect flow
    true))

(defn update-compute-cluster!
  [conn leadership-atom ctx]
  (if @leadership-atom
    (let [cluster-name (-> ctx :request :body-params :name)
          exists? (compute-cluster-exists? conn cluster-name)]
      (if-not exists?
        [false {::error {:message (str "Compute cluster with name " cluster-name " does not exist")}}]
        (let [{:keys [cluster-definition-template reason valid?]} (validate-compute-cluster-definition-template ctx)]
          (if valid?
            (let [body-params (-> ctx :request :body-params)
                  {:keys [result succeeded?]} (create-or-update-compute-cluster! conn body-params)]
              (log/info "Result of update-compute-cluster! REST API call"
                        {:input body-params
                         :result result
                         :succeeded? succeeded?
                         :cluster-definition-template cluster-definition-template})
              (if succeeded?
                [true {:response result}]
                [false {::error {:message "Cluster update was not successful"
                                 :details result}}]))
            [false {::error {:message reason}}]))))
    ; When we're not the leader, we need processable? to go down
    ; the "true" branch in order to trigger the redirect flow
    true))

(defn check-compute-cluster-allowed
  [is-authorized-fn ctx]
  (let [request-user (get-in ctx [:request :authorization/user])
        impersonator (get-in ctx [:request :authorization/impersonator])
        request-method (get-in ctx [:request :request-method])]
    (if-not (is-authorized-fn request-user
                              request-method
                              impersonator
                              {:owner ::system :item :compute-clusters})
      [false {::error
              (str "You are not authorized to access compute cluster information")}]
      true)))

(defn base-compute-cluster-handler
  [is-authorized-fn leadership-atom leader-selector resource-attrs]
  (base-cook-handler
    (merge
      ;; only the leader handles compute-cluster requests
      (redirect-to-leader leadership-atom leader-selector)
      {:allowed? (partial check-compute-cluster-allowed is-authorized-fn)}
      resource-attrs)))

(defn post-compute-clusters-handler
  [conn is-authorized-fn leadership-atom leader-selector]
  (base-compute-cluster-handler
    is-authorized-fn
    leadership-atom
    leader-selector
    {:allowed-methods [:post]
     :handle-created (fn [{:keys [response]}] response)
     :processable? (partial create-compute-cluster! conn leadership-atom)}))

(defn put-compute-clusters-handler
  [conn is-authorized-fn leadership-atom leader-selector]
  (base-compute-cluster-handler
    is-authorized-fn
    leadership-atom
    leader-selector
    {:allowed-methods [:put]
     :handle-created (fn [{:keys [response]}] response)
     :processable? (partial update-compute-cluster! conn leadership-atom)}))

(defn read-compute-clusters
  [conn _]
  (cc/get-compute-clusters conn))

(defn get-compute-clusters-handler
  [conn is-authorized-fn leadership-atom leader-selector]
  (base-compute-cluster-handler
    is-authorized-fn
    leadership-atom
    leader-selector
    {:allowed-methods [:get]
     :handle-ok (partial read-compute-clusters conn)}))

(defn delete-compute-cluster!
  [conn ctx]
  (cc/delete-compute-cluster conn (-> ctx :request :body-params)))

(defn check-delete-compute-cluster-malformed
  [conn ctx]
  (if-let [name (-> ctx :request :body-params :name)]
    (if (compute-cluster-exists? conn name)
      false
      [true {::error {:message (str "Compute cluster with name " name " does not exist")}}])
    [true {::error {:message (str "You must specify the cluster name to delete")}}]))

(defn delete-compute-clusters-handler
  [conn is-authorized-fn leadership-atom leader-selector]
  (base-compute-cluster-handler
    is-authorized-fn
    leadership-atom
    leader-selector
    {:allowed-methods [:delete]
     :delete! (partial delete-compute-cluster! conn)
     :malformed? (partial check-delete-compute-cluster-malformed conn)}))


;;
;; /shutdown-leader
;;

(def ShutdownLeaderRequest
  {:reason s/Str})

(defn check-shutdown-leader-allowed
  [is-authorized-fn ctx]
  (let [request-user (get-in ctx [:request :authorization/user])
        impersonator (get-in ctx [:request :authorization/impersonator])
        request-method (get-in ctx [:request :request-method])]
    (if-not (is-authorized-fn request-user
                              request-method
                              impersonator
                              {:owner ::system :item :shutdown-leader})
      [false {::error
              (str "You are not authorized to shutdown the leader")}]
      true)))

(defn shutdown!
  []
  (async/thread
    (log/info "Sleeping for 5 seconds before exiting")
    (Thread/sleep 5000)
    (log/info "Exiting now")
    (System/exit 0)))

(defn post-shutdown-leader!
  [ctx]
  (let [reason (get-in ctx [:request :body-params :reason])
        request-user (get-in ctx [:request :authorization/user])]
    (log/info "Exiting due to /shutdown-leader request from" request-user "with reason:" reason)
    (shutdown!)))

(defn post-shutdown-leader-handler
  [is-authorized-fn leadership-atom leader-selector]
  (base-cook-handler
    (merge
      ;; only the leader handles shutdown-leader requests
      (redirect-to-leader leadership-atom leader-selector)
      {:allowed-methods [:post]
       :allowed? (partial check-shutdown-leader-allowed is-authorized-fn)
       :post-enacted? (constantly false)
       :post! post-shutdown-leader!})))

;;
;; /incremental-config
;;

(def IncrementalConfigRequest
  {; configs
   :configs [{; key
              :key s/Keyword
              ; values
              :values [{; actual config value
                        :value s/Str
                        ; number between 0 and 1. portion of jobs that should have the associated value. portions must add up to 1
                        :portion s/Num
                        ; optional comment to describe how this incremental value is different from the others
                        (s/optional-key :comment) s/Str}]}]})

(defn upsert-incremental-configs!
  [conn leadership-atom ctx]
  (let [{:keys [configs]} (-> ctx :request :body-params)
        result (:tempids (config-incremental/write-configs configs))]
    (log/info "Result of upsert-incremental-config! REST API call"
              {:configs configs
               :result result})
    [true {:response result}]))

(defn check-incremental-config-allowed
  [is-authorized-fn ctx]
  (let [request-user (get-in ctx [:request :authorization/user])
        impersonator (get-in ctx [:request :authorization/impersonator])
        request-method (get-in ctx [:request :request-method])]
    (if-not (is-authorized-fn request-user
                              request-method
                              impersonator
                              {:owner ::system :item :incremental-config})
      [false {::error
              (str "You are not authorized to access incremental configurations")}]
      true)))

(defn base-incremental-config-handler
  [is-authorized-fn leadership-atom leader-selector resource-attrs]
  (base-cook-handler
    (merge {:allowed? (partial check-incremental-config-allowed is-authorized-fn)}
           resource-attrs)))

(defn post-incremental-config-handler
  [conn is-authorized-fn leadership-atom leader-selector]
  (base-incremental-config-handler
    is-authorized-fn
    leadership-atom
    leader-selector
    {:allowed-methods [:post]
     :handle-created (fn [{:keys [response]}] response)
     :processable? (partial upsert-incremental-configs! conn leadership-atom)}))

(defn read-incremental-config
  [conn ctx]
  (config-incremental/read-all-configs))

(defn get-incremental-config-handler
  [conn is-authorized-fn leadership-atom leader-selector]
  (base-incremental-config-handler
    is-authorized-fn
    leadership-atom
    leader-selector
    {:allowed-methods [:get]
     :handle-ok (partial read-incremental-config conn)}))

(defn streaming-json-encoder
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
                         JobInstanceProgressRequest (partial pc/map-keys ->kebab-case)
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
                         UsageResponse (partial pc/map-keys ->snake_case)
                         UserUsageResponse (partial pc/map-keys ->snake_case)
                         UserUsageInPool (partial pc/map-keys ->snake_case)
                         UsageGroupInfo (partial pc/map-keys ->snake_case)
                         JobsUsageResponse (partial pc/map-keys ->snake_case)
                         s/Uuid str})]
    (constantly
      (-> c-mw/default-coercion-matchers
          (update :body body-matchers)
          (update :response resp-matchers)))))

(defn- truncate
  "Truncates `in-str` to a maximum length of `max-len` and adds an ellipsis if truncated."
  [in-str max-len]
  (let [ellipsis "..."
        ellipsis-len (count ellipsis)]
    (if (and (string? in-str) (> (count in-str) max-len) (> max-len ellipsis-len))
      (str (subs in-str 0 (- max-len ellipsis-len)) ellipsis)
      in-str)))

(defn- logging-exception-handler
  "Wraps `base-handler` with an additional `log/info` call which logs the request and exception"
  [base-handler]
  (fn logging-exception-handler [ex data req]
    (log/info ex "Error when processing request"
              (-> (dissoc req ::c-mw/options :body :ctrl :request-time :servlet-request :ssl-client-cert)
                  (update-in [:headers] (fn [headers] (pc/map-vals (fn [value] (truncate value 80)) headers)))))
    (base-handler ex data req)))

;;
;; "main" - the entry point that routes to other handlers
;;
(defn main-handler
  [conn mesos-pending-jobs-fn
   {:keys [is-authorized-fn task-constraints]
    gpu-enabled? :mesos-gpu-enabled
    :as settings}
   leader-selector
   leadership-atom
   {:keys [progress-aggregator-chan]}]
  (->
    (routes
      (c-api/api
        {:swagger {:ui "/swagger-ui"
                   :spec "/swagger-docs"
                   :data {:info {:title "Cook API"
                                 :description "How to Cook things"}
                          :tags [{:name "api", :description "some apis"}]}}
         ; Wrap all of the default exception handlers with more logging
         :exceptions {:handlers (pc/map-vals (fn [handler] (logging-exception-handler handler))
                                             (get-in c-mw/api-middleware-defaults [:exceptions :handlers]))}
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
                               404 {:description "The supplied UUIDs don't correspond to valid jobs."}}
                   :handler (read-jobs-handler-deprecated conn is-authorized-fn)}
             :post {:summary "Schedules one or more jobs (deprecated)."
                    :parameters {:body-params RawSchedulerRequestDeprecated}
                    :responses {201 {:description "The jobs were successfully scheduled."}
                                400 {:description "One or more of the jobs were incorrectly specified."}
                                409 {:description "One or more of the jobs UUIDs are already in use."}}
                    :handler (create-jobs-handler conn task-constraints gpu-enabled? is-authorized-fn)}
             :delete {:summary "Cancels jobs, halting execution when possible."
                      :responses {204 {:description "The jobs have been marked for termination."}
                                  400 {:description "Non-UUID values were passed as jobs."}
                                  404 {:description "The supplied UUIDs don't correspond to valid jobs."}}
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
                               404 {:description "The supplied UUID doesn't correspond to a valid job."}}
                   :handler (read-jobs-handler-single conn is-authorized-fn)}}))

        (c-api/context
          "/jobs" []
          (c-api/resource
            {:get {:summary "Returns info about a set of Jobs"
                   :parameters {:query-params QueryJobsParams}
                   :responses {200 {:schema [JobResponse]
                                    :description "The jobs were returned."}
                               400 {:description "Non-UUID values were passed."}
                               404 {:description "The supplied UUIDs don't correspond to valid jobs."}}
                   :handler (read-jobs-handler-multiple conn is-authorized-fn)}
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
                   :handler (cook-info-handler settings leader-selector)}}))

        (c-api/context
          "/instances/:uuid" [uuid]
          :path-params [uuid :- s/Uuid]
          (c-api/resource
            {:get {:summary "Returns info about a single Job Instance"
                   :responses {200 {:schema InstanceResponse
                                    :description "The job instance was returned."}
                               400 {:description "A non-UUID value was passed."}
                               404 {:description "The supplied UUID doesn't correspond to a valid job instance."}}
                   :handler (read-instances-handler-single conn is-authorized-fn)}}))

        (c-api/context
          "/instances" []
          (c-api/resource
            {:get {:summary "Returns info about a set of Job Instances"
                   :parameters {:query-params QueryInstancesParams}
                   :responses {200 {:schema [InstanceResponse]
                                    :description "The job instances were returned."}
                               400 {:description "Non-UUID values were passed."}
                               404 {:description "The supplied UUIDs don't correspond to valid job instances."}}
                   :handler (read-instances-handler-multiple conn is-authorized-fn)}}))

        (c-api/context
          "/share" []
          (c-api/resource
            {:produces ["application/json"],
             :responses {200 {:schema UserLimitsResponse
                              :description "User's share found"}
                         400 {:description "Invalid request format."}
                         401 {:description "Not authorized to read shares."}}
             :get {:summary "Read a user's share"
                   :parameters {:query-params UserParam}
                   :handler (read-limit-handler :share share/get-share conn is-authorized-fn)}
             :post {:summary "Change a user's share"
                    :parameters (set-limit-params :share)
                    :handler (update-limit-handler :share []
                                                   share/get-share share/set-share!
                                                   conn is-authorized-fn
                                                   leadership-atom leader-selector)}
             :delete {:summary "Reset a user's share to the default"
                      :parameters {:query-params UserLimitChangeParams}
                      :handler (destroy-limit-handler :share share/retract-share!
                                                      conn is-authorized-fn
                                                      leadership-atom leader-selector)}}))

        (c-api/context
          "/quota" []
          ;; only the leader handles quota change requests to flush any rate limit state after any changes.
          (c-api/resource
            {:produces ["application/json"],
             :responses {200 {:schema UserLimitsResponse
                              :description "User's quota found"}
                         400 {:description "Invalid request format."}
                         401 {:description "Not authorized to read quota."}}
             :get {:summary "Read a user's quota"
                   :parameters {:query-params UserParam}
                   :handler (read-limit-handler :quota quota/get-quota conn is-authorized-fn)}
             :post {:summary "Change a user's quota"
                    :parameters (set-limit-params :quota)
                    :handler (update-limit-handler :quota [:count :launch-rate-saved :launch-rate-per-minute]
                                                   quota/get-quota quota/set-quota!
                                                   conn is-authorized-fn
                                                   leadership-atom leader-selector)}
             :delete {:summary "Reset a user's quota to the default"
                      :parameters {:query-params UserLimitChangeParams}
                      :handler (destroy-limit-handler :delete
                                                      quota/retract-quota!
                                                      conn is-authorized-fn
                                                      leadership-atom leader-selector)}}))

        (c-api/context
          "/usage" []
          (c-api/resource
            {:produces ["application/json"],
             :responses {200 {:schema UsageResponse
                              :description "User's usage calculated."}
                         400 {:description "Invalid request format."}
                         401 {:description "Not authorized to read usage."}}
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
                               404 {:description "The supplied UUIDs don't correspond to valid groups."}}
                   :handler (groups-action-handler conn task-constraints is-authorized-fn)}
             :delete
             {:summary "Kill all jobs within a set of groups"
              :parameters {:query-params KillGroupsParams}
              :responses {204 {:description "The groups' jobs have been marked for termination."}
                          400 {:description "Non-UUID values were passed."}
                          404 {:description "The supplied UUIDs don't correspond to valid groups."}}
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
                   :handler (read-unscheduled-handler conn is-authorized-fn leadership-atom leader-selector)
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
                         400 {:description "Invalid request format."}
                         401 {:description "Not authorized to read stats."}}
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
                   :handler (pools-handler)}}))

        (c-api/context
          "/shutdown-leader" []
          (c-api/resource
            {:produces ["application/json"]

             :post
             {:summary "Shutdown the Cook leader"
              :parameters {:body-params ShutdownLeaderRequest}
              :handler (post-shutdown-leader-handler is-authorized-fn
                                                     leadership-atom
                                                     leader-selector)
              :responses {202 {:description "The shutdown request was accepted."}
                          307 {:description "Redirecting request to leader node."}
                          400 {:description "Invalid request format."}}}}))

        (c-api/context
          "/compute-clusters" []
          (c-api/resource
            {:produces ["application/json"]

             :post
             {:summary "Create a new compute cluster"
              :parameters {:body-params InsertComputeClusterRequest}
              :handler (post-compute-clusters-handler conn
                                                      is-authorized-fn
                                                      leadership-atom
                                                      leader-selector)
              :responses {201 {:description "The compute cluster was created."}
                          307 {:description "Redirecting request to leader node."}
                          409 {:description "There is already a compute cluster with the given name."}}}

             :put
             {:summary "Update an existing compute cluster"
              :parameters {:body-params InsertComputeClusterRequest}
              :handler (put-compute-clusters-handler conn
                                                     is-authorized-fn
                                                     leadership-atom
                                                     leader-selector)
              :responses {201 {:description "The compute cluster was updated."}
                          307 {:description "Redirecting request to leader node."}
                          404 {:description "Could not find a compute cluster with the given name."}}}

             :get
             {:summary "Get the set of compute clusters"
              :handler (get-compute-clusters-handler conn
                                                     is-authorized-fn
                                                     leadership-atom
                                                     leader-selector)
              :responses {200 {:description "The compute clusters were returned."}
                          307 {:description "Redirecting request to leader node."}}}

             :delete
             {:summary "Deletes a compute cluster"
              :handler (delete-compute-clusters-handler conn
                                                        is-authorized-fn
                                                        leadership-atom
                                                        leader-selector)
              :responses {204 {:description "The compute cluster was deleted."}
                          307 {:description "Redirecting request to leader node."}}}}))

        (c-api/context
          "/incremental-config" []
          (c-api/resource
            {:produces ["application/json"]

             :post
             {:summary "Upsert the dynamic incremental configuration specified by the key"
              :parameters {:body-params IncrementalConfigRequest}
              :handler (post-incremental-config-handler conn
                                                        is-authorized-fn
                                                        leadership-atom
                                                        leader-selector)
              :responses {201 {:description "The incremental configuration was upserted."}
                          307 {:description "Redirecting request to leader node."}}}

             :get
             {:summary "Get the dynamic incremental configuration specified by the key"
              :handler (get-incremental-config-handler conn
                                                       is-authorized-fn
                                                       leadership-atom
                                                       leader-selector)
              :responses {200 {:description "The incremental configuration was returned."}
                          307 {:description "Redirecting request to leader node."}}}}))
        (c-api/context
          ;; endpoint for prometheus metrics
          "/metrics" []
          (c-api/resource
            ;; NOTE: The authentication for this endpoint is disabled via cook.components/conditional-auth-bypass
            {:produces ["text/plain"]
             :get
             {:summary "Get metrics"
              :response {200 {:description "OK"}
                         500 {:description "Internal server error"}}
              :handler (metrics-handler)}}))
        (c-api/undocumented
          ;; internal api endpoints (don't include in swagger)
          (c-api/context
            "/progress/:uuid" [uuid]
            :path-params [uuid :- s/Uuid]
            (c-api/resource
              ;; NOTE: The authentication for this endpoint is disabled via cook.components/conditional-auth-bypass
              {:post {:summary "Update the progress of a Job Instance"
                      :parameters {:body-params JobInstanceProgressRequest}
                      :responses {202 {:description "The progress update was accepted."}
                                  307 {:description "Redirecting request to leader node."}
                                  400 {:description "Invalid request format."}
                                  404 {:description "The supplied UUID doesn't correspond to a valid job instance."}
                                  503 {:description "The leader node is temporarily unavailable."}}
                      :handler (let [;; TODO: add lightweight auth -- https://github.com/twosigma/Cook/issues/1367
                                     mock-auth-fn (constantly true)]
                                 (update-instance-progress-handler
                                   conn mock-auth-fn leadership-atom leader-selector progress-aggregator-chan))}}))))

      (ANY "/queue" []
        (waiting-jobs conn mesos-pending-jobs-fn is-authorized-fn leadership-atom leader-selector))
      (ANY "/running" []
        (running-jobs conn is-authorized-fn))
      (ANY "/list" []
        (list-resource (d/db conn) is-authorized-fn)))
    (format-params/wrap-restful-params {:formats [:json-kw]
                                        :handle-error c-mw/handle-req-error})
    (streaming-json-middleware)))
