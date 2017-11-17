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
(ns cook.mesos.scheduler
  (:require [chime :refer [chime-at chime-ch]]
            [clj-time.coerce :as tc]
            [clj-time.core :as time]
            [clj-time.periodic :as periodic]
            [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cook.components :refer (default-fitness-calculator)]
            [cook.datomic :refer (transact-with-retries)]
            [cook.mesos.constraints :as constraints]
            [cook.mesos.dru :as dru]
            [cook.mesos.fenzo-utils :as fenzo]
            [cook.mesos.task :as task]
            [cook.mesos.group :as group]
            [cook.mesos.heartbeat :as heartbeat]
            [cook.mesos.quota :as quota]
            [cook.mesos.reason :as reason]
            [cook.mesos.sandbox :as sandbox]
            [cook.mesos.share :as share]
            [cook.mesos.task :as task]
            [cook.mesos.util :as util]
            [datomic.api :as d :refer (q)]
            [mesomatic.scheduler :as mesos]
            [metatransaction.core :refer (db)]
            [metrics.counters :as counters]
            [metrics.gauges :as gauges]
            [metrics.histograms :as histograms]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [plumbing.core :as pc])
  (import [com.netflix.fenzo ConstraintEvaluator ConstraintEvaluator$Result
                             TaskAssignmentResult TaskRequest TaskScheduler TaskScheduler$Builder
                             VMTaskFitnessCalculator VirtualMachineLease VirtualMachineLease$Range
                             VirtualMachineCurrentState]
          [com.netflix.fenzo.functions Action1 Func1]))

(defn now
  []
  (tc/to-date (time/now)))

(defn offer-resource-values
  [offer resource-name value-type]
  (->> offer :resources (filter #(= (:name %) resource-name)) (map value-type)))

(defn offer-resource-scalar
  [offer resource-name]
  (reduce + 0.0 (offer-resource-values offer resource-name :scalar)))

(defn offer-resource-ranges
  [offer resource-name]
  (reduce into [] (offer-resource-values offer resource-name :ranges)))

(defn get-offer-attr-map
  "Gets all the attributes from an offer and puts them in a simple, less structured map of the form
   name->value"
  [offer]
  (let [mesos-attributes (->> offer
                              :attributes
                              (map #(vector (:name %) (case (:type %)
                                                        :value-scalar (:scalar %)
                                                        :value-ranges (:ranges %)
                                                        :value-set (:set %)
                                                        :value-text (:text %)
                                                        ; Default
                                                        (:value %))))
                              (into {}))
        cook-attributes {"HOSTNAME" (:hostname offer)
                         "COOK_GPU?" (-> offer
                                         (offer-resource-scalar "gpus")
                                         (or 0.0)
                                         (> 0))}]
    (merge mesos-attributes cook-attributes)))

(timers/deftimer [cook-mesos scheduler handle-status-update-duration])
(timers/deftimer [cook-mesos scheduler handle-framework-message-duration])
(meters/defmeter [cook-mesos scheduler tasks-killed-in-status-update])

(meters/defmeter [cook-mesos scheduler tasks-completed])
(meters/defmeter [cook-mesos scheduler tasks-completed-mem])
(meters/defmeter [cook-mesos scheduler tasks-completed-cpus])
(histograms/defhistogram [cook-mesos scheduler hist-task-complete-times])
(meters/defmeter [cook-mesos scheduler task-complete-times])

(meters/defmeter [cook-mesos scheduler tasks-succeeded])
(meters/defmeter [cook-mesos scheduler tasks-succeeded-mem])
(meters/defmeter [cook-mesos scheduler tasks-succeeded-cpus])
(histograms/defhistogram [cook-mesos scheduler hist-task-succeed-times])
(meters/defmeter [cook-mesos scheduler task-succeed-times])

(meters/defmeter [cook-mesos scheduler tasks-failed])
(meters/defmeter [cook-mesos scheduler tasks-failed-mem])
(meters/defmeter [cook-mesos scheduler tasks-failed-cpus])
(histograms/defhistogram [cook-mesos scheduler hist-task-fail-times])
(meters/defmeter [cook-mesos scheduler task-fail-times])


(def success-throughput-metrics
  {:completion-rate tasks-succeeded
   :completion-mem tasks-succeeded-mem
   :completion-cpus tasks-succeeded-cpus
   :completion-hist-run-times hist-task-succeed-times
   :completion-MA-run-times task-succeed-times})

(def fail-throughput-metrics
  {:completion-rate tasks-failed
   :completion-mem tasks-failed-mem
   :completion-cpus tasks-failed-cpus
   :completion-hist-run-times hist-task-fail-times
   :completion-MA-run-times task-fail-times})

(def complete-throughput-metrics
  {:completion-rate tasks-completed
   :completion-mem tasks-completed-mem
   :completion-cpus tasks-completed-cpus
   :completion-hist-run-times hist-task-complete-times
   :completion-MA-run-times task-complete-times})

(defn handle-throughput-metrics [{:keys [completion-rate completion-mem completion-cpus
                                         completion-hist-run-times completion-MA-run-times]}
                                 job-resources
                                 run-time]
  (meters/mark! completion-rate)
  (meters/mark!
    completion-mem
    (:mem job-resources))
  (meters/mark!
    completion-cpus
    (:cpus job-resources))
  (histograms/update!
    completion-hist-run-times
    run-time)
  (meters/mark!
    completion-MA-run-times
    run-time))

(defn interpret-task-status
  "Converts the status packet from Mesomatic into a more friendly data structure"
  [s]
  {:task-id (-> s :task-id :value)
   :reason (:reason s)
   :task-state (:state s)
   :progress (try
               (when (:data s)
                 (:percent (edn/read-string (String. (.toByteArray (:data s))))))
               (catch Exception e
                 (try (log/debug e (str "Error parsing mesos status data to edn."
                                        "Is it in the format we expect?"
                                        "String representation: "
                                        (String. (.toByteArray (:data s)))))
                      (catch Exception e
                        (log/debug e "Error reading a string from mesos status data. Is it in the format we expect?")))))})

(defn handle-status-update
  "Takes a status update from mesos."
  [conn driver ^TaskScheduler fenzo sync-agent-sandboxes-fn status]
  (log/info "Mesos status is:" status)
  (timers/time!
    handle-status-update-duration
    (try (let [db (db conn)
               {:keys [task-id reason task-state progress]} (interpret-task-status status)
               _ (when-not task-id
                   (throw (ex-info "task-id is nil. Something unexpected has happened."
                                   {:status status
                                    :task-id task-id
                                    :reason reason
                                    :task-state task-state
                                    :progress progress})))
               [job instance prior-instance-status] (first (q '[:find ?j ?i ?status
                                                                :in $ ?task-id
                                                                :where
                                                                [?i :instance/task-id ?task-id]
                                                                [?i :instance/status ?s]
                                                                [?s :db/ident ?status]
                                                                [?j :job/instance ?i]]
                                                              db task-id))
               job-ent (d/entity db job)
               instance-ent (d/entity db instance)
               previous-reason (reason/instance-entity->reason-entity db instance-ent)
               instance-status (condp contains? task-state
                                 #{:task-staging} :instance.status/unknown
                                 #{:task-starting
                                   :task-running} :instance.status/running
                                 #{:task-finished} :instance.status/success
                                 #{:task-failed
                                   :task-killed
                                   :task-lost
                                   :task-error} :instance.status/failed)
               prior-job-state (:job/state (d/entity db job))
               current-time (now)
               instance-runtime (- (.getTime current-time) ; Used for reporting
                                   (.getTime (or (:instance/start-time instance-ent) current-time)))
               job-resources (util/job-ent->resources job-ent)]
           (when (#{:instance.status/success :instance.status/failed} instance-status)
             (log/debug "Unassigning task" task-id "from" (:instance/hostname instance-ent))
             (try
               (locking fenzo
                 (.. fenzo
                     (getTaskUnAssigner)
                     (call task-id (:instance/hostname instance-ent))))
               (catch Exception e
                 (log/error e "Failed to unassign task" task-id "from" (:instance/hostname instance-ent)))))
           (when (= instance-status :instance.status/success)
             (handle-throughput-metrics success-throughput-metrics
                                        job-resources
                                        instance-runtime)
             (handle-throughput-metrics complete-throughput-metrics
                                        job-resources
                                        instance-runtime))
           (when (= instance-status :instance.status/failed)
             (handle-throughput-metrics fail-throughput-metrics
                                        job-resources
                                        instance-runtime)
             (handle-throughput-metrics complete-throughput-metrics
                                        job-resources
                                        instance-runtime))
           ;; This code kills any task that "shouldn't" be running
           (when (and
                   (or (nil? instance) ; We could know nothing about the task, meaning a DB error happened and it's a waste to finish
                       (= prior-job-state :job.state/completed) ; The task is attached to a failed job, possibly due to instances running on multiple hosts
                       (= prior-instance-status :instance.status/failed)) ; The kill-task message could've been glitched on the network
                   (contains? #{:task-running
                                :task-staging
                                :task-starting}
                              task-state)) ; killing an unknown task causes a TASK_LOST message. Break the cycle! Only kill non-terminal tasks
             (log/warn "Attempting to kill task" task-id
                       "as instance" instance "with" prior-job-state "and" prior-instance-status
                       "should've been put down already")
             (meters/mark! tasks-killed-in-status-update)
             (mesos/kill-task! driver {:value task-id}))
           (when-not (nil? instance)
             (when (and (not= :executor/cook (:instance/executor instance-ent))
                        (#{:task-starting :task-running} task-state))
               (sync-agent-sandboxes-fn (:instance/hostname instance-ent)))
             ;; (println "update:" task-id task-state job instance instance-status prior-job-state)
             (log/debug "Transacting updated state for instance" instance "to status" instance-status)
             ;; The database can become inconsistent if we make multiple calls to :instance/update-state in a single
             ;; transaction; see the comment in the definition of :instance/update-state for more details
             (transact-with-retries
               conn
               (reduce
                 into
                 [[:instance/update-state instance instance-status (or (:db/id previous-reason)
                                                                       (reason/mesos-reason->cook-reason-entity-id db reason)
                                                                       [:reason.name :unknown])]] ; Warning: Default is not mea-culpa
                 [(when (and (#{:instance.status/failed} instance-status) (not previous-reason) reason)
                    [[:db/add instance :instance/reason (reason/mesos-reason->cook-reason-entity-id db reason)]])
                  (when (and (#{:instance.status/success
                                :instance.status/failed} instance-status)
                             (nil? (:instance/end-time instance-ent)))
                    [[:db/add instance :instance/end-time (now)]])
                  (when (and (#{:task-starting :task-running} task-state)
                             (nil? (:instance/mesos-start-time instance-ent)))
                    [[:db/add instance :instance/mesos-start-time (now)]])
                  (when progress
                    [[:db/add instance :instance/progress progress]])]))))
         (catch Exception e
           (log/error e "Mesos scheduler status update error")))))

(defn- task-id->instance-id
  "Retrieves the instance-id given a task-id"
  [db task-id]
  (-> (d/entity db [:instance/task-id task-id])
      :db/id))

(meters/defmeter [cook-mesos scheduler progress-aggregator-message-rate])
(meters/defmeter [cook-mesos scheduler progress-aggregator-drop-rate])
(counters/defcounter [cook-mesos scheduler progress-aggregator-drop-count])
(counters/defcounter [cook-mesos scheduler progress-aggregator-pending-states-count])

(defn progress-aggregator
  "Aggregates the progress state specified in `data` into the current progress state `instance-id->progress-state`.
   It drops messages if the aggregated state has more than `pending-threshold` different entries.
   It returns the new progress state.
   The aggregator also makes a local best-effort to avoid updating the progress state of individual instances with
   stale data based on the value of progress-sequence in the message.
   We avoid this stale check in the datomic transaction as it is relatively expensive to perform the checks on a per
   instance basis in the query."
  [pending-threshold sequence-cache-store instance-id->progress-state {:keys [instance-id] :as data}]
  (meters/mark! progress-aggregator-message-rate)
  (if (or (< (count instance-id->progress-state) pending-threshold)
          (contains? instance-id->progress-state instance-id))
    (if (integer? (:progress-sequence data))
      (let [progress-state (select-keys data [:progress-message :progress-percent :progress-sequence])
            instance-id->progress-state' (update instance-id->progress-state instance-id
                                                 (fn [current-state]
                                                   (let [new-progress-sequence (:progress-sequence progress-state)
                                                         old-progress-sequence (util/cache-lookup! sequence-cache-store instance-id -1)]
                                                     (if (or (nil? current-state)
                                                             (< old-progress-sequence new-progress-sequence))
                                                       (do
                                                         (util/cache-update! sequence-cache-store instance-id new-progress-sequence)
                                                         progress-state)
                                                       current-state))))]
        (let [old-count (count instance-id->progress-state)
              new-count (count instance-id->progress-state')]
          (when (zero? old-count)
            (counters/clear! progress-aggregator-pending-states-count))
          (counters/inc! progress-aggregator-pending-states-count (- new-count old-count)))
        instance-id->progress-state')
      (do
        (log/warn "skipping" data "as it is missing an integer progress-sequence")
        instance-id->progress-state))
    (do
      (meters/mark! progress-aggregator-drop-rate)
      (counters/inc! progress-aggregator-drop-count)
      (log/debug "Dropping" data "as threshold has been reached")
      instance-id->progress-state)))

(defn progress-update-aggregator
  "Launches a long running go block that triggers publishing the latest aggregated instance-id->progress-state
   wrapped inside a chan whenever there is a read on the progress-state-chan.
   It drops messages if the progress-aggregator-chan queue is larger than pending-threshold or the aggregated
   state has more than pending-threshold different entries.
   It returns the progress-aggregator-chan which can be used to send progress-state messages to the aggregator.

   Note: the wrapper chan is used due to our use of `util/xform-pipe`"
  [{:keys [pending-threshold publish-interval-ms sequence-cache-threshold]} progress-state-chan]
  (log/info "Starting progress update aggregator")
  (let [progress-aggregator-chan (async/chan (async/sliding-buffer pending-threshold))
        sequence-cache-store (-> {}
                                 (cache/lru-cache-factory :threshold sequence-cache-threshold)
                                 (cache/ttl-cache-factory :ttl (* 2 publish-interval-ms))
                                 atom)
        progress-aggregator-fn (fn progress-aggregator-fn [instance-id->progress-state data]
                                 (progress-aggregator pending-threshold sequence-cache-store instance-id->progress-state data))
        aggregator-go-chan (util/reducing-pipe progress-aggregator-chan progress-aggregator-fn progress-state-chan
                                               :initial-state {})]
    (async/go
      (async/<! aggregator-go-chan)
      (log/info "Progress update aggregator exited"))
    progress-aggregator-chan))

(histograms/defhistogram [cook-mesos scheduler progress-updater-pending-states])
(meters/defmeter [cook-mesos scheduler progress-updater-publish-rate])
(timers/deftimer [cook-mesos scheduler progress-updater-publish-duration])
(meters/defmeter [cook-mesos scheduler progress-updater-tx-rate])
(timers/deftimer [cook-mesos scheduler progress-updater-tx-duration])

(defn- publish-progress-to-datomic!
  "Transacts the latest aggregated instance-id->progress-state to datomic.
   No more than batch-size facts are updated in individual datomic transactions."
  [conn instance-id->progress-state batch-size]
  (histograms/update! progress-updater-pending-states (count instance-id->progress-state))
  (meters/mark! progress-updater-publish-rate)
  (timers/time!
    progress-updater-publish-duration
    (doseq [instance-id->progress-state-partition
            (partition-all batch-size instance-id->progress-state)]
      (try
        (letfn [(build-progress-txns [[instance-id {:keys [progress-percent progress-message]}]]
                  (cond-> []
                          progress-percent (conj [:db/add instance-id :instance/progress (int progress-percent)])
                          progress-message (conj [:db/add instance-id :instance/progress-message progress-message])))]
          (let [txns (mapcat build-progress-txns instance-id->progress-state-partition)]
            (when (seq txns)
              (log/info "Performing" (count txns) "in progress state update")
              (meters/mark! progress-updater-tx-rate)
              (timers/time!
                progress-updater-tx-duration
                @(d/transact conn txns)))))
        (catch Exception e
          (log/error e "Progress batch update error"))))))

(defn progress-update-transactor
  "Launches a long running go block that triggers transacting the latest aggregated instance-id->progress-state
   to datomic whenever there is a message on the progress-updater-trigger-chan.
   No more than batch-size facts are updated in individual datomic transactions.
   It returns a map containing the progress-state-chan which can be used to send messages about the latest
   instance-id->progress-state which must be wrapped inside a channel. The producer must guarantee that this
   channel is promptly fulfilled if it is successfully put in the progress-state-chan.
   If no such message is on the progress-state-chan, then no datomic interactions occur."
  [progress-updater-trigger-chan batch-size conn]
  (log/info "Starting progress update transactor")
  (let [progress-state-chan (async/chan)]
    (letfn [(progress-update-transactor-error-handler [e]
              (log/error e "Failed to update progress message on tasks!"))
            (progress-update-transactor-on-finished []
              (log/info "Exiting progress update transactor"))
            (process-progress-update-transactor-event []
              ;; progress-state-chan is expected to receive a promise-chan that contains the instance-id->progress-state
              (let [[instance-id->progress-state _] (async/alts!! [progress-state-chan (async/timeout 100)] :priority true)]
                (when instance-id->progress-state
                  (log/info "Received" (count instance-id->progress-state) "in progress-update-transactor")
                  (publish-progress-to-datomic! conn instance-id->progress-state batch-size))))]
      {:cancel-handle (util/chime-at-ch progress-updater-trigger-chan process-progress-update-transactor-event
                                        {:error-handler progress-update-transactor-error-handler
                                         :on-finished progress-update-transactor-on-finished})
       :progress-state-chan progress-state-chan})))

(defn- handle-progress-message!
  "Processes a progress message by sending it along the progress-aggregator-chan channel."
  [progress-aggregator-chan progress-message-map]
  (async/put! progress-aggregator-chan progress-message-map))

(defn handle-framework-message
  "Processes a framework message from Mesos."
  [conn handle-progress-message
   {:strs [exit-code progress-message progress-percent progress-sequence task-id] :as message}]
  (log/info "Received framework message:" {:task-id task-id, :message message})
  (timers/time!
    handle-framework-message-duration
    (try
      (when (str/blank? task-id)
        (throw (ex-info "task-id is empty in framework message" {:message message})))
      (let [instance-id (task-id->instance-id (db conn) task-id)]
        (if (nil? instance-id)
          (throw (ex-info "No instance found!" {:task-id task-id}))
          (do
            (when (or progress-message progress-percent)
              (log/debug "Updating instance" instance-id "progress to" progress-percent progress-message)
              (handle-progress-message {:instance-id instance-id
                                        :progress-message progress-message
                                        :progress-percent progress-percent
                                        :progress-sequence progress-sequence}))
            (when exit-code
              (log/info "Updating instance" instance-id "exit-code to" exit-code)
              (transact-with-retries conn [[:db/add instance-id :instance/exit-code (int exit-code)]])))))
      (catch Exception e
        (log/error e "Mesos scheduler framework message error")))))

(timers/deftimer [cook-mesos scheduler tx-report-queue-processing-duration])
(meters/defmeter [cook-mesos scheduler tx-report-queue-datoms])
(meters/defmeter [cook-mesos scheduler tx-report-queue-update-job-state])
(meters/defmeter [cook-mesos scheduler tx-report-queue-job-complete])
(meters/defmeter [cook-mesos scheduler tx-report-queue-tasks-killed])

(defn monitor-tx-report-queue
  "Takes an async channel that will have tx report queue elements on it"
  [tx-report-chan conn driver-ref]
  (log/info "Starting tx-report-queue")
  (let [kill-chan (async/chan)]
    (async/go
      (loop []
        (async/alt!
          tx-report-chan ([tx-report]
                           (async/go
                             (timers/start-stop-time! ; Use this in go blocks, time! doesn't play nice
                               tx-report-queue-processing-duration
                               (let [{:keys [tx-data db-before]} tx-report
                                     db (db conn)]
                                 (meters/mark! tx-report-queue-datoms (count tx-data))
                                 ;; Monitoring whether a job is completed.
                                 (doseq [{:keys [e a v]} tx-data]
                                   (try
                                     (when (and (= a (d/entid db :job/state))
                                                (= v (d/entid db :job.state/completed)))
                                       (meters/mark! tx-report-queue-job-complete)
                                       (doseq [[task-id] (q '[:find ?task-id
                                                              :in $ ?job [?status ...]
                                                              :where
                                                              [?job :job/instance ?i]
                                                              [?i :instance/status ?status]
                                                              [?i :instance/task-id ?task-id]]
                                                            db e [:instance.status/unknown
                                                                  :instance.status/running])]
                                         (if-let [driver @driver-ref]
                                           (do (log/info "Attempting to kill task" task-id "due to job completion")
                                               (meters/mark! tx-report-queue-tasks-killed)
                                               (mesos/kill-task! driver {:value task-id}))
                                           (log/error "Couldn't kill task" task-id "due to no Mesos driver!"))))
                                     (catch Exception e
                                       (log/error e "Unexpected exception on tx report queue processor")))))))
                           (recur))
          kill-chan ([_] nil))))
    #(async/close! kill-chan)))

;;; ===========================================================================
;;; API for matcher
;;; ===========================================================================

(defrecord VirtualMachineLeaseAdapter [offer time]
  VirtualMachineLease
  (cpuCores [_] (or (offer-resource-scalar offer "cpus") 0.0))
  (diskMB [_] (or (offer-resource-scalar offer "disk") 0.0))
  (getScalarValue [_ name] (or (double (offer-resource-scalar offer name)) 0.0))
  (getScalarValues [_]
    (reduce (fn [result resource]
              (if-let [value (:scalar resource)]
                ;; Do not remove the following fnil--either arg to + can be nil!
                (update-in result [(:name resource)] (fnil + 0.0 0.0) value)
                result))
            {}
            (:resources offer)))
  ; Some Fenzo plugins (which are included with fenzo, such as host attribute constraints) expect the "HOSTNAME"
  ; attribute to contain the hostname of this virtual machine.
  (getAttributeMap [_] (get-offer-attr-map offer))
  (getId [_] (-> offer :id :value))
  (getOffer [_] (throw (UnsupportedOperationException.)))
  (getOfferedTime [_] time)
  (getVMID [_] (-> offer :slave-id :value))
  (hostname [_] (:hostname offer))
  (memoryMB [_] (or (offer-resource-scalar offer "mem") 0.0))
  (networkMbps [_] 0.0)
  (portRanges [_] (mapv (fn [{:keys [begin end]}]
                          (VirtualMachineLease$Range. begin end))
                        (offer-resource-ranges offer "ports"))))

(defn novel-host-constraint
  "This returns a Fenzo hard constraint that ensures the given job won't run on the same host again"
  [job]
  (reify ConstraintEvaluator
    (getName [_] "novel_host_constraint")
    (evaluate [_ task-request target-vm task-tracker-state]
      (let [previous-hosts (->> (:job/instance job)
                                (remove #(true? (:instance/preempted? %)))
                                (mapv :instance/hostname))]
        (ConstraintEvaluator$Result.
          (not-any? #(= % (.getHostname target-vm))
                    previous-hosts)
          (str "Can't run on " (.getHostname target-vm)
               " since we already ran on that: " previous-hosts))))))

(defn gpu-host-constraint
  "This returns a Fenzo hard constraint that ensure that if the givn job requires gpus, it will be assigned
   to a GPU host, and if it doesn't require gpus, it will be assigned to a non-GPU host."
  [job]
  (let [job-needs-gpus? (fn [job]
                          (delay (->> (:job/resource job)
                                      (filter (fn gpu-resource? [res]
                                                (and (= (:resource/type res) :resource.type/gpus)
                                                     (pos? (:resource/amount res)))))
                                      (seq)
                                      (boolean))))
        needs-gpus? (job-needs-gpus? job)]
    (reify ConstraintEvaluator
      (getName [_] (str (if @needs-gpus? "" "non_") "gpu_host_constraint"))
      (evaluate [_ task-request target-vm task-tracker-state]
        (let [has-gpus? (boolean (or (> (or (.getScalarValue (.getCurrAvailableResources target-vm) "gpus") 0.0) 0)
                                     (some (fn gpu-task? [req]
                                             @(job-needs-gpus? (:job req)))
                                           (into (vec (.getRunningTasks target-vm))
                                                 (mapv #(.getRequest %) (.getTasksCurrentlyAssigned target-vm))))))]
          (ConstraintEvaluator$Result.
            (if @needs-gpus?
              has-gpus?
              (not has-gpus?))
            (str "The machine " (.getHostname target-vm) (if @needs-gpus? " doesn't have" " has") " gpus")))))))

(defrecord TaskRequestAdapter [job resources task-id assigned-resources guuid->considerable-cotask-ids constraints needs-gpus? scalar-requests]
  TaskRequest
  (getCPUs [_] (:cpus resources))
  (getDisk [_] 0.0)
  (getHardConstraints [_] constraints)
  (getId [_] task-id)
  (getScalarRequests [_] scalar-requests)
  (getAssignedResources [_] @assigned-resources)
  (setAssignedResources [_ v] (reset! assigned-resources v))
  (getCustomNamedResources [_] {})
  (getMemory [_] (:mem resources))
  (getNetworkMbps [_] 0.0)
  (getPorts [_] (:ports resources))
  (getSoftConstraints [_] [])
  (taskGroupName [_] (str (:job/uuid job))))

(defn make-task-request
  "Helper to create a TaskRequest using TaskRequestAdapter. TaskRequestAdapter implements Fenzo's TaskRequest interface
   given a job, its resources, its task-id and a function assigned-cotask-getter. assigned-cotask-getter should be a
   function that takes a group uuid and returns a set of task-ids, which correspond to the tasks that will be assigned
   during the same Fenzo scheduling cycle as the newly created TaskRequest."
  [db job & {:keys [resources task-id assigned-resources guuid->considerable-cotask-ids]
          :or {resources (util/job-ent->resources job)
               task-id (str (java.util.UUID/randomUUID))
               assigned-resources (atom nil)
               guuid->considerable-cotask-ids (constantly #{})}}]
  (let [constraints (into (constraints/make-fenzo-job-constraints job)
                          (remove nil?
                                  (mapv (fn make-group-constraints [group]
                                          (constraints/make-fenzo-group-constraint
                                           db group #(guuid->considerable-cotask-ids (:group/uuid group))))
                                        (:group/_job job))))
        needs-gpus? (constraints/job-needs-gpus? job)
        scalar-requests (reduce (fn [result resource]
                                  (if-let [value (:resource/amount resource)]
                                    (assoc result (name (:resource/type resource)) value)
                                    result))
                                {}
                                (:job/resource job))]
    (->TaskRequestAdapter job resources task-id assigned-resources guuid->considerable-cotask-ids constraints needs-gpus? scalar-requests)))

(defn match-offer-to-schedule
  "Given an offer and a schedule, computes all the tasks should be launched as a result.

   A schedule is just a sorted list of tasks, and we're going to greedily assign them to
   the offer.

   Returns {:matches (list of tasks that got matched to the offer)
            :failures (list of unmatched tasks, and why they weren't matched)}"
  [db ^TaskScheduler fenzo considerable offers]
  (log/debug "Matching" (count offers) "offers to" (count considerable) "jobs with fenzo")
  (log/debug "offers to scheduleOnce" offers)
  (log/debug "tasks to scheduleOnce" considerable)
  (let [t (System/currentTimeMillis)
        _ (log/debug "offer to scheduleOnce" offers)
        _ (log/debug "tasks to scheduleOnce" considerable)
        leases (mapv #(->VirtualMachineLeaseAdapter % t) offers)
        considerable->task-id (plumbing.core/map-from-keys (fn [_] (str (java.util.UUID/randomUUID))) considerable)
        guuid->considerable-cotask-ids (util/make-guuid->considerable-cotask-ids considerable->task-id)
        ; Important that requests maintains the same order as considerable
        requests (mapv (fn [job]
                         (make-task-request db job :task-id (considerable->task-id job) :guuid->considerable-cotask-ids guuid->considerable-cotask-ids))
                       considerable)
        ;; Need to lock on fenzo when accessing scheduleOnce because scheduleOnce and
        ;; task assigner can not be called at the same time.
        ;; task assigner may be called when reconciling
        result (locking fenzo
                 (.scheduleOnce fenzo requests leases))
        failure-results (.. result getFailures values)
        assignments (.. result getResultMap values)]

    (log/debug "Found this assignment:" result)

    {:matches (mapv (fn [assignment]
                      {:leases (.getLeasesUsed assignment)
                       :tasks (.getTasksAssigned assignment)
                       :hostname (.getHostname assignment)})
                    assignments)
     :failures failure-results}))

(meters/defmeter [cook-mesos scheduler scheduler-offer-declined])

(defn decline-offers
  "declines a collection of offer ids"
  [driver offer-ids]
  (log/debug "Declining offers:" offer-ids)
  (doseq [id offer-ids]
    (meters/mark! scheduler-offer-declined)
    (mesos/decline-offer driver id)))

(timers/deftimer [cook-mesos scheduler handle-resource-offer!-duration])
(timers/deftimer [cook-mesos scheduler handle-resource-offer!-transact-task-duration])
(timers/deftimer [cook-mesos scheduler handle-resource-offer!-process-matches-duration])
(timers/deftimer [cook-mesos scheduler handle-resource-offer!-mesos-submit-duration])
(timers/deftimer [cook-mesos scheduler handle-resource-offer!-match-duration])
(timers/deftimer [cook-mesos scheduler handle-resource-offer!-considerable-jobs-duration])
(timers/deftimer [cook-mesos scheduler handle-resource-offer!-match-job-uuids-duration])
(meters/defmeter [cook-mesos scheduler pending-job-atom-contended])

(histograms/defhistogram [cook-mesos scheduler offer-size-mem])
(histograms/defhistogram [cook-mesos scheduler offer-size-cpus])
(histograms/defhistogram [cook-mesos scheduler number-tasks-matched])
(histograms/defhistogram [cook-mesos-scheduler number-offers-matched])
(meters/defmeter [cook-mesos scheduler scheduler-offer-matched])
(meters/defmeter [cook-mesos scheduler handle-resource-offer!-errors])
(meters/defmeter [cook-mesos scheduler matched-tasks])
(meters/defmeter [cook-mesos scheduler matched-tasks-cpus])
(meters/defmeter [cook-mesos scheduler matched-tasks-mem])
(def front-of-job-queue-mem-atom (atom 0))
(def front-of-job-queue-cpus-atom (atom 0))
(gauges/defgauge [cook-mesos scheduler front-of-job-queue-mem] (fn [] @front-of-job-queue-mem-atom))
(gauges/defgauge [cook-mesos scheduler front-of-job-queue-cpus] (fn [] @front-of-job-queue-cpus-atom))

(defn below-quota?
  "Returns true if the usage is below quota-constraints on all dimensions"
  [{:keys [count cpus mem] :as quota}
   {:keys [count cpus mem] :as usage}]
  (every? (fn [[usage-key usage-val]]
            (<= usage-val (get quota usage-key 0)))
          (seq usage)))

(defn job->usage
  "Takes a job-ent and returns a map of the usage of that job,
   specifically :cpus, :gpus (when available), :mem, and :count (which is 1)"
  [job-ent]
  (let [{:keys [cpus gpus mem]} (util/job-ent->resources job-ent)]
    (cond-> {:count 1 :cpus cpus :mem mem}
            gpus (assoc :gpus gpus))))

(defn filter-based-on-quota
  "Lazily filters jobs for which the sum of running jobs and jobs earlier in the queue exceeds one of the constraints,
   max-jobs, max-cpus or max-mem"
  [user->quota user->usage queue]
  (letfn [(filter-with-quota [user->usage job]
            (let [user (:job/user job)
                  job-usage (job->usage job)
                  user->usage' (update-in user->usage [user] #(merge-with + job-usage %))]
              (log/debug "Quota check" {:user user
                                        :usage (get user->usage' user)
                                        :quota (user->quota user)})
              [user->usage' (below-quota? (user->quota user) (get user->usage' user))]))]
    (util/filter-sequential filter-with-quota user->usage queue)))

(defn generate-user-usage-map
  "Returns a mapping from user to usage stats"
  [unfiltered-db]
  (->> (util/get-running-task-ents unfiltered-db)
       (map :job/_instance)
       (group-by :job/user)
       (pc/map-vals (fn [jobs]
                      (->> jobs
                           (map job->usage)
                           (reduce (partial merge-with +)))))))

(defn category->pending-jobs->category->considerable-jobs
  "Limit the pending jobs to considerable jobs based on usage and quota.
   Further limit the considerable jobs to a maximum of num-considerable jobs."
  [db category->pending-jobs user->quota user->usage num-considerable]
  (log/debug "There are" (apply + (map count category->pending-jobs)) "pending jobs")
  (log/debug "pending-jobs:" category->pending-jobs)
  (let [filter-considerable-jobs (fn filter-considerable-jobs [jobs]
                                   (->> jobs
                                        (filter-based-on-quota user->quota user->usage)
                                        (filter (fn [job]
                                                  (util/job-allowed-to-start? db job)))
                                        (take num-considerable)))
        category->considerable-jobs (->> category->pending-jobs
                                         (pc/map-vals filter-considerable-jobs))]
    (log/debug "We'll consider scheduling" (map (fn [[k v]] [k (count v)]) category->considerable-jobs)
               "of those pending jobs (limited to " num-considerable " due to backdown)")
    category->considerable-jobs))

(defn matches->category->job-uuids
  "Returns the matched job uuid sets by category."
  [matches]
  (let [category->jobs (group-by util/categorize-job
                                 (->> matches
                                      (mapcat #(-> % :tasks))
                                      (map #(-> % .getRequest :job))))
        category->job-uuids (pc/map-vals #(set (map :job/uuid %)) category->jobs)]
    (log/debug "matched jobs:" (pc/map-vals count category->job-uuids))
    (when (not (empty? matches))
      (let [matched-normal-jobs-resource-requirements (-> category->jobs :normal util/sum-resources-of-jobs)]
        (meters/mark! matched-tasks-cpus (:cpus matched-normal-jobs-resource-requirements))
        (meters/mark! matched-tasks-mem (:mem matched-normal-jobs-resource-requirements))))
    category->job-uuids))

(defn remove-matched-jobs-from-pending-jobs
  "Removes matched jobs from category->pending-jobs."
  [category->pending-jobs category->matched-job-uuids]
  (let [remove-matched-jobs (fn remove-matched-jobs [category]
                              (let [existing-jobs (category->pending-jobs category)
                                    matched-job-uuids (category->matched-job-uuids category)]
                                (remove #(contains? matched-job-uuids (:job/uuid %)) existing-jobs)))]
    (pc/map-from-keys remove-matched-jobs (keys category->pending-jobs))))

(defn- update-match-with-task-metadata-seq
  "Updates the match with an entry for the task metadata for all tasks."
  [{:keys [tasks] :as match} db framework-id executor-config]
  (let [task-metadata-seq (->> tasks
                               ;; sort-by makes task-txns created in matches->task-txns deterministic
                               (sort-by (comp :job/uuid :job #(.getRequest ^TaskAssignmentResult %)) )
                               (map (partial task/TaskAssignmentResult->task-metadata db framework-id executor-config)))]
    (assoc match :task-metadata-seq task-metadata-seq)))

(defn- matches->task-txns
  "Converts matches to a task transactions."
  [matches]
  (for [{:keys [leases task-metadata-seq]} matches
        :let [offers (mapv :offer leases)
              slave-id (-> offers first :slave-id :value)]
        {:keys [executor hostname ports-assigned task-id task-request]} task-metadata-seq
        :let [job-ref [:job/uuid (get-in task-request [:job :job/uuid])]]]
    [[:job/allowed-to-start? job-ref]
     ;; NB we set any job with an instance in a non-terminal
     ;; state to running to prevent scheduling the same job
     ;; twice; see schema definition for state machine
     [:db/add job-ref :job/state :job.state/running]
     {:db/id (d/tempid :db.part/user)
      :job/_instance job-ref
      :instance/executor executor
      :instance/executor-id task-id ;; NB command executor uses the task-id as the executor-id
      :instance/hostname hostname
      :instance/ports ports-assigned
      :instance/preempted? false
      :instance/progress 0
      :instance/slave-id slave-id
      :instance/start-time (now)
      :instance/status :instance.status/unknown
      :instance/task-id task-id}]))

(defn- launch-matched-tasks!
  "Updates the state of matched tasks in the database and then launches them."
  [matches conn db driver fenzo framework-id executor-config]
  (let [matches (map #(update-match-with-task-metadata-seq % db framework-id executor-config) matches)
        task-txns (matches->task-txns matches)]
    ;; Note that this transaction can fail if a job was scheduled
    ;; during a race. If that happens, then other jobs that should
    ;; be scheduled will not be eligible for rescheduling until
    ;; the pending-jobs atom is repopulated
    (timers/time!
      handle-resource-offer!-transact-task-duration
      @(d/transact
         conn
         (reduce into [] task-txns)))
    (log/info "Launching" (count task-txns) "tasks")
    (log/debug "Matched tasks" task-txns)
    ;; This launch-tasks MUST happen after the above transaction in
    ;; order to allow a transaction failure (due to failed preconditions)
    ;; to block the launch
    (let [num-offers-matched (->> matches
                                  (mapcat (comp :id :offer :leases))
                                  (distinct)
                                  (count))]
      (meters/mark! scheduler-offer-matched num-offers-matched)
      (histograms/update! number-offers-matched num-offers-matched))
    (meters/mark! matched-tasks (count task-txns))
    (timers/time!
      handle-resource-offer!-mesos-submit-duration
      (doseq [{:keys [leases task-metadata-seq]} matches
              :let [offers (mapv :offer leases)
                    task-infos (task/compile-mesos-messages offers task-metadata-seq)]]
        (log/debug "Matched task-infos" task-infos)
        (mesos/launch-tasks! driver (mapv :id offers) task-infos)
        (doseq [{:keys [hostname task-request]} task-metadata-seq]
          (locking fenzo
            (.. fenzo
                (getTaskAssigner)
                (call task-request hostname))))))))

(defn handle-resource-offers!
  "Gets a list of offers from mesos. Decides what to do with them all--they should all
   be accepted or rejected at the end of the function."
  [conn driver ^TaskScheduler fenzo framework-id executor-config category->pending-jobs-atom offer-cache user->usage user->quota num-considerable offers-chan offers]
  (log/debug "invoked handle-resource-offers!")
  (let [offer-stash (atom nil)] ;; This is a way to ensure we never lose offers fenzo assigned if an error occurs in the middle of processing
    ;; TODO: It is possible to have an offer expire by mesos because we recycle it a bunch of times.
    ;; TODO: If there is an exception before offers are sent to fenzo (scheduleOnce) then the offers will be lost. This is fine with offer expiration, but not great.
    (timers/time!
      handle-resource-offer!-duration
      (try
        (let [db (db conn)
              category->pending-jobs @category->pending-jobs-atom
              category->considerable-jobs (timers/time!
                                            handle-resource-offer!-considerable-jobs-duration
                                            (category->pending-jobs->category->considerable-jobs
                                              db category->pending-jobs user->quota user->usage num-considerable))
              {:keys [matches failures]} (timers/time!
                                           handle-resource-offer!-match-duration
                                           (match-offer-to-schedule db fenzo (reduce into [] (vals category->considerable-jobs)) offers))
              _ (log/debug "got matches:" matches)
              offers-scheduled (for [{:keys [leases]} matches
                                     lease leases]
                                 (:offer lease))
              {matched-normal-job-uuids :normal :as category->job-uuids} (timers/time!
                                                                           handle-resource-offer!-match-job-uuids-duration
                                                                           (matches->category->job-uuids matches))
              first-normal-considerable-job-resources (-> category->considerable-jobs :normal first util/job-ent->resources)
              matched-normal-considerable-jobs-head? (contains? matched-normal-job-uuids (-> category->considerable-jobs :normal first :job/uuid))]

          (fenzo/record-placement-failures! conn failures)

          (reset! offer-stash offers-scheduled)
          (reset! front-of-job-queue-mem-atom (or (:mem first-normal-considerable-job-resources) 0))
          (reset! front-of-job-queue-cpus-atom (or (:cpus first-normal-considerable-job-resources) 0))

          (cond
            ;; Possible innocuous reasons for no matches: no offers, or no pending jobs.
            ;; Even beyond that, if Fenzo fails to match ANYTHING, "penalizing" it in the form of giving
            ;; it fewer jobs to look at is unlikely to improve the situation.
            ;; "Penalization" should only be employed when Fenzo does successfully match,
            ;; but the matches don't align with Cook's priorities.
            (empty? matches) true
            :else
            (do
              (swap! category->pending-jobs-atom remove-matched-jobs-from-pending-jobs category->job-uuids)
              (log/debug "updated category->pending-jobs:" @category->pending-jobs-atom)
              (launch-matched-tasks! matches conn db driver fenzo framework-id executor-config)
              matched-normal-considerable-jobs-head?)))
        (catch Throwable t
          (meters/mark! handle-resource-offer!-errors)
          (log/error t "Error in match:" (ex-data t))
          (when-let [offers @offer-stash]
            (async/go
              (async/>! offers-chan offers)))
          ; if an error happened, it doesn't mean we need to penalize Fenzo
          true)))))

(defn view-incubating-offers
  [^TaskScheduler fenzo]
  (let [pending-offers (for [^VirtualMachineCurrentState state (locking fenzo (.getVmCurrentStates fenzo))
                             :let [lease (.getCurrAvailableResources state)]
                             :when lease]
                         {:hostname (.hostname lease)
                          :slave-id (.getVMID lease)
                          :resources (.getScalarValues lease)})]
    (log/debug "We have" (count pending-offers) "pending offers")
    pending-offers))

(def fenzo-num-considerable-atom (atom 0))
(gauges/defgauge [cook-mesos scheduler fenzo-num-considerable] (fn [] @fenzo-num-considerable-atom))
(counters/defcounter [cook-mesos scheduler iterations-at-fenzo-floor])
(meters/defmeter [cook-mesos scheduler fenzo-abandon-and-reset-meter])
(counters/defcounter [cook-mesos scheduler offer-chan-depth])

(defn make-offer-handler
  [conn driver-atom fenzo framework-id executor-config pending-jobs-atom offer-cache
   max-considerable scaleback
   floor-iterations-before-warn floor-iterations-before-reset
   trigger-chan]
  (let [chan-length 100
        offers-chan (async/chan (async/buffer chan-length))
        resources-atom (atom (view-incubating-offers fenzo))]
    (reset! fenzo-num-considerable-atom max-considerable)
    (util/chime-at-ch
      trigger-chan
      (fn match-jobs-event []
        (let [num-considerable @fenzo-num-considerable-atom
              next-considerable
              (try
                (let [
                      ;; There are implications to generating the user->usage here:
                      ;;  1. Currently cook has two oddities in state changes.
                      ;;  We plan to correct both of these but are important for the time being.
                      ;;    a. Cook doesn't mark as a job as running when it schedules a job.
                      ;;       While this is technically correct, it confuses some process.
                      ;;       For example, it will mean that the user->usage generated here
                      ;;       may not include jobs that have been scheduled but haven't started.
                      ;;       Since we do the filter for quota first, this is ok because those jobs
                      ;;       show up in the queue. However, it is important to know about
                      ;;    b. Cook doesn't update the job state when cook hears from mesos about the
                      ;;       state of an instance. Cook waits until it hears from datomic about the
                      ;;       instance state change to change the state of the job. This means that it
                      ;;       is possible to have large delays between when a instance changes status
                      ;;       and the job reflects that change
                      ;;  2. Once the above two items are addressed, user->usage should always correctly
                      ;;     reflect *Cook*'s understanding of the state of the world at this point.
                      ;;     When this happens, users should never exceed their quota
                      user->usage-future (future (generate-user-usage-map (d/db conn)))
                      ;; Try to clear the channel
                      offers (->> (util/read-chan offers-chan chan-length)
                                  ((fn decrement-offer-chan-depth [offer-lists]
                                     (counters/dec! offer-chan-depth (count offer-lists))
                                     offer-lists))
                                  (reduce into []))
                      _ (doseq [offer offers
                                :let [slave-id (-> offer :slave-id :value)
                                      attrs (get-offer-attr-map offer)]]
                          ; Cache all used offers (offer-cache is a map of hostnames to most recent offer)
                          (swap! offer-cache (fn [c]
                                               (if (cache/has? c slave-id)
                                                 (cache/hit c slave-id)
                                                 (cache/miss c slave-id attrs)))))
                      _ (log/debug "Passing following offers to handle-resource-offers!" offers)
                      user->quota (quota/create-user->quota-fn (d/db conn))
                      matched-head? (handle-resource-offers! conn @driver-atom fenzo framework-id executor-config pending-jobs-atom offer-cache @user->usage-future user->quota num-considerable offers-chan offers)]
                  (when (seq offers)
                    (reset! resources-atom (view-incubating-offers fenzo)))
                  ;; This check ensures that, although we value Fenzo's optimizations,
                  ;; we also value Cook's sensibility of fairness when deciding which jobs
                  ;; to schedule.  If Fenzo produces a set of matches that doesn't include
                  ;; Cook's highest-priority job, on the next cycle, we give Fenzo it less
                  ;; freedom in the form of fewer jobs to consider.
                  (if matched-head?
                    max-considerable
                    (max 1 (long (* scaleback num-considerable))))) ;; With max=1000 and 1 iter/sec, this will take 88 seconds to reach 1
                (catch Exception e
                  (log/error e "Offer handler encountered exception; continuing")
                  max-considerable))]

          (if (= next-considerable 1)
            (counters/inc! iterations-at-fenzo-floor)
            (counters/clear! iterations-at-fenzo-floor))

          (if (>= (counters/value iterations-at-fenzo-floor) floor-iterations-before-warn)
            (log/warn "Offer handler has been showing Fenzo only 1 job for "
                      (counters/value iterations-at-fenzo-floor) " iterations."))

          (reset! fenzo-num-considerable-atom
                  (if (>= (counters/value iterations-at-fenzo-floor) floor-iterations-before-reset)
                    (do
                      (log/error "FENZO CANNOT MATCH THE MOST IMPORTANT JOB."
                                 "Fenzo has seen only 1 job for " (counters/value iterations-at-fenzo-floor)
                                 "iterations, and still hasn't matched it.  Cook is now giving up and will "
                                 "now give Fenzo " max-considerable " jobs to look at.")
                      (meters/mark! fenzo-abandon-and-reset-meter)
                      max-considerable)
                    next-considerable))))
      {:error-handler (fn [ex] (log/error ex "Error occurred in match"))})
    [offers-chan resources-atom]))

(defn reconcile-jobs
  "Ensure all jobs saw their final state change"
  [conn]
  (let [jobs (map first (q '[:find ?j
                             :in $ [?status ...]
                             :where
                             [?j :job/state ?status]]
                           (db conn) [:job.state/waiting
                                      :job.state/running]))]
    (doseq [js (partition-all 25 jobs)]
      (async/<!! (transact-with-retries conn
                                        (mapv (fn [j]
                                                [:job/update-state j])
                                              js))))))

;; TODO test that this fenzo recovery system actually works
(defn reconcile-tasks
  "Finds all non-completed tasks, and has Mesos let us know if any have changed."
  [db driver framework-id fenzo]
  (let [running-tasks (q '[:find ?task-id ?status ?slave-id
                           :in $ [?status ...]
                           :where
                           [?i :instance/status ?status]
                           [?i :instance/task-id ?task-id]
                           [?i :instance/slave-id ?slave-id]]
                         db
                         [:instance.status/unknown
                          :instance.status/running])
        sched->mesos {:instance.status/unknown :task-staging
                      :instance.status/running :task-running}]
    (when (seq running-tasks)
      (log/info "Preparing to reconcile" (count running-tasks) "tasks")
      ;; TODO: When turning on periodic reconcilation, probably want to move this to startup
      (doseq [[task-id] running-tasks
              :let [task-ent (d/entity db [:instance/task-id task-id])
                    hostname (:instance/hostname task-ent)
                    job (util/job-ent->map (:job/_instance task-ent))
                    task-request (make-task-request db job :task-id task-id)]]
        ;; Need to lock on fenzo when accessing taskAssigner because taskAssigner and
        ;; scheduleOnce can not be called at the same time.
        (locking fenzo
          (.. fenzo
              (getTaskAssigner)
              (call task-request hostname))))
      (doseq [ts (partition-all 50 running-tasks)]
        (log/info "Reconciling" (count ts) "tasks, including task" (first ts))
        (mesos/reconcile-tasks driver (mapv (fn [[task-id status slave-id]]
                                              {:task-id {:value task-id}
                                               :state (sched->mesos status)
                                               :slave-id {:value slave-id}})
                                            ts)))
      (log/info "Finished reconciling all tasks"))))

(timers/deftimer [cook-mesos scheduler reconciler-duration])

;; TODO this should be running and enabled
(defn reconciler
  [conn driver framework-id fenzo & {:keys [interval]
                            :or {interval (* 30 60 1000)}}]
  (log/info "Starting reconciler. Interval millis:" interval)
  (chime-at (periodic/periodic-seq (time/now) (time/millis interval))
            (fn [time]
              (timers/time!
                reconciler-duration
                (reconcile-jobs conn)
                (reconcile-tasks (db conn) driver framework-id fenzo)))))

;; Unfortunately, clj-time.core/millis only accepts ints, not longs.
;; The Period class has a constructor that accepts "long milliseconds",
;; but since that isn't exposed through the clj-time API, we have to call it directly.
(defn- millis->period
  "Create a time period (duration) from a number of milliseconds."
  [millis]
  (org.joda.time.Period. (long millis)))

(defn get-lingering-tasks
  "Return a list of lingering tasks.

   A lingering task is a task that runs longer than timeout-hours."
  [db now max-timeout-hours default-timeout-hours]
  (let [jobs-with-max-runtime
        (q '[:find ?task-id ?start-time ?max-runtime
             :in $ ?default-runtime
             :where
             [(ground [:instance.status/unknown :instance.status/running]) [?status ...]]
             [?i :instance/status ?status]
             [?i :instance/task-id ?task-id]
             [?i :instance/start-time ?start-time]
             [?j :job/instance ?i]
             [(get-else $ ?j :job/max-runtime ?default-runtime) ?max-runtime]]
           db (-> default-timeout-hours time/hours time/in-millis))
        max-allowed-timeout-ms (-> max-timeout-hours time/hours time/in-millis)]
    (for [[task-id start-time max-runtime-ms] jobs-with-max-runtime
          :let [timeout-period (millis->period (min max-runtime-ms max-allowed-timeout-ms))
                timeout-boundary (time/plus (tc/from-date start-time) timeout-period)]
          :when (time/after? now timeout-boundary)]
      task-id)))

(defn kill-lingering-tasks
  [now conn driver config]
  (let [{:keys [max-timeout-hours
                default-timeout-hours
                timeout-hours]} config
        db (d/db conn)
        ;; These defaults are for backwards compatibility
        max-timeout-hours (or max-timeout-hours timeout-hours)
        default-timeout-hours (or default-timeout-hours timeout-hours)
        lingering-tasks (get-lingering-tasks db now max-timeout-hours default-timeout-hours)]
    (when (seq lingering-tasks)
      (log/info "Starting to kill lingering jobs running more than their max-runtime."
                {:default-timeout-hours default-timeout-hours
                 :max-timeout-hours max-timeout-hours}
                "There are in total" (count lingering-tasks) "lingering tasks.")
      (doseq [task-id lingering-tasks]
        (log/info "Killing lingering task" task-id)
        ;; Note that we probably should update db to mark a task failed as well.
        ;; However in the case that we fail to kill a particular task in Mesos,
        ;; we could lose the chances to kill this task again.
        (mesos/kill-task! driver {:value task-id})
        ;; BUG - the following transaction races with the update that is triggered
        ;; when the task is actually killed and sends its exit status code.
        ;; See issue #515 on GitHub.
        @(d/transact
           conn
           [[:instance/update-state [:instance/task-id task-id] :instance.status/failed [:reason/name :max-runtime-exceeded]]
            [:db/add [:instance/task-id task-id] :instance/reason [:reason/name :max-runtime-exceeded]]])))))

(defn lingering-task-killer
  "Periodically kill lingering tasks.

   The config is a map with optional keys where
   :timout-hours specifies the timeout hours for lingering tasks"
  [conn driver config trigger-chan]
  (let [config (merge {:timeout-hours (* 2 24)}
                      config)]
    (util/chime-at-ch trigger-chan
                      (fn kill-linger-task-event []
                        (kill-lingering-tasks (time/now) conn driver config))
                      {:error-handler (fn [e]
                                        (log/error e "Failed to reap timeout tasks!"))})))

(defn handle-stragglers
  "Searches for running jobs in groups and runs the associated straggler handler"
  [conn kill-task-fn]
  (let [running-task-ents (util/get-running-task-ents (d/db conn))
        running-job-ents (map :job/_instance running-task-ents)
        groups (distinct (mapcat :group/_job running-job-ents))]
    (doseq [group groups]
      (log/debug "Checking group " (d/touch group) " for stragglers")

      (let [straggler-task-ents (group/find-stragglers group)]
        (log/debug "Group " group " had stragglers: " straggler-task-ents)

        (doseq [{task-ent-id :db/id :as task-ent} straggler-task-ents]
          (log/info "Killing " task-ent " of group " (:group/uuid group) " because it is a straggler")
          ;; Mark as killed first so that if we fail after this it is still marked failed
          @(d/transact
             conn
             [[:instance/update-state task-ent-id :instance.status/failed [:reason/name :straggler]]
              [:db/add task-ent-id :instance/reason [:reason/name :straggler]]])
          (kill-task-fn task-ent))))))

(defn straggler-handler
  "Periodically checks for running jobs that are in groups and runs the associated
   straggler handler."
  [conn driver trigger-chan]
  (util/chime-at-ch trigger-chan
                    (fn straggler-handler-event []
                      (handle-stragglers conn #(mesos/kill-task! driver {:value (:instance/task-id %)})))
                    {:error-handler (fn [e]
                                      (log/error e "Failed to handle stragglers"))}))

(defn killable-cancelled-tasks
  [db]
  (->> (q '[:find ?i
            :in $ [?status ...]
            :where
            [?i :instance/cancelled true]
            [?i :instance/status ?status]]
          db [:instance.status/running :instance.status/unknown])
       (map (fn [[x]] (d/entity db x)))))

(timers/deftimer [cook-mesos scheduler killing-cancelled-tasks-duration])

(defn cancelled-task-killer
  "Every trigger, kill tasks that have been cancelled (e.g. via the API)."
  [conn driver trigger-chan]
  (util/chime-at-ch
    trigger-chan
    (fn cancelled-task-killer-event []
      (timers/time!
        killing-cancelled-tasks-duration
        (doseq [task (killable-cancelled-tasks (d/db conn))]
          (log/warn "killing cancelled task " (:instance/task-id task))
          @(d/transact conn [[:db/add (:db/id task) :instance/reason
                              [:reason/name :mesos-executor-terminated]]])
          (mesos/kill-task! driver {:value (:instance/task-id task)}))))
    {:error-handler (fn [e]
                      (log/error e "Failed to kill cancelled tasks!"))}))

(defn get-user->used-resources
  "Return a map from user'name to his allocated resources, in the form of
   {:cpus cpu :mem mem}
   If a user does NOT has any running jobs, then there is NO such
   user in this map.

   (get-user-resource-allocation [db user])
   Return a map from user'name to his allocated resources, in the form of
   {:cpus cpu :mem mem}
   If a user does NOT has any running jobs, all the values in the
   resource map is 0.0"
  ([db]
   (let [user->used-resources (->> (q '[:find ?j
                                        :in $
                                        :where
                                        [?j :job/state :job.state/running]]
                                      db)
                                   (map (fn [[eid]]
                                          (d/entity db eid)))
                                   (group-by :job/user)
                                   (map (fn [[user job-ents]]
                                          [user (util/sum-resources-of-jobs job-ents)]))
                                   (into {}))]
     user->used-resources))
  ([db user]
   (let [used-resources (->> (q '[:find ?j
                                  :in $ ?u
                                  :where
                                  [?j :job/state :job.state/running]
                                  [?j :job/user ?u]]
                                db user)
                             (map (fn [[eid]]
                                    (d/entity db eid)))
                             (util/sum-resources-of-jobs))]
     {user (if (seq used-resources)
             used-resources
             ;; Return all 0's for a user who does NOT have any running job.
             (zipmap (util/get-all-resource-types db) (repeat 0.0)))})))

(defn sort-jobs-by-dru-helper
  "Return a list of job entities ordered by the provided sort function"
  [pending-task-ents running-task-ents user->dru-divisors sort-task-scored-task-pairs sort-jobs-duration]
  (let [tasks (into (vec running-task-ents) pending-task-ents)
        task-comparator (util/same-user-task-comparator tasks)
        pending-task-ents-set (into #{} pending-task-ents)
        jobs (timers/time!
               sort-jobs-duration
               (->> tasks
                    (group-by util/task-ent->user)
                    (pc/map-vals (fn [task-ents] (sort task-comparator task-ents)))
                    (sort-task-scored-task-pairs user->dru-divisors)
                    (filter (fn [[task _]] (contains? pending-task-ents-set task)))
                    (map (fn [[task _]] (:job/_instance task)))))]
    jobs))

(timers/deftimer [cook-mesos scheduler sort-jobs-hierarchy-duration])

(defn- sort-normal-jobs-by-dru
  "Return a list of normal job entities ordered by dru"
  [pending-task-ents running-task-ents user->dru-divisors]
  (sort-jobs-by-dru-helper pending-task-ents running-task-ents user->dru-divisors
                           dru/sorted-task-scored-task-pairs sort-jobs-hierarchy-duration))

(timers/deftimer [cook-mesos scheduler sort-gpu-jobs-hierarchy-duration])

(defn- sort-gpu-jobs-by-dru
  "Return a list of gpu job entities ordered by dru"
  [pending-task-ents running-task-ents user->dru-divisors]
  (sort-jobs-by-dru-helper pending-task-ents running-task-ents user->dru-divisors
                           dru/sorted-task-cumulative-gpu-score-pairs sort-gpu-jobs-hierarchy-duration))

(defn sort-jobs-by-dru-category
  "Returns a map from job category to a list of job entities, ordered by dru"
  [unfiltered-db]
  ;; This function does not use the filtered db when it is not necessary in order to get better performance
  ;; The filtered db is not necessary when an entity could only arrive at a given state if it was already committed
  ;; e.g. running jobs or when it is always considered committed e.g. shares
  ;; The unfiltered db can also be used on pending job entities once the filtered db is used to limit
  ;; to only those jobs that have been committed.
  (let [category->pending-job-ents (group-by util/categorize-job (util/get-pending-job-ents unfiltered-db))
        category->pending-task-ents (pc/map-vals #(map util/create-task-ent %1) category->pending-job-ents)
        category->running-task-ents (group-by (comp util/categorize-job :job/_instance)
                                              (util/get-running-task-ents unfiltered-db))
        user->dru-divisors (share/create-user->share-fn unfiltered-db)
        category->sort-jobs-by-dru-fn {:normal sort-normal-jobs-by-dru, :gpu sort-gpu-jobs-by-dru}]
    (letfn [(sort-jobs-by-dru-category-helper [[category sort-jobs-by-dru]]
             (let [pending-tasks (category->pending-task-ents category)
                   running-tasks (category->running-task-ents category)]
               [category (sort-jobs-by-dru pending-tasks running-tasks user->dru-divisors)]))]
      (into {} (map sort-jobs-by-dru-category-helper) category->sort-jobs-by-dru-fn))))

(timers/deftimer [cook-mesos scheduler filter-offensive-jobs-duration])

(defn is-offensive?
  [max-memory-mb max-cpus job]
  (let [{memory-mb :mem
         cpus :cpus} (util/job-ent->resources job)]
    (or (> memory-mb max-memory-mb)
        (> cpus max-cpus))))

(defn filter-offensive-jobs
  "Base on the constraints on memory and cpus, given a list of job entities it
   puts the offensive jobs into offensive-job-ch asynchronically and returns
   the inoffensive jobs.

   A job is offensive if and only if its required memory or cpus exceeds the
   limits"
  ;; TODO these limits should come from the largest observed host from Fenzo
  ;; .getResourceStatus on TaskScheduler will give a map of hosts to resources; we can compute the max over those
  [{max-memory-gb :memory-gb max-cpus :cpus} offensive-jobs-ch jobs]
  (timers/time!
    filter-offensive-jobs-duration
    (let [max-memory-mb (* 1024.0 max-memory-gb)
          is-offensive? (partial is-offensive? max-memory-mb max-cpus)
          inoffensive (remove is-offensive? jobs)
          offensive (filter is-offensive? jobs)]
      ;; Put offensive jobs asynchronically such that it could return the
      ;; inoffensive jobs immediately.
      (async/go
        (when (seq offensive)
          (log/info "Found" (count offensive) "offensive jobs")
          (async/>! offensive-jobs-ch offensive)))
      inoffensive)))

(defn make-offensive-job-stifler
  "It returns an async channel which will be used to receive offensive jobs expected
   to be killed / aborted.

   It asynchronically pulls offensive jobs from the channel and abort these
   offensive jobs by marking job state as completed."
  [conn]
  (let [offensive-jobs-ch (async/chan (async/sliding-buffer 256))]
    (async/thread
      (loop []
        (when-let [offensive-jobs (async/<!! offensive-jobs-ch)]
          (try
            (doseq [jobs (partition-all 32 offensive-jobs)]
              ;; Transact synchronously so that it won't accidentally put a huge
              ;; spike of load on the transactor.
              (async/<!!
                (transact-with-retries conn
                                       (mapv
                                         (fn [job]
                                           [:db/add [:job/uuid (:job/uuid job)]
                                            :job/state :job.state/completed])
                                         jobs))))
            (log/warn "Suppressed offensive" (count offensive-jobs) "jobs" (mapv :job/uuid offensive-jobs))
            (catch Exception e
              (log/error e "Failed to kill the offensive job!")))
          (recur))))
    offensive-jobs-ch))

(timers/deftimer [cook-mesos scheduler rank-jobs-duration])
(meters/defmeter [cook-mesos scheduler rank-jobs-failures])

(defn rank-jobs
  "Return a map of lists of job entities ordered by dru, keyed by category.

   It ranks the jobs by dru first and then apply several filters if provided."
  [unfiltered-db offensive-job-filter]
  (timers/time!
    rank-jobs-duration
    (try
      (let [jobs (->> (sort-jobs-by-dru-category unfiltered-db)
                      ;; Apply the offensive job filter first before taking.
                      (pc/map-vals offensive-job-filter)
                      (pc/map-vals #(map util/job-ent->map %)))]
        (log/debug "Total number of pending jobs is:" (apply + (map count (vals jobs)))
                   "The first 20 pending normal jobs:" (take 20 (:normal jobs))
                   "The first 5 pending gpu jobs:" (take 5 (:gpu jobs)))
        jobs)
      (catch Throwable t
        (log/error t "Failed to rank jobs")
        (meters/mark! rank-jobs-failures)
        {}))))

(defn- start-jobs-prioritizer!
  [conn pending-jobs-atom task-constraints trigger-chan]
  (let [offensive-jobs-ch (make-offensive-job-stifler conn)
        offensive-job-filter (partial filter-offensive-jobs task-constraints offensive-jobs-ch)]
    (util/chime-at-ch trigger-chan
                      (fn rank-jobs-event []
                        (reset! pending-jobs-atom
                                (rank-jobs (d/db conn) offensive-job-filter))))))

(meters/defmeter [cook-mesos scheduler mesos-error])
(meters/defmeter [cook-mesos scheduler offer-chan-full-error])

(defn config-string->fitness-calculator
  "Given a string specified in the configuration, attempt to resolve it
  to and return an instance of com.netflix.fenzo.VMTaskFitnessCalculator.
  The config string can either be a reference to a clojure symbol, or to a
  static member of a java class (for example, one of the fitness calculators
  that ship with Fenzo).  An exception will be thrown if a VMTaskFitnessCalculator
  can't be found using either method."
  ^VMTaskFitnessCalculator
  [config-string]
  (let [calculator
        (try
          (-> config-string symbol resolve deref)
          (catch NullPointerException e
            (log/debug "fitness-calculator" config-string
                       "couldn't be resolved to a clojure symbol."
                       "Seeing if it refers to a java static field...")
            (try
              (let [[java-class-name field-name] (str/split config-string #"/")
                    java-class (-> java-class-name symbol resolve)]
                (clojure.lang.Reflector/getStaticField java-class field-name))
              (catch Exception e
                (throw (IllegalArgumentException.
                         (str config-string " could not be resolved to a clojure symbol or to a java static field")))))))]
    (if (instance? VMTaskFitnessCalculator calculator)
      calculator
      (throw (IllegalArgumentException.
               (str config-string " is not a VMTaskFitnessCalculator"))))))

(defn make-fenzo-scheduler
  [driver offer-incubate-time-ms fitness-calculator good-enough-fitness]
  (.. (TaskScheduler$Builder.)
      (disableShortfallEvaluation) ;; We're not using the autoscaling features
      (withLeaseOfferExpirySecs (max (-> offer-incubate-time-ms time/millis time/in-seconds) 1)) ;; should be at least 1 second
      (withRejectAllExpiredOffers)
      (withFitnessCalculator (config-string->fitness-calculator (or fitness-calculator default-fitness-calculator)))
      (withFitnessGoodEnoughFunction (reify Func1
                                       (call [_ fitness]
                                         (> fitness good-enough-fitness))))
      (withLeaseRejectAction (reify Action1
                               (call [_ lease]
                                 (let [offer (:offer lease)
                                       id (:id offer)]
                                   (log/debug "Fenzo is declining offer" offer)
                                   (if-let [driver @driver]
                                     (try
                                       (decline-offers driver [id])
                                       (catch Exception e
                                         (log/error e "Unable to decline fenzos rejected offers")))
                                     (log/error "Unable to decline offer; no current driver"))))))
      (build)))

(defn persist-mea-culpa-failure-limit!
  "The Datomic transactor needs to be able to access this part of the
  configuration, so on cook startup we transact the configured value into Datomic."
  [conn limits]
  (when (map? limits)
    (let [default (:default limits)
          overrides (mapv (fn [[reason limit]] {:db/id [:reason/name reason]
                                                :reason/failure-limit limit})
                          (dissoc limits :default))]
      (when default
        @(d/transact conn [{:db/id :scheduler/config
                            :scheduler.config/mea-culpa-failure-limit default}]))
      (when (seq overrides)
        @(d/transact conn overrides))))
  (when (number? limits)
    @(d/transact conn [{:db/id :scheduler/config
                        :scheduler.config/mea-culpa-failure-limit limits}])))

(defn receive-offers
  [offers-chan match-trigger-chan driver offers]
  (log/info "Got offers, putting them into the offer channel:" offers)
  (doseq [offer offers]
    (histograms/update! offer-size-cpus (get-in offer [:resources :cpus] 0))
    (histograms/update! offer-size-mem (get-in offer [:resources :mem] 0)))
  (if (async/offer! offers-chan offers)
    (do
      (counters/inc! offer-chan-depth)
      (async/offer! match-trigger-chan :trigger)) ; :trigger is arbitrary, the value is ignored
    (do (log/warn "Offer chan is full. Are we not handling offers fast enough?")
        (meters/mark! offer-chan-full-error)
        (future
          (try
            (decline-offers driver (map :id offers))
            (catch Exception e
              (log/error e "Unable to decline offers!")))))))

(let [in-order-queue-counter (counters/counter ["cook-mesos" "scheduler" "in-order-queue-size"])
      in-order-queue-timer (timers/timer ["cook-mesos" "scheduler" "in-order-queue-delay-duration"])
      parallelism 19 ; a prime number to potentially help make the distribution uniform
      processor-agents (->> #(agent nil)
                            (repeatedly parallelism)
                            vec)
      safe-call (fn agent-processor [_ body-fn]
                  (try
                    (body-fn)
                    (catch Exception e
                      (log/error e "Error processing mesos status/message."))))]
  (defn async-in-order-processing
    "Asynchronously processes the body-fn by queueing the task in an agent to ensure in-order processing."
    [order-id body-fn]
    (counters/inc! in-order-queue-counter)
    (let [timer-context (timers/start in-order-queue-timer)
          processor-agent (->> (mod (hash order-id) parallelism)
                               (nth processor-agents))]
      (send processor-agent safe-call
            #(do
               (timers/stop timer-context)
               (counters/dec! in-order-queue-counter)
               (body-fn))))))

(defn create-mesos-scheduler
  "Creates the mesos scheduler which processes status updates asynchronously but in order of receipt."
  [configured-framework-id gpu-enabled? conn heartbeat-ch fenzo offers-chan match-trigger-chan handle-progress-message
   sandbox-syncer-state]
  (let [sync-agent-sandboxes-fn #(sandbox/sync-agent-sandboxes sandbox-syncer-state configured-framework-id %)]
    (mesos/scheduler
      (registered [this driver framework-id master-info]
                  (log/info "Registered with mesos with framework-id " framework-id)
                  (let [value (-> framework-id mesomatic.types/pb->data :value)]
                    (when (not= configured-framework-id value)
                      (let [message (str "The framework-id provided by Mesos (" value ") "
                                         "does not match the one Cook is configured with (" configured-framework-id ")")]
                        (log/error message)
                        (throw (ex-info message {:framework-id-mesos value :framework-id-cook configured-framework-id})))))
                  (when (and gpu-enabled? (not (re-matches #"1\.\d+\.\d+" (:version master-info))))
                    (binding [*out* *err*]
                      (println "Cannot enable GPU support on pre-mesos 1.0. The version we found was " (:version master-info)))
                    (log/error "Cannot enable GPU support on pre-mesos 1.0. The version we found was " (:version master-info))
                    (Thread/sleep 1000)
                    (System/exit 1))
                  ;; Use future because the thread that runs mesos/scheduler doesn't load classes correctly. for reasons.
                  ;; As Sophie says, you want to future proof your code.
                  (future
                    (try
                      (reconcile-jobs conn)
                      (reconcile-tasks (db conn) driver configured-framework-id fenzo)
                      (catch Exception e
                        (log/error e "Reconciliation error")))))
      (reregistered [this driver master-info]
                    (log/info "Reregistered with new master")
                    (future
                      (try
                        (reconcile-jobs conn)
                        (reconcile-tasks (db conn) driver configured-framework-id fenzo)
                        (catch Exception e
                          (log/error e "Reconciliation error")))))
      ;; Ignore this--we can just wait for new offers
      (offer-rescinded [this driver offer-id]
                       ;; TODO: Rescind the offer in fenzo
                       )
      (framework-message [this driver executor-id slave-id message]
                         (try
                           (let [{:strs [task-id type] :as parsed-message} (json/read-str (String. ^bytes message "UTF-8"))]
                             (case type
                               "directory" (sandbox/update-sandbox sandbox-syncer-state parsed-message)
                               "heartbeat" (heartbeat/notify-heartbeat heartbeat-ch executor-id slave-id parsed-message)
                               (async-in-order-processing
                                 task-id #(handle-framework-message conn handle-progress-message parsed-message))))
                           (catch Exception e
                             (log/error e "Unable to process framework message"
                                        {:executor-id executor-id, :message message, :slave-id slave-id}))))
      (disconnected [this driver]
                    (log/error "Disconnected from the previous master"))
      ;; We don't care about losing slaves or executors--only tasks
      (slave-lost [this driver slave-id])
      (executor-lost [this driver executor-id slave-id status])
      (error [this driver message]
             (meters/mark! mesos-error)
             (log/error "Got a mesos error!!!!" message))
      (resource-offers [this driver offers]
                       (receive-offers offers-chan match-trigger-chan driver offers))
      (status-update [this driver status]
                     (let [task-id (-> status :task-id :value)]
                       (async-in-order-processing
                         task-id #(handle-status-update conn driver fenzo sync-agent-sandboxes-fn status)))))))

(defn create-datomic-scheduler
  [conn driver-atom pending-jobs-atom offer-cache heartbeat-ch offer-incubate-time-ms mea-culpa-failure-limit
   fenzo-max-jobs-considered fenzo-scaleback fenzo-floor-iterations-before-warn fenzo-floor-iterations-before-reset
   fenzo-fitness-calculator task-constraints gpu-enabled? good-enough-fitness framework-id sandbox-syncer-state
   executor-config progress-config trigger-chans]

  (persist-mea-culpa-failure-limit! conn mea-culpa-failure-limit)

  (let [{:keys [match-trigger-chan progress-updater-trigger-chan rank-trigger-chan]} trigger-chans
        fenzo (make-fenzo-scheduler driver-atom offer-incubate-time-ms fenzo-fitness-calculator good-enough-fitness)
        [offers-chan resources-atom]
        (make-offer-handler conn driver-atom fenzo framework-id executor-config pending-jobs-atom offer-cache fenzo-max-jobs-considered
                            fenzo-scaleback fenzo-floor-iterations-before-warn fenzo-floor-iterations-before-reset match-trigger-chan)
        {:keys [batch-size]} progress-config
        {:keys [progress-state-chan]} (progress-update-transactor progress-updater-trigger-chan batch-size conn)
        progress-aggregator-chan (progress-update-aggregator progress-config progress-state-chan)
        handle-progress-message (fn handle-progress-message-curried [progress-message-map]
                                  (handle-progress-message! progress-aggregator-chan progress-message-map))]
    (start-jobs-prioritizer! conn pending-jobs-atom task-constraints rank-trigger-chan)
    {:scheduler (create-mesos-scheduler framework-id gpu-enabled? conn heartbeat-ch fenzo offers-chan
                                        match-trigger-chan handle-progress-message sandbox-syncer-state)
     :view-incubating-offers (fn get-resources-atom [] @resources-atom)}))
