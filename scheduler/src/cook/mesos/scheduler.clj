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
  (:require [mesomatic.scheduler :as mesos]
            [mesomatic.types :as mtypes]
            [cook.mesos.dru :as dru]
            [cook.mesos.task :as task]
            [cook.mesos.group :as group]
            cook.mesos.schema
            [clojure.tools.logging :as log]
            [datomic.api :as d :refer (q)]
            [metatransaction.core :refer (db)]
            [cook.datomic :as datomic :refer (transact-with-retries)]
            [cook.reporter]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [metrics.histograms :as histograms]
            [metrics.counters :as counters]
            [metrics.gauges :as gauges]
            [clojure.core.async :as async]
            [clojure.edn :as edn]
            [clj-time.core :as time]
            [clj-time.periodic :as periodic]
            [clj-time.coerce :as tc]
            [chime :refer [chime-at chime-ch]]
            [cook.mesos.heartbeat :as heartbeat]
            [cook.mesos.share :as share]
            [cook.mesos.quota :as quota]
            [cook.mesos.util :as util]
            [plumbing.core :refer (map-vals)]
            [swiss.arrows :refer :all]
            [clojure.core.cache :as cache]
            [cook.mesos.reason :as reason])
  (import java.util.concurrent.TimeUnit
          org.apache.mesos.Protos$Offer
          com.netflix.fenzo.TaskAssignmentResult
          com.netflix.fenzo.TaskScheduler
          com.netflix.fenzo.VirtualMachineLease
          com.netflix.fenzo.plugins.BinPackingFitnessCalculators))

(defn now
  []
  (java.util.Date.))


(defn offer-resource-values
  [offer resource-name value-type]
  (->> offer :resources (filter #(= (:name %) resource-name)) (map value-type)))

(defn offer-resource-scalar
  [offer resource-name]
  (reduce + 0.0 (offer-resource-values offer resource-name :scalar)))

(defn offer-resource-ranges
  [offer resource-name]
  (reduce into [] (offer-resource-values offer resource-name :ranges)))

(defn tuplify-offer
  "Takes an offer (from Mesomatic) and converts it to a queryable format for datomic"
  [offer]
  [(-> offer :slave-id :value)
   (offer-resource-scalar offer "cpus")
   (offer-resource-scalar offer "mem")])

(defn get-job-resource-matches
  "Given an offer and a set of pending jobs, figure out which ones match the offer"
  [db pending-jobs offer]
  (q '[:find ?j ?slave
       :in $ [[?j]] [?slave ?cpus ?mem]
       :where
       [?j :job/resource ?r-cpu]
       [?r-cpu :resource/type :resource.type/cpus]
       [?r-cpu :resource/amount ?cpu-req]
       [(>= ?cpus ?cpu-req)]
       [?j :job/resource ?r-mem]
       [?r-mem :resource/type :resource.type/mem]
       [?r-mem :resource/amount ?mem-req]
       [(>= ?mem ?mem-req)]]
     db pending-jobs (tuplify-offer offer)))

(timers/deftimer [cook-mesos scheduler handle-status-update-duration])
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

(meters/defmeter [cook-mesos scheduler backfilled-count])
(histograms/defhistogram [cook-mesos scheduler hist-backfilled-count])
(meters/defmeter [cook-mesos scheduler backfilled-cpu])
(meters/defmeter [cook-mesos scheduler backfilled-mem])

(meters/defmeter [cook-mesos scheduler upgraded-count])
(histograms/defhistogram [cook-mesos scheduler hist-upgraded-count])
(meters/defmeter [cook-mesos scheduler upgraded-cpu])
(meters/defmeter [cook-mesos scheduler upgraded-mem])

(meters/defmeter [cook-mesos scheduler fully-processed-count])
(histograms/defhistogram [cook-mesos scheduler hist-fully-processed-count])
(meters/defmeter [cook-mesos scheduler fully-processed-cpu])
(meters/defmeter [cook-mesos scheduler fully-processed-mem])


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
                 (log/debug e "Error parse mesos status data. Is it in the format we expect?")))})

(defn handle-status-update
  "Takes a status update from mesos."
  [conn driver ^TaskScheduler fenzo status]
  (log/info "Mesos status is: " status)
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
               retries-so-far (count (:job/instance job-ent))
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
               instance-ent (d/entity db instance)
               instance-runtime (- (.getTime (now)) ; Used for reporting
                                   (.getTime (or (:instance/start-time instance-ent) (now))))
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
             ;; (println "update:" task-id task-state job instance instance-status prior-job-state)
             (log/debug "Transacting updated state for instance" instance "to status" instance-status)
             ;; The database can become inconsistent if we make multiple calls to :instance/update-state in a single
             ;; transaction; see the comment in the definition of :instance/update-state for more details
             (transact-with-retries conn
               (reduce into
                 [[:instance/update-state instance instance-status (or (:db/id previous-reason)
                                                                       (reason/mesos-reason->cook-reason-entity-id db reason)
                                                                       [:reason.name :unknown])]] ; Warning: Default is not mea-culpa
                 [(when (and (#{:instance.status/failed} instance-status) (not previous-reason) reason)
                    [[:db/add instance :instance/reason (reason/mesos-reason->cook-reason-entity-id db reason)]])
                  (when (#{:instance.status/success
                           :instance.status/failed} instance-status)
                    [[:db/add instance :instance/end-time (now)]])
                  (when progress
                    [[:db/add instance :instance/progress progress]])]))))
      (catch Exception e
        (log/error e "Mesos scheduler status update error")))))

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
  com.netflix.fenzo.VirtualMachineLease
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
  (getAttributeMap [_] {}) ;;TODO
  (getId [_] (-> offer :id :value))
  (getOffer [_] (throw (UnsupportedOperationException.)))
  (getOfferedTime [_] time)
  (getVMID [_] (-> offer :slave-id :value))
  (hostname [_] (:hostname offer))
  (memoryMB [_] (or (offer-resource-scalar offer "mem") 0.0))
  (networkMbps [_] 0.0)
  (portRanges [_] (mapv (fn [{:keys [begin end]}]
                          (com.netflix.fenzo.VirtualMachineLease$Range. begin end))
                        (offer-resource-ranges offer "ports"))))

(defn novel-host-constraint
  "This returns a Fenzo hard constraint that ensures the given job won't run on the same host again"
  [job]
  (reify com.netflix.fenzo.ConstraintEvaluator
    (getName [_] "novel_host_constraint")
    (evaluate [_ task-request target-vm task-tracker-state]
      (let [previous-hosts (->> (:job/instance job)
                                (remove #(true? (:instance/preempted? %)))
                                (mapv :instance/hostname))]
        (com.netflix.fenzo.ConstraintEvaluator$Result.
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
    (reify com.netflix.fenzo.ConstraintEvaluator
      (getName [_] (str (if @needs-gpus? "" "non_") "gpu_host_constraint"))
      (evaluate [_ task-request target-vm task-tracker-state]
        (let [has-gpus? (boolean (or (> (or (.getScalarValue (.getCurrAvailableResources target-vm) "gpus") 0.0) 0)
                                     (some (fn gpu-task? [req]
                                             @(job-needs-gpus? (:job req)))
                                           (concat (.getRunningTasks target-vm)
                                                   (mapv #(.getRequest %) (.getTasksCurrentlyAssigned target-vm))))))]
          (com.netflix.fenzo.ConstraintEvaluator$Result.
            (if @needs-gpus?
              has-gpus?
              (not has-gpus?))
            (str "The machine " (.getHostname target-vm) (if @needs-gpus? " doesn't have" " has") " gpus")))))))

(defrecord TaskRequestAdapter [job resources task-id assigned-resources]
  com.netflix.fenzo.TaskRequest
  (getCPUs [_] (:cpus resources))
  (getDisk [_] 0.0)
  (getHardConstraints [_] [(novel-host-constraint job) (gpu-host-constraint job)])
  (getId [_] task-id)
  (getScalarRequests [_]
    (reduce (fn [result resource]
                 (if-let [value (:resource/amount resource)]
                   (assoc result (name (:resource/type resource)) value)
                   result))
               {}
               (:job/resource job)))
  (getAssignedResources [_] @assigned-resources)
  (setAssignedResources [_ v] (reset! assigned-resources v))
  (getCustomNamedResources [_] {})
  (getMemory [_] (:mem resources))
  (getNetworkMbps [_] 0.0)
  (getPorts [_] (:ports resources))
  (getSoftConstraints [_] [])
  (taskGroupName [_] (str (:job/uuid job))))

(defn match-offer-to-schedule
  "Given an offer and a schedule, computes all the tasks should be launched as a result.

   A schedule is just a sorted list of tasks, and we're going to greedily assign them to
   the offer.

   Returns a list of tasks that got matched to the offer"
  [^TaskScheduler fenzo considerable offers]
  (log/debug "Matching" (count offers) "offers to" (count considerable) "jobs with fenzo")
  (log/debug "offer to scheduleOnce" offers)
  (log/debug "tasks to scheduleOnce" considerable)
  (let [t (System/currentTimeMillis)
        _ (log/debug "offer to scheduleOnce" offers)
        _ (log/debug "tasks to scheduleOnce" considerable)
        leases (mapv #(->VirtualMachineLeaseAdapter % t) offers)
        requests (mapv (fn [job]
                         (->TaskRequestAdapter job
                                               (util/job-ent->resources job)
                                               (str (java.util.UUID/randomUUID))
                                               (atom nil)))
                       considerable)
        ;; Need to lock on fenzo when accessing scheduleOnce because scheduleOnce and
        ;; task assigner can not be called at the same time.
        ;; task assigner may be called when reconciling
        result (locking fenzo
                 (.scheduleOnce fenzo requests leases))
        failure-results (.. result getFailures values)
        assignments (.. result getResultMap values)]
    (log/debug "Found this assigment:" result)
    (when (and (seq failure-results) (log/enabled? :debug))
      (log/debug "Task placement failure information follows:")
      (doseq [failure-result failure-results
              failure failure-result
              :let [_ (log/debug (str (.getConstraintFailure failure)))]
              f (.getFailures failure)]
        (log/debug (str f)))
      (log/debug "Task placement failure information concluded."))
    (mapv (fn [assignment]
            {:leases (.getLeasesUsed assignment)
             :tasks (.getTasksAssigned assignment)
             :hostname (.getHostname assignment)})
          assignments)))

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

(defn ids-of-backfilled-instances
  "Returns a list of the ids of running-or-unknown, backfilled instances of the given job"
  [job]
  (->> (:job/instance job)
       (filter #(and (contains? #{:instance.status/running :instance.status/unknown}
                                (:instance/status %))
                     (:instance/backfilled? %)))
       (map :db/id)))

(defn process-matches-for-backfill
  "This computes some sets:

   fully-processed: this is a set of job uuids that should be removed from the list scheduler jobs. These are not backfilled
   upgrade-backfill: this is a set of instance datomic ids that should be upgraded to non-backfill
   backfill-jobs: this is a set of job uuids for jobs that were matched, but are in the backfill QoS class
   "
  [scheduler-contents head-of-considerable matched-jobs]
  (let [matched-job-uuids (set (mapv :job/uuid matched-jobs))
        ;; ids-of-backfilled-instances hits datomic; without memoization this would
        ;; happen multiple times for the same job:
        backfilled-ids-memo (memoize ids-of-backfilled-instances)
        matched? (fn [job]
                   (contains? matched-job-uuids (:job/uuid job)))
        previously-backfilled? (fn [job]
                                 (seq (backfilled-ids-memo job)))
        index-if-never-matched (fn [index job]
                                 (if (not (or
                                           (matched? job)
                                           (previously-backfilled? job)))
                                   index))
        last-index-before-backfill (->> scheduler-contents
                                        (keep-indexed index-if-never-matched)
                                        first)
        num-before-backfilling (if last-index-before-backfill
                                 (inc last-index-before-backfill)
                                 (count scheduler-contents))
        jobs-before-backfilling (take num-before-backfilling scheduler-contents)
        jobs-after-backfilling (drop num-before-backfilling scheduler-contents)
        jobs-to-backfill (filter matched? jobs-after-backfilling)
        jobs-fully-processed (filter matched? jobs-before-backfilling)
        jobs-to-upgrade (filter previously-backfilled? jobs-before-backfilling)
        tasks-ids-to-upgrade (->> jobs-to-upgrade (mapv backfilled-ids-memo) (reduce into []))]

    (let [resources-backfilled (util/sum-resources-of-jobs jobs-to-backfill)
          resources-fully-processed (util/sum-resources-of-jobs jobs-fully-processed)
          resources-upgraded (util/sum-resources-of-jobs jobs-to-upgrade)]
      (meters/mark! backfilled-count (count jobs-to-backfill))
      (histograms/update! hist-backfilled-count (count jobs-to-backfill))
      (meters/mark! backfilled-cpu (:cpus resources-backfilled))
      (meters/mark! backfilled-mem (:mem resources-backfilled))

      (meters/mark! fully-processed-count (count jobs-fully-processed))
      (histograms/update! hist-fully-processed-count (count jobs-fully-processed))
      (meters/mark! fully-processed-cpu (:cpus resources-fully-processed))
      (meters/mark! fully-processed-mem (:mem resources-fully-processed))

      (meters/mark! upgraded-count (count jobs-to-upgrade))
      (histograms/update! hist-upgraded-count (count jobs-to-upgrade))
      (meters/mark! upgraded-cpu (:cpus resources-upgraded))
      (meters/mark! upgraded-mem (:mem resources-upgraded)))

    {:fully-processed (set (mapv :job/uuid jobs-fully-processed))
     :upgrade-backfill (set tasks-ids-to-upgrade)
     :backfill-jobs (set (mapv :job/uuid jobs-to-backfill))
     :matched-head? (matched? head-of-considerable)}))

(defn below-quota?
  "Returns true if the usage is below quota-constraints on all dimensions"
  [{:keys [count cpus mem] :as quota}
   {:keys [count cpus mem] :as usage}]
  ;; The select-keys on quota was added because if there is a
  ;; resource in quota that the current usage doesn't use below-quota?
  ;; will incorrectly return false
  (every? true? (vals (merge-with <= usage
                                  (select-keys quota (keys usage))))))

(defn job->usage
  "Takes a job-ent and returns a map of the usage of that job,
   specifically :mem, :cpus, and :count (which is 1)"
  [job-ent]
  (let [{:keys [cpus mem]} (util/job-ent->resources job-ent)]
    {:cpus cpus :mem mem :count 1}))

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
       (map-vals (fn [jobs]
                   (->> jobs
                        (map job->usage)
                        (reduce (partial merge-with +)))))))

(defn handle-resource-offers!
  "Gets a list of offers from mesos. Decides what to do with them all--they should all
   be accepted or rejected at the end of the function."
  [conn driver ^TaskScheduler fenzo fid pending-jobs user->usage user->quota num-considerable offers-chan offers]
  (log/debug "invoked handle-resource-offers!")
  (let [offer-stash (atom nil)] ;; This is a way to ensure we never lose offers fenzo assigned if an errors occures in the middle of processing
    ;; TODO: It is possible to have an offer expire by mesos because we recycle it a bunch of times.
    ;; TODO: If there is an exception before offers are sent to fenzo (scheduleOnce) then the offers will be lost. This is fine with offer expiration, but not great.
    (timers/time!
      handle-resource-offer!-duration
      (try
        (let [scheduler-contents-by @pending-jobs
              db (db conn)
              _ (log/debug "There are" (apply + (map count scheduler-contents-by)) "pending jobs")
              _ (log/debug "scheduler-contents:" scheduler-contents-by)
              considerable-by (->> scheduler-contents-by
                                   (map (fn [[category jobs]]
                                          [category (->> jobs
                                                         ;; Refresh cached job entity data to prevent cases where stale
                                                         ;; data lingers for too long.  This is problematic in the case
                                                         ;; of backfilled jobs because they remain in the queue after
                                                         ;; they are scheduled in the hopes that the job will be
                                                         ;; upgraded. However, this means it is possible the instance
                                                         ;; will fail and it will be considered again.
                                                         (map #(d/entity db (:db/id %)))
                                                         (filter (fn [job]
                                                                   ;; Remove backfill jobs
                                                                   (= (:job/state job)
                                                                      :job.state/waiting)))
                                                         (filter-based-on-quota user->quota user->usage)
                                                         (filter (fn [job]
                                                                   (util/job-allowed-to-start? db job)))
                                                         (take num-considerable))]))
                                   (into {}))
              _ (log/debug "We'll consider scheduling" (map (fn [[k v]] [k (count v)]) considerable-by)
                           "of those pending jobs (limited to " num-considerable " due to backdown)")
              matches (timers/time!
                        handle-resource-offer!-match-duration
                        (match-offer-to-schedule fenzo (apply concat (vals considerable-by)) offers))
              offers-scheduled (for [{:keys [leases]} matches
                                     lease leases]
                                 (:offer lease))
              offers-not-scheduled (clojure.set/intersection (set offers) (set offers-scheduled))
              _ (reset! offer-stash offers-scheduled)
              matched-normal-jobs (for [match matches
                                        ^TaskAssignmentResult task-result (:tasks match)
                                        :let [task-request (.getRequest task-result)]
                                        :when (= :normal (util/categorize-job (:job task-request)))]
                                    (:job task-request))
              matched-gpu-job-uuids (set (for [match matches
                                               ^TaskAssignmentResult task-result (:tasks match)
                                               :let [task-request (.getRequest task-result)]
                                               :when (= :gpu (util/categorize-job (:job task-request)))]
                                           (:job/uuid (:job task-request))))
              ;; Backfill only applies to normal (i.e. non-scarce resource dependent) jobs
              processed-matches (timers/time!
                                  handle-resource-offer!-process-matches-duration
                                  ;; We take 10x num-considerable as a heuristic
                                  ;; scheduler-contents can be very large and so we don't want to go through the whole thing
                                  ;; it is better to be mostly right in back fill decisions and schedule fast
                                  (process-matches-for-backfill (take (* 10 num-considerable) (:normal scheduler-contents-by))
                                                                (first (:normal considerable-by))
                                                                matched-normal-jobs))
              update-scheduler-contents (fn update-scheduler-contents [scheduler-contents-by]
                                          (-> scheduler-contents-by
                                              (update-in [:gpu] #(remove (fn [{pending-job-uuid :job/uuid}]
                                                                           (contains? matched-gpu-job-uuids pending-job-uuid))
                                                                         %))
                                              (update-in [:normal] #(remove (fn [{pending-job-uuid :job/uuid}]
                                                                              (or (contains? (:fully-processed processed-matches) pending-job-uuid)
                                                                                  (contains? (:upgrade-backfill processed-matches) pending-job-uuid)))
                                                                            %))))
              ;; We don't remove backfilled jobs here, because although backfilled
              ;; jobs have already been scheduled in a sense, the scheduler still can't
              ;; adjust the status of backfilled tasks.
              ;; Backfilled tasks can be updgraded to non-backfilled after the jobs
              ;; prioritized above them are also scheduled.
              first-considerable-resources (-> considerable-by :normal first util/job-ent->resources)
              match-resource-requirements (util/sum-resources-of-jobs matched-normal-jobs)]
          (log/debug "got matches:" matches)
          (log/debug "matched normal jobs:" (count matched-normal-jobs))
          (log/debug "matched gpu jobs:" (count matched-gpu-job-uuids))
          (log/debug "updated-scheduler-contents:" (update-scheduler-contents @pending-jobs))
          (reset! front-of-job-queue-mem-atom
                  (or (:mem first-considerable-resources) 0))
          (reset! front-of-job-queue-cpus-atom
                  (or (:cpus first-considerable-resources) 0))
          (cond
            ;; Possible inocuous reasons for no matches: no offers, or no pending jobs.
            ;; Even beyond that, if Fenzo fails to match ANYTHING, "penalizing" it in the form of giving
            ;; it fewer jobs to look at is unlikely to improve the situation.
            ;; "Penalization" should only be employed when Fenzo does successfully match,
            ;; but the matches don't align with Cook's priorities.
            (empty? matches) true
            :else
            (let [_ (swap! pending-jobs update-scheduler-contents)
                  task-txns (for [{:keys [tasks leases]} matches
                                  :let [offers (mapv :offer leases)
                                        slave-id (-> offers first :slave-id :value)]
                                  ^TaskAssignmentResult task tasks
                                  :let [request (.getRequest task)
                                        task-id (:task-id request)
                                        job-id (get-in request [:job :db/id])]]
                              [[:job/allowed-to-start? job-id]
                               ;; NB we set any job with an instance in a non-terminal
                               ;; state to running to prevent scheduling the same job
                               ;; twice; see schema definition for state machine
                               [:db/add job-id :job/state :job.state/running]
                               {:db/id (d/tempid :db.part/user)
                                :job/_instance job-id
                                :instance/task-id task-id
                                :instance/hostname (.getHostname task)
                                :instance/start-time (now)
                                ;; NB command executor uses the task-id
                                ;; as the executor-id
                                :instance/executor-id task-id
                                :instance/backfilled? (contains? (:backfill-jobs processed-matches) (get-in request [:job :job/uuid]))
                                :instance/slave-id slave-id
                                :instance/ports (.getAssignedPorts task)
                                :instance/progress 0
                                :instance/status :instance.status/unknown
                                :instance/preempted? false}])
                  upgrade-txns (mapv (fn [instance-id]
                                      [:db/add instance-id :instance/backfilled? false])
                                    (:upgrade-backfill processed-matches))]
              ;; Note that this transaction can fail if a job was scheduled
              ;; during a race. If that happens, then other jobs that should
              ;; be scheduled will not be eligible for rescheduling until
              ;; the pending-jobs atom is repopulated
              (timers/time!
                handle-resource-offer!-transact-task-duration
                @(d/transact
                   conn
                   (reduce into upgrade-txns task-txns)))
              (log/info "Launching" (count task-txns) "tasks")
              (log/info "Upgrading" (count (:upgrade-backfill processed-matches)) "tasks from backfilled to proper")
              (log/info "Matched tasks" task-txns)
              ;; This launch-tasks MUST happen after the above transaction in
              ;; order to allow a transaction failure (due to failed preconditions)
              ;; to block the launch
              (meters/mark! scheduler-offer-matched
                            (->> matches
                                 (mapcat (comp :id :offer :leases))
                                 (distinct)
                                 (count)))
              (histograms/update! number-offers-matched
                                  (->> matches
                                       (mapcat (comp :id :offer :leases))
                                       (distinct)
                                       (count)))
              (meters/mark! matched-tasks (count task-txns))
              (meters/mark! matched-tasks-cpus (:cpus match-resource-requirements))
              (meters/mark! matched-tasks-mem (:mem match-resource-requirements))
              (timers/time!
                handle-resource-offer!-mesos-submit-duration
                (doseq [{:keys [tasks leases]} matches
                      :let [offers (mapv :offer leases)
                            task-data-maps (map #(task/TaskAssignmentResult->task-metadata db fid %)
                                                    tasks)
                            task-infos (task/compile-mesos-messages offers task-data-maps)]]

                (log/debug "Matched task-infos" task-infos)
                (mesos/launch-tasks! driver (mapv :id offers) task-infos)

                (doseq [^TaskAssignmentResult task tasks]
                  (locking fenzo
                    (.. fenzo
                        (getTaskAssigner)
                        (call (.getRequest task) (get-in (first leases) [:offer :hostname])))))))
              (:matched-head? processed-matches))))
        (catch Throwable t
          (meters/mark! handle-resource-offer!-errors)
          (log/error t "Error in match:" (ex-data t))
          (when-let [offers @offer-stash]
            (async/go
              (async/>! offers-chan offers)))
          true  ; if an error happened, it doesn't mean we need to penalize Fenzo
          )))))

(defn view-incubating-offers
  [^TaskScheduler fenzo]
  (let [pending-offers (for [^com.netflix.fenzo.VirtualMachineCurrentState state (locking fenzo (.getVmCurrentStates fenzo))
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
  [conn driver-atom fenzo fid-atom pending-jobs-atom
   max-considerable scaleback
   floor-iterations-before-warn floor-iterations-before-reset]
  (let [chan-length 100
        offers-chan (async/chan (async/buffer chan-length))
        resources-atom (atom (view-incubating-offers fenzo))
        timer-chan (chime-ch (periodic/periodic-seq (time/now) (time/seconds 1))
                             {:ch (async/chan (async/sliding-buffer 1))})]
    (async/thread
      (loop [num-considerable max-considerable]
        (reset! fenzo-num-considerable-atom num-considerable)

        ;;TODO make this cancelable (if we want to be able to restart the server w/o restarting the JVM)
        (let [next-considerable
              (try
                (let [;; There are implications to generating the user->usage here:
                      ;;s ok  1. Currently cook has two oddities in state changes.
                      ;;  We plan to correct both of these but are important for the time being.
                      ;;    a. Cook doesn't mark as a job as running when it schedules a job.
                      ;;       While this is technically correct, it confuses some process.
                      ;;       For example, it will mean that the user->usage generated here
                      ;;       may not include jobs that have been scheduled but haven't started.
                      ;;       Since we do the filter for quota first, this is ok because those jobs
                      ;;       show up in the queue. However, it is important to know about
                      ;;    b. Cook doesn't updat ethe job state when cook hears from mesos about the
                      ;;       state of an instance. Cook waits until it hears from datomic about the
                      ;;       instance state change to change the state of the job. This means that it
                      ;;       is possible to have large delays between when a instance changes status
                      ;;       and the job reflects that change
                      ;;  2. Once the above two items are addressed, user->usage should always correctly
                      ;;     reflect *Cook*'s understanding of the state of the world at this point.
                      ;;     When this happens, users should never exceed their quota
                      user->usage-future (future (generate-user-usage-map (d/db conn)))
                      offers (async/alt!!
                               offers-chan ([offers]
                                            (counters/dec! offer-chan-depth)
                                            offers)
                               timer-chan ([_] [])
                               :priority true)
                      ;; Try to clear the channel
                      offers (->> (repeatedly chan-length #(async/poll! offers-chan))
                                  (filter nil?)
                                  (reduce into offers))
                      _ (log/debug "Passing following offers to handle-resource-offers!" offers)
                      user->quota (quota/create-user->quota-fn (d/db conn))
                      matched-head? (handle-resource-offers! conn @driver-atom fenzo @fid-atom pending-jobs-atom @user->usage-future user->quota num-considerable offers-chan offers)]
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

          (recur
           (if (>= (counters/value iterations-at-fenzo-floor) floor-iterations-before-reset)
             (do
               (log/error "FENZO CANNOT MATCH THE MOST IMPORTANT JOB."
                          "Fenzo has seen only 1 job for " (counters/value iterations-at-fenzo-floor)
                          "iterations, and still hasn't matched it.  Cook is now giving up and will "
                          "now give Fenzo " max-considerable " jobs to look at.")
               (meters/mark! fenzo-abandon-and-reset-meter)
               max-considerable)
             next-considerable)))))
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
  [db driver fid fenzo]
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
                    job (:job/_instance task-ent)
                    task-request (->TaskRequestAdapter job (util/job-ent->resources job) task-id (atom nil))]]
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

(timers/deftimer  [cook-mesos scheduler reconciler-duration])

;; TODO this should be running and enabled
(defn reconciler
  [conn driver fid fenzo & {:keys [interval]
                  :or {interval (* 30 60 1000)}}]
  (log/info "Starting reconciler. Interval millis:" interval)
  (chime-at (periodic/periodic-seq (time/now) (time/millis interval))
            (fn [time]
              (timers/time!
                reconciler-duration
                (reconcile-jobs conn)
                (reconcile-tasks (db conn) driver fid fenzo)))))

(defn get-lingering-tasks
  "Return a list of lingering tasks.

   A lingering task is a task that runs longer than timeout-hours."
  [db now max-timeout-hours default-timeout-hours]
  (->> (q '[:find ?task-id ?start-time ?max-runtime
            :in $ ?default-runtime
            :where
            [(ground [:instance.status/unknown :instance.status/running]) [?status ...]]
            [?i :instance/status ?status]
            [?i :instance/task-id ?task-id]
            [?i :instance/start-time ?start-time]
            [?j :job/instance ?i]
            [(get-else $ ?j :job/max-runtime ?default-runtime) ?max-runtime]]
          db (-> default-timeout-hours time/hours time/in-millis))
       (keep (fn [[task-id start-time max-runtime]]
               ;; The convertion between random time units is because time doesn't like
               ;; Long and Integer/MAX_VALUE is too small for milliseconds
               (let [timeout-minutes (min (.toMinutes TimeUnit/MILLISECONDS max-runtime)
                                          (.toMinutes TimeUnit/HOURS max-timeout-hours))]
                 (when (time/before?
                        (time/plus (tc/from-date start-time) (time/minutes timeout-minutes))
                        now)
                   task-id))))))

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
        @(d/transact
           conn
           [[:instance/update-state [:instance/task-id task-id] :instance.status/failed [:reason/name :max-runtime-exceeded]]
            [:db/add [:instance/task-id task-id] :instance/reason [:reason/name :max-runtime-exceeded]]])))))

(defn lingering-task-killer
  "Periodically kill lingering tasks.

   The config is a map with optional keys where
   :interval-minutes specifies the frequency of killing
   :timout-hours specifies the timeout hours for lingering tasks"
  [conn driver config]
  (let [config (merge {:timeout-interval-minutes 10
                       :timeout-hours (* 2 24)}
                      config)]
    (chime-at (periodic/periodic-seq (time/now) (time/minutes (:timeout-interval-minutes config)))
              (fn [now] (kill-lingering-tasks now conn driver config))
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
  [conn driver {:keys [interval-minutes] :or {interval-minutes 1}}]
  (chime-at (periodic/periodic-seq (time/now) (time/minutes interval-minutes))
            (fn [now] (handle-stragglers conn #(mesos/kill-task! driver {:value (:instance/task-id %)})))
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
  "Every 3 seconds, kill tasks that have been cancelled (e.g. via the API)."
  [conn driver]
  (chime-at (periodic/periodic-seq (time/now) (time/seconds 3))
            (fn [now]
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

(timers/deftimer [cook-mesos scheduler sort-jobs-hierarchy-duration])

(defn sort-normal-jobs-by-dru
  "Return a list of normal job entities ordered by dru"
  [pending-task-ents running-task-ents user->dru-divisors]
  (let [jobs (timers/time!
               sort-jobs-hierarchy-duration
               (-<>> (into running-task-ents pending-task-ents)
                     (group-by util/task-ent->user)
                     (map (fn [[user task-ents]]
                            [user (into (sorted-set-by (util/same-user-task-comparator)) task-ents)]))
                     (into {})
                     (dru/sorted-task-scored-task-pairs <> user->dru-divisors)
                     (filter (fn [[task scored-task]]
                               (or (contains? pending-task-ents task)
                                   ;; backfilled tasks can be upgraded to non-backfilled
                                   ;; during scheduling, so they also need to be considered.
                                   (:instance/backfilled? task))))
                     (map (fn [[task scored-task]]
                            (:job/_instance task)))))]
    jobs))

(timers/deftimer [cook-mesos scheduler sort-gpu-jobs-hierarchy-duration])

(defn sort-gpu-jobs-by-dru
  "Return a list of gpu job entities ordered by dru"
  [pending-task-ents running-task-ents user->dru-divisors]
  (let [jobs (timers/time!
               sort-gpu-jobs-hierarchy-duration
               (-<>> (concat running-task-ents pending-task-ents)
                     (group-by util/task-ent->user)
                     (map (fn [[user task-ents]]
                            [user (into (sorted-set-by (util/same-user-task-comparator)) task-ents)]))
                     (into {})
                     (dru/gpu-task-scored-task-pairs <> user->dru-divisors)
                     (filter (fn [[task _]]
                               (contains? pending-task-ents task)))
                     (map (fn [[task _]]
                            (:job/_instance task)))))]
    jobs))

(defn sort-jobs-by-dru
  "Returns a map from job category to a list of job entities, ordered by dru"
  [filtered-db unfiltered-db]
  ;; This function does not use the filtered db when it is not necessary in order to get better performance
  ;; The filtered db is not necessary when an entity could only arrive at a given state if it was already committed
  ;; e.g. running jobs or when it is always considered committed e.g. shares
  ;; The unfiltered db can also be used on pending job entities once the filtered db is used to limit
  ;; to only those jobs that have been committed.
  (let [pending-job-ents-by (group-by util/categorize-job (util/get-pending-job-ents filtered-db unfiltered-db))
        pending-task-ents-by (reduce-kv (fn [m category pending-job-ents]
                                          (assoc m category
                                                 (into #{}
                                                       (map util/create-task-ent)
                                                       pending-job-ents)))
                                        {}
                                        pending-job-ents-by)
        running-task-ents-by (group-by (comp util/categorize-job :job/_instance)
                                       (util/get-running-task-ents unfiltered-db))
        user->dru-divisors (share/create-user->share-fn unfiltered-db)]
    {:normal (sort-normal-jobs-by-dru (:normal pending-task-ents-by)
                                      (:normal running-task-ents-by)
                                      user->dru-divisors)
     :gpu (sort-gpu-jobs-by-dru (:gpu pending-task-ents-by)
                                (:gpu running-task-ents-by)
                                user->dru-divisors)}))

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
  [filtered-db unfiltered-db offensive-job-filter]
  (timers/time!
    rank-jobs-duration
    (try
      (let [jobs (->> (sort-jobs-by-dru filtered-db unfiltered-db)
                      ;; Apply the offensive job filter first before taking.
                      (map (fn [[category jobs]]
                             (log/debug "filtering category" category jobs)
                             [category (offensive-job-filter jobs)]))
                      (into {} ))]
        (log/debug "Total number of pending jobs is:" (apply + (map count (vals jobs)))
                   "The first 20 pending normal jobs:" (take 20 (:normal jobs))
                   "The first 5 pending gpu jobs:" (take 5 (:gpu jobs)))
        jobs)
      (catch Throwable t
        (log/error t "Failed to rank jobs")
        (meters/mark! rank-jobs-failures)
        {}))))

(defn- start-jobs-prioritizer!
  [conn pending-jobs-atom task-constraints]
  (let [offensive-jobs-ch (make-offensive-job-stifler conn)
        offensive-job-filter (partial filter-offensive-jobs task-constraints offensive-jobs-ch)]
    (chime-at (periodic/periodic-seq (time/now) (time/seconds 5))
              (fn [time]
                (reset! pending-jobs-atom
                        (rank-jobs (db conn) (d/db conn) offensive-job-filter))))))

(meters/defmeter [cook-mesos scheduler mesos-error])
(meters/defmeter [cook-mesos scheduler offer-chan-full-error])

(defn make-fenzo-scheduler
  [driver offer-incubate-time-ms good-enough-fitness]
  (.. (com.netflix.fenzo.TaskScheduler$Builder.)
      (disableShortfallEvaluation) ;; We're not using the autoscaling features
      (withLeaseOfferExpirySecs (max (-> offer-incubate-time-ms time/millis time/in-seconds) 1)) ;; should be at least 1 second
      (withRejectAllExpiredOffers)
      (withFitnessCalculator BinPackingFitnessCalculators/cpuMemBinPacker)
      (withFitnessGoodEnoughFunction (reify com.netflix.fenzo.functions.Func1
                                       (call [_ fitness]
                                         (> fitness good-enough-fitness))))
      (withLeaseRejectAction (reify com.netflix.fenzo.functions.Action1
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
  [conn limit]
  (when limit
    @(d/transact conn [{:db/id :scheduler/config
                        :scheduler.config/mea-culpa-failure-limit limit}])))

(defn create-datomic-scheduler
  [conn set-framework-id driver-atom pending-jobs-atom heartbeat-ch offer-incubate-time-ms mea-culpa-failure-limit fenzo-max-jobs-considered fenzo-scaleback fenzo-floor-iterations-before-warn fenzo-floor-iterations-before-reset task-constraints gpu-enabled? good-enough-fitness]

  (persist-mea-culpa-failure-limit! conn mea-culpa-failure-limit)

  (let [fid (atom nil)
        fenzo (make-fenzo-scheduler driver-atom offer-incubate-time-ms good-enough-fitness)
        [offers-chan resources-atom] (make-offer-handler conn driver-atom fenzo fid pending-jobs-atom fenzo-max-jobs-considered fenzo-scaleback fenzo-floor-iterations-before-warn fenzo-floor-iterations-before-reset)]
    (start-jobs-prioritizer! conn pending-jobs-atom task-constraints)
    {:scheduler
     (mesos/scheduler
      (registered [this driver framework-id master-info]
                  (log/info "Registered with mesos with framework-id " framework-id)
                  (reset! fid framework-id)
                  (set-framework-id framework-id)
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
                      (reconcile-tasks (db conn) driver @fid fenzo)
                      (catch Exception e
                        (log/error e "Reconciliation error")))))
      (reregistered [this driver master-info]
                    (log/info "Reregistered with new master")
                    (future
                      (try
                        (reconcile-jobs conn)
                        (reconcile-tasks (db conn) driver @fid fenzo)
                        (catch Exception e
                          (log/error e "Reconciliation error")))))
      ;; Ignore this--we can just wait for new offers
      (offer-rescinded [this driver offer-id]
                       ;; TODO: Rescind the offer in fenzo
                       )
      (framework-message [this driver executor-id slave-id data]
                        (heartbeat/notify-heartbeat heartbeat-ch executor-id slave-id data))
      (disconnected [this driver]
                    (log/error "Disconnected from the previous master"))
      ;; We don't care about losing slaves or executors--only tasks
      (slave-lost [this driver slave-id])
      (executor-lost [this driver executor-id slave-id status])
      (error [this driver message]
             (meters/mark! mesos-error)
             (log/error "Got a mesos error!!!!" message))
      (resource-offers [this driver offers]
                       (log/info "Got offers, putting them into the offer channel:" offers)
                      (doseq [offer offers]
                        (histograms/update!
                         offer-size-cpus
                         (get-in offer [:resources :cpus] 0))
                        (histograms/update!
                         offer-size-mem
                         (get-in offer [:resources :mem] 0)))

                      (if (async/offer! offers-chan offers)
                        (counters/inc! offer-chan-depth)
                        (do (log/warn "Offer chan is full. Are we not handling offers fast enough?")
                            (meters/mark! offer-chan-full-error)
                            (future
                              (try
                                (decline-offers driver offers)
                                (catch Exception e
                                  (log/error e "Unable to decline offers!")))))))
      (status-update [this driver status]
                     (future (handle-status-update conn driver fenzo status))))
     :view-incubating-offers (fn get-resources-atom [] @resources-atom)}))
