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
  (:require [clj-mesos.scheduler :as mesos]
            [cook.mesos.util :as util]
            [cook.mesos.dru :as dru]
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
            [clj-time.core :as time]
            [clj-time.periodic :as periodic]
            [clj-time.coerce :as tc]
            [chime :refer [chime-at chime-ch]]
            [cook.mesos.heartbeat :as heartbeat]
            [cook.mesos.share :as share]
            [swiss.arrows :refer :all]
            [clojure.core.cache :as cache]
            [cook.mesos.reason :refer :all])
  (import java.util.concurrent.TimeUnit
          com.netflix.fenzo.TaskAssignmentResult
          com.netflix.fenzo.TaskScheduler
          com.netflix.fenzo.VirtualMachineLease))

(defn now
  []
  (java.util.Date.))

(defn tuplify-offer
  "Takes an offer and converts it to a queryable format for datomic"
  [{:as offer
    :keys [slave-id]
    {:keys [cpus mem]} :resources}]
  [slave-id cpus mem])

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

(defn job->task-info
  "Takes a job entity, returns a taskinfo"
  [db fid job]
  (let [job-ent (d/entity db job)
        task-id (str (java.util.UUID/randomUUID))
        resources (util/job-ent->resources job-ent)
        ;; If the custom-executor attr isn't set, we default to using a custom
        ;; executor in order to support jobs submitted before we added this field
        container (util/job-ent->container db job job-ent)
        custom-executor (:job/custom-executor job-ent true)
        environment (util/job-ent->env job-ent)
        labels (util/job-ent->label job-ent)
        command {:value (:job/command job-ent)
                 :environment environment
                 :user (:job/user job-ent)
                 :uris (:uris resources [])}
        ;; executor-{key,value} configure whether this is a command or custom
        ;; executor
        executor-key (if custom-executor :executor :command)
        executor-value (if custom-executor
                         (merge {:executor-id (str (java.util.UUID/randomUUID))
                                 :framework-id fid
                                 :name "cook agent executor"
                                 :source "cook_scheduler"
                                 :command command}
                                (when (seq container)
                                  {:container container}))
                         command)]
    ;; If the there is no value for key :job/name, the following name will contain a substring "null".
    (merge {:name (format "%s_%s_%s" (:job/name job-ent "cookjob") (:job/user job-ent) task-id)
            :task-id task-id
            :num-ports (:ports resources)
            :resources (select-keys resources [:mem :cpus])
            :labels labels
            ;;TODO this data is a race-condition
            :data (.getBytes
                   (pr-str
                    {:instance (str (count (:job/instance job-ent)))})
                   "UTF-8")
            executor-key executor-value}
           (when (and (seq container) (not custom-executor))
             {:container container}))))

(defn rescind-offer!
  [rescinded-offer-id-cache offer-id]
  (log/info "Rescinding offer" offer-id)
  (swap! rescinded-offer-id-cache (fn [c]
                                    (if (cache/has? c offer-id)
                                      (cache/hit c offer-id)
                                      (cache/miss c offer-id offer-id)))))

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

(defn handle-status-update
  "Takes a status update from mesos."
  [conn driver ^TaskScheduler fenzo status]
  (future
    (log/info "Mesos status is: " status)
    (timers/time!
      handle-status-update-duration
      (try (let [db (db conn)
                 {:keys [task-id reason] task-state :state} status
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
                 previous-reason (:instance/reason-code instance-ent)
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
                 progress (try 
                              (when (:data status)
                                  (:percent (read-string (String. (:data status)))))
                          (catch Exception e
                              (log/debug e "Error parse mesos status data. Is it in the format we expect?")))
                 instance-ent (d/entity db instance)
                 instance-runtime (- (.getTime (now)) ; Used for reporting
                                     (.getTime (:instance/start-time instance-ent)))
                 job-resources (util/job-ent->resources job-ent)]
             (when (#{:instance.status/success :instance.status/failed} instance-status)
               (log/debug "Unassigning task" task-id "from" (:instance/hostname instance-ent))
               (try
                 (.. fenzo
                     (getTaskUnAssigner)
                     (call task-id (:instance/hostname instance-ent)))
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
               (mesos/kill-task driver task-id))
             (when-not (nil? instance)
               ;; (println "update:" task-id task-state job instance instance-status prior-job-state)
               (log/debug "Transacting updated state for instance" instance "to status" instance-status)
               (transact-with-retries conn
                                      (concat
                                        [[:instance/update-state instance instance-status]]
                                        (when (and (#{:instance.status/failed} instance-status) (not previous-reason) reason)
                                          [[:db/add instance :instance/reason-code (mesos-reason->cook-reason-code reason)]])
                                        (when (#{:instance.status/success
                                                 :instance.status/failed} instance-status)
                                          [[:db/add instance :instance/end-time (now)]])
                                        (when progress
                                          [[:db/add instance :instance/progress progress]])))))
           (catch Exception e
             (log/error e "Mesos scheduler status update error"))))))

(timers/deftimer [cook-mesos scheduler tx-report-queue-processing-duration])
(meters/defmeter [cook-mesos scheduler tx-report-queue-datoms])
(meters/defmeter [cook-mesos scheduler tx-report-queue-update-job-state])
(meters/defmeter [cook-mesos scheduler tx-report-queue-job-complete])
(meters/defmeter [cook-mesos scheduler tx-report-queue-tasks-killed])

(defn monitor-tx-report-queue
  "Takes an async channel that will have tx report queue elements on it"
  [tx-report-chan conn driver-ref]
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
                               ;; Monitoring whether an instance status is updated.
                               (doseq [e (set (map :e tx-data))]
                                 (try
                                   (when-let [job (:job/_instance (d/entity db e))]
                                     (log/debug "Updating state of job" job "due to update of instance" e)
                                     (meters/mark! tx-report-queue-update-job-state)
                                     (transact-with-retries conn [[:job/update-state (:db/id job)]]))
                                   (catch Exception e
                                     (log/error e "Unexpected exception on tx report queue processor"))))
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
                                         (do (log/debug "Attempting to kill task" task-id "due to job completion")
                                             (meters/mark! tx-report-queue-tasks-killed)
                                             (mesos/kill-task driver task-id))
                                         (log/error "Couldn't kill task" task-id "due to no Mesos driver!"))))
                                   (catch Exception e
                                     (log/error e "Unexpected exception on tx report queue processor")))))))
                          (recur))
          kill-chan ([_] nil))))
    #(async/close! kill-chan)))

(defn job-uuid->job
  "Return the job entity id from a string or java.util.UUID representation of a
   job uuid."
  [db uuid]
  (ffirst (q '[:find ?j
               :in $ ?uuid
               :where
               [?j :job/uuid ?uuid]]
             db (java.util.UUID/fromString (str uuid)))))

;;; ===========================================================================
;;; API for matcher
;;; ===========================================================================

(defrecord VirtualMachineLeaseAdapter [offer time]
  com.netflix.fenzo.VirtualMachineLease
  (cpuCores [_] (get-in offer [:resources :cpus] 0.0))
  (diskMB [_] (get-in offer [:resources :disk] 0.0))
  (getAttributeMap [_] {}) ;;TODO
  (getId [_] (:id offer))
  (getOffer [_] (throw (UnsupportedOperationException.)))
  (getOfferedTime [_] time)
  (getVMID [_] (:slave-id offer))
  (hostname [_] (:hostname offer))
  (memoryMB [_] (get-in offer [:resources :mem] 0.0))
  (networkMbps [_] 0.0)
  (portRanges [_] (mapv (fn [{:keys [begin end]}]
                          (com.netflix.fenzo.VirtualMachineLease$Range. begin end))
                        (get-in offer [:resources :ports]))))

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

(defrecord TaskRequestAdapter [job task-info]
  com.netflix.fenzo.TaskRequest
  (getCPUs [_] (get-in task-info [:resources :cpus]))
  (getDisk [_] 0.0)
  (getHardConstraints [_] [(novel-host-constraint job)])
  (getId [_] (:task-id task-info))
  (getMemory [_] (get-in task-info [:resources :mem]))
  (getNetworkMbps [_] 0.0)
  (getPorts [_] (:num-ports task-info))
  (getSoftConstraints [_] [])
  (taskGroupName [_] (str (:job/uuid job))))

(defn match-offer-to-schedule
  "Given an offer and a schedule, computes all the tasks should be launched as a result.

   A schedule is just a sorted list of tasks, and we're going to greedily assign them to
   the offer.

   Returns a list of tasks that got matched to the offer"
  [^TaskScheduler fenzo considerable offers db fid]
  (log/debug "Matching" (count offers) "offers to" (count considerable) "jobs with fenzo")
  (let [t (System/currentTimeMillis)
        leases (mapv #(->VirtualMachineLeaseAdapter % t) offers)
        requests (mapv (fn [job]
                         (->TaskRequestAdapter job (job->task-info db fid (:db/id job))))
                       considerable)
        result (.scheduleOnce fenzo requests leases)
        failure-results (.. result getFailures values)
        assignments (.. result getResultMap values)]
    (log/info "Found this assigment:" result)
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
(timers/deftimer [cook-mesos scheduler handle-resource-offer!-match-duration])
(meters/defmeter [cook-mesos scheduler pending-job-atom-contended])

(histograms/defhistogram [cook-mesos scheduler offer-size-mem])
(histograms/defhistogram [cook-mesos scheduler offer-size-cpus])
(histograms/defhistogram [cook-mesos scheduler number-tasks-matched])
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
        tasks-ids-to-upgrade (->> jobs-to-upgrade (mapv backfilled-ids-memo) (apply concat) vec)]

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

    {:fully-processed (mapv :job/uuid jobs-fully-processed)
     :upgrade-backfill tasks-ids-to-upgrade
     :backfill-jobs (mapv :job/uuid jobs-to-backfill)
     :matched-head? (matched? head-of-considerable)}))

(defn handle-resource-offers!
  "Gets a list of offers from mesos. Decides what to do with them all--they should all
   be accepted or rejected at the end of the function."
  [conn driver ^TaskScheduler fenzo fid pending-jobs num-considerable offers-chan offers]
  (log/debug "invoked handle-resource-offers!")
  (let [offer-stash (atom nil)] ;; This is a way to ensure we never lose offers fenzo assigned if an errors occures in the middle of processing
    (timers/time!
      handle-resource-offer!-duration
      (try
        (loop [] ;; This loop is for compare-and-set! below
          (let [scheduler-contents @pending-jobs
                db (db conn)
                _ (log/debug "There are" (count scheduler-contents) "pending jobs")
                considerable (->> scheduler-contents
                                  (filter (fn [job]
                                            (util/job-allowed-to-start? db job)))
                                  (take num-considerable))
                _ (log/debug "We'll consider scheduling" (count considerable) "of those pending jobs (limited to " num-considerable " due to backdown)")
                matches (match-offer-to-schedule fenzo considerable offers db fid)
                _ (reset! offer-stash (doall (for [{:keys [leases]} matches
                                                   lease leases]
                                               (:offer lease))))
                matched-jobs (for [match matches
                                   ^TaskAssignmentResult task-result (:tasks match)
                                   :let [task-request (.getRequest task-result)]]
                               (:job task-request))
                processed-matches (process-matches-for-backfill scheduler-contents (first considerable) matched-jobs)
                new-scheduler-contents (remove (fn [{pending-job-uuid :job/uuid}]
                                                 (or (contains? (:fully-processed processed-matches) pending-job-uuid)
                                                     (contains? (:upgrade-backfill processed-matches) pending-job-uuid)))
                                               scheduler-contents)
                ;; We don't remove backfilled jobs here, because although backfilled
                ;; jobs have already been scheduled in a sense, the scheduler still can't
                ;; adjust the status of backfilled tasks.
                ;; Backfilled tasks can be updgraded to non-backfilled after the jobs
                ;; prioritized above them are also scheduled.
                first-considerable-resources (-> considerable first util/job-ent->resources)
                match-resource-requirements (util/sum-resources-of-jobs matched-jobs)]
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
              (not (compare-and-set! pending-jobs scheduler-contents new-scheduler-contents))
              (do
                (log/debug "Pending job atom contention encountered, recycling offers:" @offer-stash)
                (async/go
                  (async/>! offers-chan @offer-stash))
                (meters/mark! pending-job-atom-contended)
                (recur))
              :else
              (let [task-txns (for [{:keys [tasks leases]} matches
                                    :let [offers (mapv :offer leases)
                                          slave-id (:slave-id (first offers))]
                                    ^TaskAssignmentResult task tasks
                                    :let [request (.getRequest task)
                                          task-info (:task-info request)
                                          job-id (get-in request [:job :db/id])]]
                                [[:job/allowed-to-start? job-id]
                                 {:db/id (d/tempid :db.part/user)
                                  :job/_instance job-id
                                  :instance/task-id (:task-id task-info)
                                  :instance/hostname (.getHostname task)
                                  :instance/start-time (now)
                                  ;; NB command executor uses the task-id
                                  ;; as the executor-id
                                  :instance/executor-id (get-in
                                                          task-info
                                                          [:executor :executor-id]
                                                          (:task-id task-info))
                                  :instance/backfilled? (contains? (:backfill-jobs processed-matches) (get-in request [:job :job/uuid]))
                                  :instance/slave-id slave-id
                                  :instance/ports (.getAssignedPorts task)
                                  :instance/progress 0
                                  :instance/status :instance.status/unknown
                                  :instance/preempted? false}])
                    upgrade-txns (map (fn [instance-id]
                                        [:db/add instance-id :instance/backfilled? false])
                                      (:upgrade-backfill processed-matches))]
                ;; Note that this transaction can fail if a job was scheduled
                ;; during a race. If that happens, then other jobs that should
                ;; be scheduled will not be eligible for rescheduling until
                ;; the pending-jobs atom is repopulated
                @(d/transact
                   conn
                   (vec (apply concat upgrade-txns task-txns)))
                (log/info "Launching" (count task-txns) "tasks")
                (log/info "Upgrading" (count (:upgrade-backfill processed-matches)) "tasks from backfilled to proper")
                (log/debug "Matched tasks" task-txns)
                ;; This launch-tasks MUST happen after the above transaction in
                ;; order to allow a transaction failure (due to failed preconditions)
                ;; to block the launch
                (meters/mark! scheduler-offer-matched
                              (->> matches
                                   (mapcat (comp :id :offer :leases))
                                   (distinct)
                                   (count)))
                (meters/mark! matched-tasks (count task-txns))
                (meters/mark! matched-tasks-cpus (:cpus match-resource-requirements))
                (meters/mark! matched-tasks-mem (:mem match-resource-requirements))
                (doseq [{:keys [tasks leases]} matches
                        :let [offers (mapv :offer leases)
                              task-infos (mapv (fn process-results [^TaskAssignmentResult task]
                                                 (reduce
                                                   (fn add-ports-to-task-info [task-info [index port]]
                                                     (log/debug "task-info" task-info [index port])
                                                     (-> task-info
                                                         (update-in [:resources :ports]
                                                                    (fnil conj [])
                                                                    {:begin port :end port})
                                                         (assoc-in [:command :environment (str "PORT" index)]
                                                                   (str port))))
                                                   (:task-info (.getRequest task))
                                                   (map-indexed (fn [index port] [index port])
                                                                (.getAssignedPorts task))))
                                               tasks)
                              slave-id (:slave-id (first offers))]]
                  (mesos/launch-tasks
                    driver
                    (mapv :id offers)
                    (mapv #(-> %
                               (assoc :slave-id slave-id)
                               (dissoc :num-ports))
                          task-infos))
                  (doseq [^TaskAssignmentResult task tasks]
                    (.. fenzo
                        (getTaskAssigner)
                        (call (.getRequest task) (get-in (first leases) [:offer :hostname])))))
                (:matched-head? processed-matches)))))
      (catch Throwable t
        (meters/mark! handle-resource-offer!-errors)
        (log/error t "Error in match:" (ex-data t))
        (async/go
          (async/>! offers-chan @offer-stash))
        true  ; if an error happened, it doesn't mean we need to penalize Fenzo
        )))))

(defn view-incubating-offers
  [^TaskScheduler fenzo]
  (let [pending-offers (for [^com.netflix.fenzo.VirtualMachineCurrentState state (.getVmCurrentStates fenzo)
                             :let [lease (.getCurrAvailableResources state)]
                             :when lease]
                         {:hostname (.hostname lease)
                          :slave-id (.getVMID lease)
                          :resources {:cpus (.cpuCores lease)
                                      :mem (.memoryMB lease)}})]
    (log/debug "We have" (count pending-offers) "pending offers")
    pending-offers))

(def fenzo-num-considerable-atom (atom 0))
(gauges/defgauge [cook-mesos scheduler fenzo-num-considerable] (fn [] @fenzo-num-considerable-atom))
(counters/defcounter [cook-mesos scheduler iterations-at-fenzo-floor])
(meters/defmeter [cook-mesos scheduler fenzo-abandon-and-reset-meter])

(defn make-offer-handler
  [conn driver-atom fenzo fid-atom pending-jobs-atom
   max-considerable scaleback
   floor-iterations-before-warn floor-iterations-before-reset]
  (let [offers-chan (async/chan (async/buffer 5))
        resources-atom (atom (view-incubating-offers fenzo))
        timer-chan (chime-ch (periodic/periodic-seq (time/now) (time/seconds 1))
                             {:ch (async/chan (async/sliding-buffer 1))})]
    (async/thread
      (loop [num-considerable max-considerable]
        (reset! fenzo-num-considerable-atom num-considerable)

        ;;TODO make this cancelable (if we want to be able to restart the server w/o restarting the JVM)
        (let [next-considerable
              (try
                (let [offers (async/alt!!
                               offers-chan ([offers] offers)
                               timer-chan ([_] [])
                               :priority true)
                      matched-head? (handle-resource-offers! conn @driver-atom fenzo @fid-atom pending-jobs-atom num-considerable offers-chan offers)]
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
;; TODO this may need a lock on the fenzo for the taskAssigner
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
      (doseq [[task-id] running-tasks
              :let [task-ent (d/entity db task-id)
                    hostname (:instance/hostname task-ent)
                    job (:job/_instance task-ent)]]
        (.. fenzo
            (getTaskAssigner)
            (call (->TaskRequestAdapter job (job->task-info db fid (:db/id job))) hostname)))
      (doseq [ts (partition-all 50 running-tasks)]
        (log/info "Reconciling" (count ts) "tasks, including task" (first ts))
        (mesos/reconcile-tasks driver (mapv (fn [[task-id status slave-id]]
                                              {:task-id task-id
                                               :state (sched->mesos status)
                                               :slave-id slave-id})
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
  [db now timeout-hours]
  (->> (q '[:find ?task-id ?start-time ?max-runtime
            :in $
            :where
            [(ground [:instance.status/unknown :instance.status/running]) [?status ...]]
            [?i :instance/status ?status]
            [?i :instance/task-id ?task-id]
            [?i :instance/start-time ?start-time]
            [?j :job/instance ?i]
            [(get-else $ ?j :job/max-runtime Long/MAX_VALUE) ?max-runtime]]
          db)
       (keep (fn [[task-id start-time max-runtime]]
               ;; The convertion between random time units is because time doesn't like
               ;; Long and Integer/MAX_VALUE is too small for milliseconds
               (let [timeout-minutes (min (.toMinutes TimeUnit/MILLISECONDS max-runtime)
                                          (.toMinutes TimeUnit/HOURS timeout-hours)
                                          Integer/MAX_VALUE)]
                 (when (time/before?
                        (time/plus (tc/from-date start-time) (time/minutes timeout-minutes))
                        now)
                   task-id))))))

(defn lingering-task-killer
  "Periodically kill lingering tasks.

   The config is a map with optional keys where
   :interval-minutes specifies the frequency of killing
   :timout-hours specifies the timeout hours for lingering tasks"
  [conn driver config]
  (let [{interval-minutes :timeout-interval-minutes
         timeout-hours :timeout-hours}
        (merge {:timeout-interval-minutes 10
                :timeout-hours (* 2 24)}
               config)]
    (chime-at (periodic/periodic-seq (time/now) (time/minutes interval-minutes))
              (fn [now]
                (let [db (d/db conn)
                      lingering-tasks (get-lingering-tasks db now timeout-hours)]
                  (when (seq lingering-tasks)
                    (log/info "Starting to kill lingering jobs running more than" timeout-hours
                              "hours. There are in total" (count lingering-tasks) "lingering tasks.")
                    (doseq [task-id lingering-tasks]
                      (log/info "Killing lingering task" task-id)
                      ;; Note that we probably should update db to mark a task failed as well.
                      ;; However in the case that we fail to kill a particular task in Mesos,
                      ;; we could lose the chances to kill this task again.
                      (mesos/kill-task driver task-id)
                      @(d/transact
                        conn
                        [[:instance/update-state [:instance/task-id task-id] :instance/status/failed]
                         [:db/add [:instance/task-id task-id] :instance/reason-code reason-max-runtime]])
                      ))))
              {:error-handler (fn [e]
                                (log/error e "Failed to reap timeout tasks!"))})))

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

(defn sort-jobs-by-dru
  "Return a list of job entities ordered by dru"
  [filtered-db unfiltered-db]
  ;; This function does not use the filtered db when it is not necessary in order to get better performance
  ;; The filtered db is not necessary when an entity could only arrive at a given state if it was already committed
  ;; e.g. running jobs or when it is always considered committed e.g. shares
  ;; The unfiltered db can also be used on pending job entities once the filtered db is used to limit
  ;; to only those jobs that have been committed.
  (let [pending-job-ents (util/get-pending-job-ents filtered-db unfiltered-db)
        pending-task-ents (into #{} (map util/create-task-ent pending-job-ents))
        running-task-ents (util/get-running-task-ents unfiltered-db)
        user->dru-divisors (dru/init-user->dru-divisors unfiltered-db running-task-ents pending-job-ents)
        jobs (timers/time!
               sort-jobs-hierarchy-duration
               (-<>> (concat running-task-ents pending-task-ents)
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

(timers/deftimer [cook-mesos scheduler rank-jobs-duration])
(meters/defmeter [cook-mesos scheduler rank-jobs-failures])

(defn filter-offensive-jobs
  "Base on the constraints on memory and cpus, given a list of job entities it
   puts the offensive jobs into offensive-job-ch asynchronically and returns
   the inoffensive jobs.

   A job is offensive if and only if its required memory or cpus exceeds the
   limits"
  ;; TODO these limits should come from the largest observed host from Fenzo
  ;; .getResourceStatus on TaskScheduler will give a map of hosts to resources; we can compute the max over those
  [{max-memory-gb :memory-gb max-cpus :cpus} offensive-jobs-ch jobs]
  (let [max-memory-mb (* 1024.0 max-memory-gb)
        categorized-jobs (group-by (fn [job]
                                     (let [{memory-mb :mem
                                            cpus :cpus} (util/job-ent->resources job)]
                                       (if (or (> memory-mb max-memory-mb)
                                               (> cpus max-cpus))
                                         :offensive
                                         :inoffensive)))
                                   jobs)]
    ;; Put offensive jobs asynchronically such that it could return the
    ;; inoffensive jobs immediately.
    (async/go
      (when (seq (:offensive categorized-jobs))
        (log/info "Found" (count (:offensive categorized-jobs)) "offensive jobs")
        (async/>! offensive-jobs-ch (:offensive categorized-jobs))))
    (:inoffensive categorized-jobs)))

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

(defn rank-jobs
  "Return a list of job entities ordered by dru.

   It ranks the jobs by dru first and then apply several filters if provided."
  [filtered-db unfiltered-db offensive-job-filter]
  (timers/time!
    rank-jobs-duration
    (try
      (let [jobs (->> (sort-jobs-by-dru filtered-db unfiltered-db)
                      ;; Apply the offensive job filter first before taking.
                      offensive-job-filter)]
        (log/debug "Total number of pending jobs is:" (count jobs)
                   "The first 20 pending jobs:" (take 20 jobs))
        jobs)
      (catch Throwable t
        (log/error t "Failed to rank jobs")
        (meters/mark! rank-jobs-failures)
        []))))

(defn- start-jobs-prioritizer!
  [conn pending-jobs-atom task-constraints]
  (let [offensive-jobs-ch (make-offensive-job-stifler conn)
        offensive-job-filter (partial filter-offensive-jobs task-constraints offensive-jobs-ch)]
    (chime-at (periodic/periodic-seq (time/now) (time/seconds 5))
              (fn [time]
                (reset! pending-jobs-atom
                        (rank-jobs (db conn) (d/db conn) offensive-job-filter))))))

(meters/defmeter [cook-mesos scheduler mesos-error])

(defn make-fenzo-scheduler
  [driver offer-incubate-time-ms]
  (.. (com.netflix.fenzo.TaskScheduler$Builder.)
      (disableShortfallEvaluation) ;; We're not using the autoscaling features
      (withLeaseOfferExpirySecs (max (-> offer-incubate-time-ms time/millis time/in-seconds) 1)) ;; should be at least 1 second
      (withRejectAllExpiredOffers)
      (withDebugEnabled)
      (withLeaseRejectAction (reify com.netflix.fenzo.functions.Action1
                               (call [_ lease]
                                 (let [offer (:offer lease)
                                       id (:id offer)]
                                   (log/debug "Fenzo is declining offer" offer)
                                   (if-let [driver @driver]
                                     (mesos/decline-offer driver id)
                                     (log/error "Unable to decline offer; no current driver"))))))
      (build)))

(defn create-datomic-scheduler
  [conn set-framework-id driver-atom pending-jobs-atom heartbeat-ch offer-incubate-time-ms fenzo-max-jobs-considered fenzo-scaleback fenzo-floor-iterations-before-warn fenzo-floor-iterations-before-reset task-constraints]
  (let [fid (atom nil)
        ;; Mesos can potentially rescind thousands of offers
        rescinded-offer-id-cache (-> {}
                                     (cache/fifo-cache-factory :threshold 10240)
                                     atom)
        offer-chan (async/chan 100)
        offer-ready-chan (async/chan 100)
        matcher-chan (async/chan) ; Don't want to buffer offers to the matcher chan. Only want when ready

        fenzo (make-fenzo-scheduler driver-atom offer-incubate-time-ms)
        [offers-chan resources-atom] (make-offer-handler conn driver-atom fenzo fid pending-jobs-atom fenzo-max-jobs-considered fenzo-scaleback fenzo-floor-iterations-before-warn fenzo-floor-iterations-before-reset)]
    (start-jobs-prioritizer! conn pending-jobs-atom task-constraints)
    {:scheduler
     (mesos/scheduler
      (registered [driver framework-id master-info]
                  (log/info "Registered with mesos with framework-id " framework-id)
                  (reset! fid framework-id)
                  (set-framework-id framework-id)
                  (future
                    (try
                      (reconcile-jobs conn)
                      (reconcile-tasks (db conn) driver @fid fenzo)
                      (catch Exception e
                        (log/error e "Reconciliation error")))))
      (reregistered [driver master-info]
                    (log/info "Reregistered with new master")
                    (future
                      (try
                        (reconcile-jobs conn)
                        (reconcile-tasks (db conn) driver @fid fenzo)
                        (catch Exception e
                          (log/error e "Reconciliation error")))))
      ;; Ignore this--we can just wait for new offers
      (offerRescinded [driver offer-id]
                      (rescind-offer! rescinded-offer-id-cache offer-id))
      (frameworkMessage [driver executor-id slave-id data]
                        (heartbeat/notify-heartbeat heartbeat-ch executor-id slave-id data))
      (disconnected [driver]
                    (log/error "Disconnected from the previous master"))
      ;; We don't care about losing slaves or executors--only tasks
      (slaveLost [driver slave-id])
      (executorLost [driver executor-id slave-id status])
      (error [driver message]
             (meters/mark! mesos-error)
             (log/error "Got a mesos error!!!!" message))
      (resourceOffers [driver offers]
                      (log/debug "Got an offer, putting it into the offer channel:" offers)
                      (doseq [offer offers]
                        (histograms/update!
                         offer-size-cpus
                         (get-in offer [:resources :cpus] 0))
                        (histograms/update!
                         offer-size-mem
                         (get-in offer [:resources :mem] 0)))

                      (when-not (async/offer! offers-chan offers)
                        (decline-offers driver offers)))
      (statusUpdate [driver status]
                    (handle-status-update conn driver fenzo status)))
     :view-incubating-offers (fn get-resources-atom [] @resources-atom)}))
