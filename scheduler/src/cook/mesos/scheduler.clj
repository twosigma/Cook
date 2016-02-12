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
            [chime :refer [chime-at]]
            [cook.mesos.heartbeat :as heartbeat]
            [cook.mesos.share :as share]
            [swiss.arrows :refer :all])
  (import java.util.concurrent.TimeUnit))

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
        custom-executor (:job/custom-executor job-ent true)
        environment (util/job-ent->env job-ent)
        command {:value (:job/command job-ent)
                 :environment environment
                 :user (:job/user job-ent)
                 :uris (:uris resources [])}
        ;; executor-{key,value} configure whether this is a command or custom
        ;; executor
        executor-key (if custom-executor :executor :command)
        executor-value (if custom-executor
                         {:executor-id (str (java.util.UUID/randomUUID))
                          :framework-id fid
                          :name "cook agent executor"
                          :source "cook_scheduler"
                          :command command}
                         command)]
    ;; If the there is no value for key :job/name, the following name will contain a substring "null".
    {:name (format "%s_%s_%s" (:job/name job-ent "cookjob") (:job/user job-ent) task-id)
     :task-id task-id
     :num-ports (count (:ports resources))
     :resources (select-keys resources [:mem :cpus])
     ;;TODO this data is a race-condition
     :data (.getBytes
             (pr-str
               {:instance (str (count (:job/instance job-ent)))})
             "UTF-8")
     executor-key executor-value}))

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
  [conn driver status]
  (future
    (log/info "Mesos status is: " status)
    (timers/time!
      handle-status-update-duration
      (try (let [db (db conn)
                 {:keys [task-id] task-state :state} status
                 [job instance prior-instance-status] (first (q '[:find ?j ?i ?status
                                                                  :in $ ?task-id
                                                                  :where
                                                                  [?i :instance/task-id ?task-id]
                                                                  [?i :instance/status ?s]
                                                                  [?s :db/ident ?status]
                                                                  [?j :job/instance ?i]]
                                                                db task-id))
                 job-ent (d/entity db job)
                 retries-so-far (count (:job/instance job-ent))
                 instance-status (condp contains? task-state
                                   #{:task-staging} :instance.status/unknown
                                   #{:task-starting
                                     :task-running} :instance.status/running
                                   #{:task-finished} :instance.status/success
                                   #{:task-failed
                                     :task-killed
                                     :task-lost} :instance.status/failed)
                 prior-job-state (:job/state (d/entity db job))
                 progress (when (:data status)
                            (:percent (read-string (String. (:data status)))))
                 instance-runtime (- (.getTime (now)) ; Used for reporting
                                     (.getTime (:instance/start-time (d/entity db instance))))
                 job-resources (util/job-ent->resources job-ent)]
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
               (transact-with-retries conn
                                      (concat
                                        [[:instance/update-state instance instance-status]]
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

(def combine-offer-key
  "Function to generate the key used to group offers."
  (juxt :hostname :slave-id :driver :fid))

(meters/defmeter [cook-mesos scheduler offers-combine])

(defn combine-offers
  "Takes a sequence of offers and returns a new sequence of offers such that all offers with the
   same slave-id, hostname, driver, fid are combined into a single offer. Offers will have the following fields
   set: `:resources` (same as a single offer, but excluding ports), `:hostname`, `:slave-id`,
   `:ids` (a list of the all the composite offer ids), ':driver', ':fid', ':time-receieved'.
   The combine offer will use the earliest time-received of the offers."
  [offers]
  (for [[[hostname slave-id driver fid] offers] (group-by combine-offer-key offers)
        :let [ids (mapcat #(or (:ids %) (seq [(:id %)])) offers)
              cpu (apply + (map (comp :cpus :resources) offers))
              mem (apply + (map (comp :mem :resources) offers))
              ports (vec (apply concat (map (comp :ports :resources) offers)))
              time-received (->> offers
                                (sort-by :time-received)
                                first
                                :time-received)]]
    (do
      (when (> (count offers) 1)
        (meters/mark! offers-combine)
        (log/debug "Combining offers " offers))
      {:ids ids
       :resources {:cpus cpu
                   :mem mem
                   :ports ports}
       :hostname hostname
       :slave-id slave-id
       :driver driver
       :fid fid
       :time-received time-received})))

(defn prefixes
  "Returns a seq of the prefixes of a seq"
  [xs]
  (if (seq xs)
    (reductions conj [(first xs)] (rest xs))
    []))

(defn match-offer-to-schedule
  "Given an offer and a schedule, computes all the tasks should be launched as a result.

   A schedule is just a sorted list of tasks, and we're going to greedily assign them to
   the offer.

   Returns a list of tasks that got matched to the offer"
  [schedule {{:keys [cpus mem]} :resources :as offer}]
  (log/debug "Matching offer " offer " to the schedule" schedule)
  (loop [[candidates & remaining-prefixes] (prefixes schedule)
         prev []]
    (let [required (util/sum-resources-of-jobs candidates)]
      (if (and candidates (>= cpus (:cpus required)) (>= mem (:mem required)))
        (recur remaining-prefixes candidates)
        prev))))

(meters/defmeter [cook-mesos scheduler scheduler-offer-declined])

(defn decline-offers
  "declines a collection of offer ids"
  [driver offer-ids]
  (log/debug "Declining offers:" offer-ids)
  (doseq [id offer-ids]
    (meters/mark! scheduler-offer-declined)
    (mesos/decline-offer driver id)))

(defn get-used-hosts
  "Return a set of hosts that the job has already used"
  [db job]
  (->> (q '[:find ?host
            :in $ ?j
            :where
            [?j :job/instance ?i]
            [?i :instance/hostname ?host]]
          db job)
       (map (fn [[x]] x))
       (into #{})))

(defn sort-offers
  "Sorts the offers such that the largest offers are in front"
  ;; TODO: Use dru instead of mem as measure of largest
  [offers]
  (->> offers
       (sort-by (fn [{{:keys [mem cpus]} :resources}] [mem cpus]))
       reverse))

(timers/deftimer [cook-mesos scheduler handle-resource-offer!-duration])
(timers/deftimer [cook-mesos scheduler handler-resource-offer!-transact-task-duration])
(timers/deftimer [cook-mesos scheduler handler-resource-offer!-match-duration])
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

(defn handle-resource-offer!
  "Gets a list of offers from mesos. Decides what to do with them all--they should all
   be accepted or rejected at the end of the function."
  [conn driver fid pending-jobs offer]
  (try
    (histograms/update!
     offer-size-cpus
     (get-in offer [:resources :cpus]))
    (histograms/update!
     offer-size-mem
     (get-in offer [:resources :mem]))
    (catch Throwable t
      (log/warn t "Error in updating offer metrics:" (ex-data t))))

  (timers/time!
    handle-resource-offer!-duration
    (try
      (loop [] ;; This loop is for compare-and-set! below
        (let [scheduler-contents @pending-jobs
              db (db conn)
              considerable (filter (fn [job]
                                     (and (try
                                            (d/invoke db :job/allowed-to-start? db (:db/id job))
                                            true
                                            (catch clojure.lang.ExceptionInfo e
                                              false))
                                          ;; ensure it always tries new hosts
                                          (not-any? #(= % (:hostname offer))
                                                    (->> (:job/instance job)
                                                         (filter #(false? (:instance/preempted? %)))
                                                         (map :instance/hostname)))))
                                   scheduler-contents)
              matches (timers/time!
                        handler-resource-offer!-match-duration
                        (match-offer-to-schedule considerable offer))
              ;; We know that matches always form a prefix of the scheduler's contents
              new-scheduler-contents (remove (set matches) scheduler-contents)
              first-considerable-resources (-> considerable first util/job-ent->resources)
              match-resource-requirements (util/sum-resources-of-jobs matches)]
          (reset! front-of-job-queue-mem-atom
                  (or (:mem first-considerable-resources) 0))
          (reset! front-of-job-queue-cpus-atom
                  (or (:cpus first-considerable-resources) 0))
          (cond
            (empty? matches) (decline-offers driver (:ids offer))
            (not (compare-and-set! pending-jobs scheduler-contents new-scheduler-contents))
            (do (log/info "Pending job atom contention encountered")
                (meters/mark! pending-job-atom-contended)
                (recur))
            :else
            (let [task-infos (map (fn [job]
                                    (let [job-id (:db/id job)]
                                      (assoc (job->task-info
                                               db
                                               fid
                                               job-id)
                                             :slave-id (:slave-id offer)
                                             :job-id job-id)))
                                  matches)]
              ;TODO assign the ports to actual available ones, and configure the environment variables; look at the Fenzo API to match up with it
              ;First we need to convince mesos to actually offer ports
              ;then, we need to allocate the ports (incremental or bulk?) copy from my book.

              ;; Note that this transaction can fail if a job was scheduled
              ;; during a race. If that happens, then other jobs that should
              ;; be scheduled will not be eligible for rescheduling until
              ;; the pending-jobs atom is repopulated
              (timers/time!
                handler-resource-offer!-transact-task-duration
                @(d/transact
                   conn
                   (mapcat (fn [task-info]
                             [[:job/allowed-to-start? (:job-id task-info)]
                              {:db/id (d/tempid :db.part/user)
                               :job/_instance (:job-id task-info)
                               :instance/task-id (:task-id task-info)
                               :instance/hostname (:hostname offer)
                               :instance/start-time (now)
                               ;; NB command executor uses the task-id
                               ;; as the executor-id
                               :instance/executor-id (get-in
                                                       task-info
                                                       [:executor :executor-id]
                                                       (:task-id task-info))
                               :instance/slave-id (:slave-id offer)
                               :instance/progress 0
                               :instance/status :instance.status/unknown
                               :instance/preempted? false}])
                           task-infos)))
              (log/info "Matched offer" offer "to tasks" task-infos)
              ;; This launch-tasks MUST happen after the above transaction in
              ;; order to allow a transaction failure (due to failed preconditions)
              ;; to block the launch
              (meters/mark! scheduler-offer-matched (count (:ids offer)))
              (meters/mark! matched-tasks (count task-infos))
              (histograms/update! number-tasks-matched (count task-infos))
              (meters/mark! matched-tasks-cpus (:cpus match-resource-requirements))
              (meters/mark! matched-tasks-mem (:mem match-resource-requirements))
              (mesos/launch-tasks
                driver
                (:ids offer)
                (mapv #(dissoc % :job-id :num-ports) task-infos))))))
      (catch Throwable t
        (decline-offers driver (:ids offer))
        (meters/mark! handle-resource-offer!-errors)
        (log/error t "Error in match:" (ex-data t))))))

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

(defn reconcile-tasks
  "Finds all non-completed tasks, and has Mesos let us know if any have changed."
  [db driver]
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
      (doseq [ts (partition-all 50 running-tasks)]
        (log/info "Reconciling" (count ts) "tasks, including task" (first ts))
        (mesos/reconcile-tasks driver (mapv (fn [[task-id status slave-id]]
                                              {:task-id task-id
                                               :state (sched->mesos status)
                                               :slave-id slave-id})
                                            ts)))
      (log/info "Finished reconciling all tasks"))))

(timers/deftimer  [cook-mesos scheduler reconciler-duration])

(defn reconciler
  [conn driver & {:keys [interval]
                  :or {interval (* 30 60 1000)}}]
  (log/info "Starting reconciler. Interval millis:" interval)
  (chime-at (periodic/periodic-seq (time/now) (time/millis interval))
            (fn [time]
              (timers/time!
                reconciler-duration
                (reconcile-jobs conn)
                (reconcile-tasks (db conn) driver)))))

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
                  (when-not (zero? (count lingering-tasks))
                    (log/info "Starting to kill lingering jobs running more than" timeout-hours
                              "hours. There are in total" (count lingering-tasks) "lingering tasks.")
                    (doseq [task-id lingering-tasks]
                      (log/info "Killing lingering task" task-id)
                      ;; Note that we probably should update db to mark a task failed as well.
                      ;; However in the case that we fail to kill a particular task in Mesos,
                      ;; we could lose the chances to kill this task again.
                      (mesos/kill-task driver task-id)))))
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

(defn sort-jobs-by-dru
  "Return a list of job entities ordered by dru"
  [db]
  (let [pending-job-ents (util/get-pending-job-ents db)
        pending-task-ents (into #{} (map util/create-task-ent pending-job-ents))
        running-task-ents (util/get-running-task-ents db)
        user->dru-divisors (dru/init-user->dru-divisors db running-task-ents pending-job-ents)
        jobs (-<>> (concat running-task-ents pending-task-ents)
                   (group-by util/task-ent->user)
                   (map (fn [[user task-ents]]
                          [user (into (sorted-set-by util/same-user-task-comparator) task-ents)]))
                   (into {})
                   (dru/init-task->scored-task <> user->dru-divisors)
                   (filter (fn [[task scored-task]]
                             (contains? pending-task-ents task)))
                   (map (fn [[task scored-task]]
                          (:job/_instance task)))
                   (reverse))]
    jobs))

(timers/deftimer [cook-mesos scheduler rank-jobs-duration])
(meters/defmeter [cook-mesos scheduler rank-jobs-failures])

(defn filter-offensive-jobs
  "Base on the constraints on memory and cpus, given a list of job entities it
   puts the offensive jobs into offensive-job-ch asynchronically and returns
   the inoffensive jobs.

   A job is offensive if and only if its required memory or cpus exceeds the
   limits"
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
  [db offensive-job-filter]
  (timers/time!
    rank-jobs-duration
    (try
      (let [jobs (->> (sort-jobs-by-dru db)
                      ;; Apply the offensive job filter first before taking.
                      offensive-job-filter
                      ;; Limit number of jobs returned.
                      ;; This is merely defensive. It's very unlikely that we schedule more than 1024 jobs between two
                      ;; rank-jobs. Even if we do, this only introduces a delay of seconds to schedule thousands of jobs.
                      (take 1024))]
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
                        (rank-jobs (db conn) offensive-job-filter))))))

;;; Offer buffer Design
;;; The offer buffer is used to store mesos resource offers made to cook until the matcher is
;;; ready to use them.
;;;
;;; Reasons for doing this:
;;;  <> We can combine offers between offer sets
;;;  <> Provide the matcher with the best (biggest) offer of all possible offers currently ready
;;;  <> Lay the foundation for additional criteria before the offer is ready for the matcher
;;;  <> Centralize outstanding offers
;;;
;;; Structure:
;;; We have three components that do the heavy lifting.
;;;   1. offer-incubator: Accepts new offers from mesos and allows them to 'incubate',
;;;                       meaning we wait some amount of time before we consider the
;;;                       offer ready for the matcher. We delay the offers in the hopes
;;;                       of combining them with other offers from the same host. When offers
;;;                       are done incubating, we attempt to combine with other offers and
;;;                       pass the combined offers to the prioritizer.
;;;   2. offer-prioritizer: Accepts offers from the incubator that are ready and sorts them by size.
;;;                         Ensures the largest offer is passed to the offer-matcher when requested.
;;;   3. offer-matcher: Pulls offers from the offer-prioritizer and calls `handle-resource-offer!`
;;;
;;; Interesting design points:
;;; <> The incubator puts a timer on each offer set (call `incubate!`) and once the timer is up,
;;;    it puts the ids onto a channel to notify the incubator to attempt to combine offers and
;;;    pass along to the prioritizer.
;;; <> Once an offer has incubated, we take it, plus any offers that can combine with it out of
;;;    the buffer and put it onto the `offer-ready-chan`. This means we can be in a position where
;;;    a later timer fires and some of the offers are no longer in the buffer. We handle this by
;;;    using `select-keys` as a set intersection between offers we are considering and offers that
;;;    still exist in the buffer.
;;; <> To avoid race conditions, the design is fully linearized using `async/alt!`. The sole exception
;;;    is the incubate timers, however the firing of the timer is used only to notify that incubating
;;;    is complete. Since the work of moving the incubated offers to the next stage of the pipeline is
;;;    handled linearly through the `async/alt!` block, there is no concern of a race with the timers
;;;    as well.

(timers/deftimer [cook-mesos scheduler incubator-offer-received-duration])
(timers/deftimer [cook-mesos scheduler incubator-offer-incubated-duration])
(histograms/defhistogram [cook-mesos scheduler scheduler-offers-received])
(counters/defcounter [cook-mesos scheduler incubating-size])

(defn start-offer-incubator!
  "Starts a thread to accept offers from mesos and 'incubate' them.
   Offers that are ready are passed to the offer-ready-chan."
  [offer-chan offer-ready-chan incubate-ms]
  (let [ready-chan (async/chan)
        view-offers-atom (atom [])
        view-offers-fn (fn [] @view-offers-atom)
        incubate! (fn incubate! [ids]
                      (async/go
                        (when ids
                          (async/<! (async/timeout incubate-ms))
                          (async/>! ready-chan ids))))]
    (async/go
     (try
       (loop [incubating {}] ; key is offer-id, value is offer
         (reset! view-offers-atom
                (->> incubating
                     (vals)))
         (recur
            (async/alt!
              offer-chan ([[offers driver fid]] ; New offer
                          (timers/start-stop-time! ; Use this in go blocks, time! doesn't play nice
                            incubator-offer-received-duration
                            (let [annotated-offers (map (fn [offer]
                                                          (-> offer
                                                              (assoc :time-received (time/now)
                                                                     :driver driver
                                                                     :fid fid)
                                                              (update-in [:resources :cpus] #(or % 0.0))
                                                              (update-in [:resources :mem] #(or % 0.0))))
                                                        offers)
                                  ids (map :id offers)]
                              (incubate! ids)
                              (histograms/update!
                                scheduler-offers-received
                                (count ids))
                              (log/debug "Got " (count annotated-offers) " offers")
                              (log/debug "Size of incubating: " (+ (count annotated-offers) (count incubating)))
                              (counters/inc! incubating-size (count annotated-offers))
                              (apply assoc incubating (interleave ids annotated-offers)))))
              ready-chan ([offer-ids] ; Offer finished incubating
                          ; Can have < (count offer-ids) elements if they were combine previously
                          (timers/start-stop-time!
                            incubator-offer-incubated-duration
                            (let [matured-offers (vals (select-keys incubating offer-ids))
                                  combine-offer-groups (group-by combine-offer-key (vals incubating))
                                  matured-combine-offers (map #(-> %
                                                                   combine-offer-key
                                                                   combine-offer-groups
                                                                   combine-offers
                                                                   first)
                                                              matured-offers)
                                  ids-to-remove (mapcat :ids matured-combine-offers)]
                              (doseq [offer matured-combine-offers]
                              (async/>! offer-ready-chan offer))
                            (log/debug (count offer-ids) " matured. "
                                       (count ids-to-remove) " offers removed from incubating. "
                                       (- (count incubating) (count ids-to-remove)) " offers in incubating")
                            (counters/dec! incubating-size (count ids-to-remove))
                            (apply dissoc incubating ids-to-remove)))))))
        (catch Exception e
          (log/error e "In start-offer-incubator"))))
    view-offers-fn))

(counters/defcounter [cook-mesos scheduler prioritizer-buffer-size])

(defn start-offer-prioritizer!
  "Starts a thread to accept offers that are done incubating. Maintains a sorted
   buffer of these offers and passes the biggest one to the matcher when requested."
  [offer-ready-chan matcher-chan]
  (let [view-offers-atom (atom [])
        view-offers-fn (fn [] @view-offers-atom)]
    (async/go
     (try
       (loop [offers []]
         (reset! view-offers-atom
                 offers)
         (recur
          (if (seq offers)
            (do
              (log/debug (count offers) " offers in prioritizer buffer")
              (async/alt!
               offer-ready-chan ([offer]
                                   (log/debug "Got new offer. Size of prioritizer buffer: "
                                              (inc (count offers)))
                                   (counters/inc! prioritizer-buffer-size)
                                   (sort-offers (conj offers offer)))
               [[matcher-chan (first offers)]] (do
                                                 (log/debug "Offer sent to matcher. "
                                                            (count (rest offers))
                                                            " offers remaining")
                                                 (counters/dec! prioritizer-buffer-size)
                                                 (rest offers))))
            (let [offer (async/<! offer-ready-chan)]
              (log/debug "Got new offer. Size of prioritizer buffer: "
                         (inc (count offers)))
              (counters/inc! prioritizer-buffer-size)
              (sort-offers (conj offers offer))))))
       (catch Exception e
         (log/error e "In start-offer-prioritizer"))))
    view-offers-fn))

(defn start-offer-matcher!
  "Create a thread to repeatedly pull from matcher-chan and have the offer matched."
  [conn pending-jobs-atom matcher-chan]
  (async/thread
    (log/debug "offer matcher started")
    (try
      (loop []
        (log/debug "Preparing to pull offer (offer-matcher)")
        (let [offer (async/<!! matcher-chan)
              fid (:fid offer)
              driver (:driver offer)]
          (log/debug "(offer-matcher) Offer taken " offer)
          (handle-resource-offer! conn driver fid pending-jobs-atom offer))
        (log/debug "(offer matcher) Handled resource offer")
        (recur))
      (catch Exception e
        (log/error e "Error in start-offer-matcher!")))))

(meters/defmeter [cook-mesos scheduler offer-back-pressure])
(meters/defmeter [cook-mesos scheduler mesos-error])

(defn create-datomic-scheduler
  [conn set-framework-id pending-jobs-atom heartbeat-ch offer-incubate-time-ms task-constraints]
  (let [fid (atom nil)
        offer-chan (async/chan 100)
        offer-ready-chan (async/chan 100)
        matcher-chan (async/chan) ; Don't want to buffer offers to the matcher chan. Only want when ready
        view-incubating-offers (start-offer-incubator! offer-chan offer-ready-chan offer-incubate-time-ms)
        view-mature-offers (start-offer-prioritizer! offer-ready-chan matcher-chan)
        _ (start-offer-matcher! conn pending-jobs-atom matcher-chan)
        _ (start-jobs-prioritizer! conn pending-jobs-atom task-constraints)
        ]
    {:scheduler
     (mesos/scheduler
      (registered [driver framework-id master-info]
                  (log/info "Registered with mesos with framework-id " framework-id)
                  (reset! fid framework-id)
                  (set-framework-id framework-id)
                  (future
                    (try
                      (reconcile-jobs conn)
                      (reconcile-tasks (db conn) driver)
                      (catch Exception e
                        (log/error e "Reconciliation error")))))
      (reregistered [driver master-info]
                    (log/info "Reregisterd with new master")
                    (future
                      (try
                        (reconcile-jobs conn)
                        (reconcile-tasks (db conn) driver)
                        (catch Exception e
                          (log/error e "Reconciliation error")))))
      ;; Ignore this--we can just wait for new offers
      (offerRescinded [driver offer-id])
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
                      (case
                          (async/alt!!
                           [[offer-chan [offers driver @fid]]] (log/debug "Passed " (count offers) " to incubator")
                           :default :back-pressure
                           :priority true)
                        :back-pressure (do
                                         (log/warn "Back pressure on offer-incubator. Declining " (count offers) "offers")
                                         (meters/mark! offer-back-pressure)
                                         (decline-offers driver (map :id offers)))
                        nil))
      (statusUpdate [driver status]
                    (handle-status-update conn driver status)))
     :view-incubating-offers view-incubating-offers
     :view-mature-offers view-mature-offers}))
