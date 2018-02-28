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
(ns cook.mesos.rebalancer
  (:require [chime :refer [chime-at]]
            [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clojure.data.priority-map :as pm]
            [clojure.tools.logging :as log]
            [clojure.walk :refer (keywordize-keys)]
            [cook.mesos.constraints :as constraints]
            [cook.mesos.dru :as dru]
            [cook.mesos.share :as share]
            [cook.mesos.util :as util]
            [datomic.api :as d :refer (q)]
            [mesomatic.scheduler :as mesos]
            [metatransaction.core :as mt]
            [metrics.histograms :as histograms]
            [metrics.timers :as timers]
            [plumbing.core :refer [map-keys]]
            [swiss.arrows :refer :all]))

;;; Design
;;; Rebalancer is designed to run independently of the scheduler. Its primary functionality is to detect that
;;; the cluster is unbalanced, and if so, to balance the cluster by performing preemption of lesser prioirty tasks.
;;;
;;; Definitions
;;; The cluster is balanced when the allocated resources for each user are above or close to its weighted fair share
;;; allocation; similarly, the cluster is unbalanced when there exists a user whose allocated resources are below and
;;; not close to its weighted fair share allocation.
;;;
;;; To preempt a task is to stop and requeue a task.
;;;
;;; A job is a work request, whereas a task is a realization of a job. A job can have many tasks. A job can be in one
;;; of three states: pending; running; completed, encompassing both success and failure states. A task can be in one of
;;; two states: running or completed, again encompassing both success and failure states.
;;;
;;; Lifecycle of Rebalancer
;;; Rebalancer runs periodically with a fixed time period. Each period consists of a single cycle, which consists of
;;; multiple iterations.
;;;
;;; At the start of a cycle, Rebalancer initializes its internal state. Then, for each iteration in a given cycle,
;;; Rebalancer processes a pending job and tries to make room for it by finding a task to preempt and updates its
;;; internal state if such preemption is found.
;;;
;;; Preemption Principle
;;; Rebalancer uses a score-based preemption algorithm.
;;;
;;; Each running task and pending job has a score. The higher the score, the more important the task/job is. The
;;; preemption principle is to preempt low score running tasks to make room for high score pending jobs. In this
;;; implementation, score is defined as the negative of dominant resource usage (DRU). The higher the DRU, the more
;;; likely it is a task will be preempted.
;;;
;;; Dominant Resource Usage (DRU)
;;; The idea of DRU is introduced in https://www.cs.berkeley.edu/~alig/papers/drf.pdf. In the paper, the DRU of a user
;;; is defined as
;;;
;;;                           (max (/ used-mem total-mem)   (/ used-cpus total-cpus)).
;;;
;;; We slightly modify it to be
;;;
;;;                           (max (/ used-mem mem-divisor) (/ used-cpus cpus-divisor)).
;;;
;;; We introduce mem-divisor and cpus-divisor because it gives us the flexibility to change how individual resources
;;; impact score. For instance, if we believe our cluster is memory bound, we can set mem-divisor to (* 0.1 total-mem)
;;; and cpus-divisor to (* 0.3 total-cpus) to encode our belief that memory 3x is as precious as cpu. Conveniently, we
;;; can set each respective divisor to per user per resource share, which gives us a weighted DRU.
;;;
;;; DRU of a Running Task
;;; Assume there is a per user ordering of tasks. We define DRU of a task to be the resource sum of this task and all
;;; tasks before it divided by the respecitve DRU divisor.
;;;
;;; For instance, suppose user A has ordered running tasks
;;;
;;;                        [{:task taskA :mem 10.0 :cpus 2.0} {:task taskB :mem 5.0 :cpus 1.0}].
;;;
;;; Assuming both DRU divisors are 100.0, then the corresponding DRUs are
;;;
;;;                        [{:task taskA :dru 0.1}            {:task taskB :dru 0.15}].
;;;
;;; The DRU of user A's tasks is a function of A's running tasks and DRU divisors. It is not affected by other users'
;;; running tasks.
;;;
;;; DRU of a Pending Job
;;; We define the DRU of a pending job to be the CUMULATIVE DRU of the running task of this pending job, were it to be launched.
;;;
;;; Continuing with the example above, suppose user A has jobs jobA, jobB, and jobC, where jobA and jobB are running,
;;; as implied above, and jobC is pending. jobC is
;;;
;;;                                       {:job jobC :mem 17.0 :cpus 3.0}
;;;
;;; and User A's task ordering function specifies that were jobC to be launched as taskC, then user A's new ordered
;;; running tasks would be
;;;
;;;         [{:task taskA :mem 10.0 :cpus 2.0} {:task taskC :mem 17.0 :cpus 3.0} {:task taskB :mem 5.0 :cpus 1.0}],
;;;
;;; which would imply that taskC would have a DRU of 0.27.
;;;
;;; For GPU preemption, the functionality works exactly the same as above, with one change: instead of computing the DRU
;;; as (max (/ used-mem mem-divisor) (/ used-cpus cpu-divisor)), we compute the DRU as (/ used-gpus gpu-divisor). All of
;;; the different code paths reflect this change, or the fact that GPU scored task pairs are [task cumulative-gpus] rather
;;; than [task scored-task], as we don't need the additional data for computing the GPU preemption.
;;;
;;; Parameters
;;; safe-dru-threshold: Task with a DRU lower than safe-dru-threshold will not be preempted. If each DRU divisor is set
;;;                     to the corresponding per user share and safe-dru-threshold is set to 1.0, then tasks that
;;;                     consume resources in aggregate less than the user resource share will not be preempted.
;;;
;;; min-dru-diff: The minimal DRU difference required to make a preemption action. This is also the maximal 'unfairness'
;;;               Rebalancer is willing to tolerate.
;;;
;;; max-preemption: The maximum number of preemptions Rebalancer can make in one cycle.
;;; min-utilization-threshold: The minimal cluster utilization to trigger rebalancer. The idea is that the rebalancer should only run when the cluster is at high utilization. If the cluster is not at high utilization, its available resources should be used first before we perform any preemption.

;;; Before you read the code...Here are something you should know about
;;;
;;; Naming
;;; A lot of the functions in this namespace takes the data structure in the old state and compute the data structure
;;; in the new state. "'" is used to indicate data structure in the new state.
;;;
;;; Schema
;;; job-resources {:mem Double :cpus Double}
;;; spare-resources {:mem Double :cpus Double}
;;; dru-divisors {:mem Double :cpus Double}
;;; scored-task {:task task-ent :dru Double :mem Double :cpus Double}
;;; preemption-decision {:hostname String :task [task-ent] :dru Double :mem Double :cpus Double}
;;; preemption-candidates [{:task task-ent :dru Double :mem Double :cpus Double}]

(defrecord State [task->scored-task user->sorted-running-task-ents host->spare-resources user->dru-divisors compute-pending-job-dru preempted-tasks])

(timers/deftimer [cook-mesos rebalancer rebalance-duration])
(timers/deftimer [cook-mesos rebalancer compute-preemption-decision-duration])

(histograms/defhistogram [cook-mesos rebalancer pending-job-drus])
(histograms/defhistogram [cook-mesos rebalancer nearest-task-drus])
(histograms/defhistogram [cook-mesos rebalancer preemption-counts-for-host])
(histograms/defhistogram [cook-mesos rebalancer positive-dru-diffs])
(histograms/defhistogram [cook-mesos rebalancer task-counts-to-preempt])
(histograms/defhistogram [cook-mesos rebalancer job-counts-to-run])

;; If running on a cluster with very small DRU values,
;; may need to change this scale to facilitate metrics to e.g. (math/expt 10 305)
(def ^:dynamic  metrics-dru-scale 1)
(defn dru-at-scale
  [dru]
  (* dru metrics-dru-scale))

(defn compute-pending-gpu-job-dru
  "Takes state and a pending gpu job entity, returns the dru of the pending-job.

   This algorithm only should be used on jobs that use GPUs. It computes the GPU DRU."
  [{:keys [task->scored-task user->sorted-running-task-ents user->dru-divisors] :as state}
   pending-job-ent]
  (let [user (:job/user pending-job-ent)
        {gpu-req :gpus} (util/job-ent->resources pending-job-ent)
        gpu-divisor (-> user user->dru-divisors :gpus)
        pending-task-ent (util/create-task-ent pending-job-ent)
        nearest-task-ent (some-> user->sorted-running-task-ents
                                 (get user)
                                 (rsubseq <= pending-task-ent)
                                 (first))
        nearest-task-dru (if nearest-task-ent
                           (get task->scored-task nearest-task-ent)
                           0.0)
        pending-job-dru (+ nearest-task-dru (/ gpu-req gpu-divisor))]
    (histograms/update! pending-job-drus (dru-at-scale pending-job-dru))
    (histograms/update! nearest-task-drus (dru-at-scale nearest-task-dru))

    pending-job-dru))

(defn compute-pending-normal-job-dru
  "Takes state and a pending job entity, returns the dru of the pending-job. In the case where the pending job causes user's dominant
   resource type to change, the dru is not accurate and is only a upper bound. However, this inaccuracy won't affect the correctness
   of the algorithm.

   This algorithm only should be used on jobs that use cpu & mem, not gpus. It computes the cpu/mem DRU."
  [{:keys [task->scored-task user->sorted-running-task-ents user->dru-divisors] :as state}
   pending-job-ent]
  (let [user (:job/user pending-job-ent)
        {mem-req :mem cpus-req :cpus} (util/job-ent->resources pending-job-ent)
        {mem-divisor :mem cpus-divisor :cpus} (user->dru-divisors user)
        pending-task-ent (util/create-task-ent pending-job-ent)
        nearest-task-ent (some-> user->sorted-running-task-ents
                                 (get user)
                                 (rsubseq <= pending-task-ent)
                                 (first))
        nearest-task-dru (if nearest-task-ent
                           (get-in task->scored-task [nearest-task-ent :dru])
                           0.0)
        pending-job-dru (max (+ nearest-task-dru (/ mem-req mem-divisor))
                             (+ nearest-task-dru (/ cpus-req cpus-divisor)))]
    (histograms/update! pending-job-drus (dru-at-scale pending-job-dru))
    (histograms/update! nearest-task-drus (dru-at-scale nearest-task-dru))

    pending-job-dru))

(defn init-state
  "Initializes state. State consists of:
   task->scored-task A priority from task entities to ScoredTasks, sorted from high dru to low dru
   user->sorted-running-task-ents A map from user to a sorted set of running task entities, from high value to low
   value
   host->spare-resources A map from host to spare resources.
   user->dru-divisors A map from user to dru divisors."
  ([db running-task-ents pending-job-ents host->spare-resources category]
   (init-state db running-task-ents pending-job-ents host->spare-resources category []))
  ([db running-task-ents pending-job-ents host->spare-resources category preempted-tasks]
   (let [running-task-ents (filter (fn [task]
                                     (-> task
                                         :job/_instance
                                         util/categorize-job
                                         (= category)))
                                   running-task-ents)
         user->dru-divisors (dru/init-user->dru-divisors db running-task-ents pending-job-ents)
         user->sorted-running-task-ents (->> running-task-ents
                                             (group-by util/task-ent->user)
                                             (map (fn [[user task-ents]]
                                                    [user (into (sorted-set-by (util/same-user-task-comparator))
                                                                task-ents)]))
                                             (into {}))
         scored-task-pairs (case category
                             :normal (dru/sorted-task-scored-task-pairs user->dru-divisors user->sorted-running-task-ents)
                             :gpu (dru/sorted-task-cumulative-gpu-score-pairs user->dru-divisors user->sorted-running-task-ents))
         task->scored-task (into (pm/priority-map-keyfn (case category
                                                          :normal (juxt (comp - :dru)
                                                                        (comp util/task-ent->user :task))
                                                          :gpu (fnil - 0)))
                                 scored-task-pairs)]
     (->State task->scored-task
              user->sorted-running-task-ents
              host->spare-resources
              user->dru-divisors
              (case category
                :normal compute-pending-normal-job-dru
                :gpu compute-pending-gpu-job-dru)
              preempted-tasks))))



(defn next-state
  "Takes state, a pending job entity to launch and a preemption decision, returns the next state"
  [{:keys [task->scored-task user->sorted-running-task-ents host->spare-resources user->dru-divisors compute-pending-job-dru preempted-tasks] :as state}
   pending-job-ent
   preemption-decision]
  {:pre [(not (nil? preemption-decision))]}
  (let [hostname (:hostname preemption-decision)
        slave-id (-> preemption-decision :task first :instance/slave-id)
        {mem-req :mem cpus-req :cpus gpus-req :gpus} (util/job-ent->resources pending-job-ent)
        new-running-task-ent (util/create-task-ent pending-job-ent :hostname hostname :slave-id slave-id)
        preempted-task-ents (:task preemption-decision)
        changed-users (->> (conj preempted-task-ents new-running-task-ent)
                           (map util/task-ent->user)
                           (into #{}))

        ;; Compute next state
        user->dru-divisors' user->dru-divisors
        user->sorted-running-task-ents'
        (reduce (fn [task-ents-by-user task-ent]
                  (let [user (util/task-ent->user task-ent)
                        f (if (= new-running-task-ent task-ent)
                            (fnil conj (sorted-set-by (util/same-user-task-comparator)))
                            disj)]
                    (update-in task-ents-by-user [user] f task-ent)))
                user->sorted-running-task-ents
                (conj preempted-task-ents new-running-task-ent))
        task->scored-task' (dru/next-task->scored-task task->scored-task
                                                       user->sorted-running-task-ents
                                                       user->sorted-running-task-ents'
                                                       user->dru-divisors'
                                                       changed-users)
        host->spare-resources' (assoc host->spare-resources hostname
                                      {:mem (- (:mem preemption-decision) mem-req)
                                       :gpus (- (:gpus preemption-decision 0.0) (or gpus-req 0.0))
                                       :cpus (- (:cpus preemption-decision) cpus-req)})
        preempted-tasks' (into preempted-tasks preempted-task-ents)
        state' (->State task->scored-task' user->sorted-running-task-ents' host->spare-resources' user->dru-divisors' compute-pending-job-dru preempted-tasks')]
    state'))

(defn exceeds-min-diff?
  [pending-job-dru min-dru-diff task]
  (let [diff (- (:dru task) pending-job-dru)]
    (if (pos? diff)
      (histograms/update! positive-dru-diffs (dru-at-scale diff)))
    (> diff min-dru-diff)))

(defn compute-preemption-decision
  "Takes state, parameters and a pending job entity, returns a preemption decision
   A preemption decision is a map that describes a possible way to perform preemption on a host. It has a hostname, a seq of tasks
   to preempt and available mem and cpus on the host after the preemption."
  [db offer-cache
   {:keys [task->scored-task host->spare-resources compute-pending-job-dru preempted-tasks] :as state}
   {:keys [min-dru-diff safe-dru-threshold] :as params}
   pending-job-ent
   cotask-cache]
  (timers/time!
   compute-preemption-decision-duration
   (let [{pending-job-mem :mem pending-job-cpus :cpus pending-job-gpus :gpus} (util/job-ent->resources pending-job-ent)
         pending-job-dru (compute-pending-job-dru state pending-job-ent)
         ;; This will preserve the ordering of task->scored-task
         host->scored-tasks (->> task->scored-task
                                 vals
                                 (remove #(< (:dru %) safe-dru-threshold))
                                 (filter (partial exceeds-min-diff? pending-job-dru min-dru-diff))
                                 (group-by (fn [{:keys [task]}]
                                             (:instance/hostname task))))

         host->formatted-spare-resources (->> host->spare-resources
                                              (map (fn [[host {:keys [mem cpus gpus]}]]
                                                     [host [{:dru Double/MAX_VALUE :task nil :mem mem :cpus cpus :gpus gpus}]]))
                                              (into {}))

         job-constraints (constraints/make-rebalancer-job-constraints
                           pending-job-ent (partial util/get-slave-attrs-from-cache offer-cache))
         group-constraints (->> pending-job-ent
                                :group/_job
                                (map #(constraints/make-rebalancer-group-constraint
                                        db
                                        %
                                        (partial util/get-slave-attrs-from-cache offer-cache)
                                        preempted-tasks
                                        cotask-cache))
                                (remove nil?))
         constraints (into (vec job-constraints) group-constraints)

         preemptable-host->slave-id (->> task->scored-task
                                         keys
                                         (map (juxt :instance/hostname :instance/slave-id))
                                         (into {}))

         passes-constraints? (fn [hostname] (every? #(% pending-job-ent (get preemptable-host->slave-id hostname))
                                                    constraints))
         ;; Get all the unconstrained slaves and hosts
         ;; Here we do a greedy search instead of bin packing. A preemption decision contains a prefix of scored
         ;; tasks on a specific host.
         ;; We try to find a preemption decision where the minimum dru of tasks to be preempted is maximum
         preemption-decision (->> (merge-with into host->formatted-spare-resources host->scored-tasks)
                                  ; Only evaluate tasks in unconstrained slaves
                                  (filter (comp passes-constraints? key))
                                  (sort-by first)
                                  (mapcat (fn compute-aggregations [[host scored-tasks]]
                                            (rest
                                             (reductions
                                              (fn aggregate-scored-tasks [aggregation {:keys [dru task mem cpus gpus] :as scored-task}]
                                                {:hostname host
                                                 :dru dru
                                                 :task (if task
                                                         (conj (:task aggregation) task)
                                                         (:task aggregation))
                                                 :gpus (+ (:gpus aggregation) (or gpus 0.0))
                                                 :mem (+ (:mem aggregation) (or mem 0))
                                                 :cpus (+ (:cpus aggregation) (or cpus 0))})
                                              {:hostname host :task nil :mem 0.0 :cpus 0.0 :gpus 0.0}
                                              scored-tasks))))
                                  (filter (fn has-enough-resource [resource-sum]
                                            (and (>= (:mem resource-sum) pending-job-mem)
                                                 (>= (:cpus resource-sum) pending-job-cpus)
                                                 (if pending-job-gpus
                                                   (>= (:gpus resource-sum) pending-job-gpus)
                                                   true))))
                                  (apply max-key (fnil :dru {:dru 0.0}) nil))]
     (histograms/update! preemption-counts-for-host (-> preemption-decision :tasks count))
     preemption-decision)))

(defn compute-next-state-and-preemption-decision
  "Takes state, params and a pending job entity, returns new state and preemption decision"
  [db offer-cache state params pending-job cotask-cache]
  (log/debug "Trying to find space for: " pending-job)
  (if-let [preemption-decision (compute-preemption-decision db offer-cache state params pending-job cotask-cache)]
    [(next-state state pending-job preemption-decision)
     (assoc preemption-decision
            :to-make-room-for pending-job)]
    [state nil]))

(defn reserve-hosts!
  "Reserves all hosts in preemption-decisions which will preempt more than one task"
  [rebalancer-reservation-atom preemption-decisions]
  (let [multiple-task-decisions (filter #(< 1 (count (:task %))) preemption-decisions)
        reservations (->> multiple-task-decisions
                          (map (fn [d] [(:job/uuid (:to-make-room-for d))
                                        (:hostname d)]))
                          (into {}))]
    (swap! rebalancer-reservation-atom (fn [{:keys [launched-job-uuids]}]
                                        ; In case one of the jobs the rebalancer has decided to reserve a host for
                                        ; launched while computing the pre-emption decisions, remove it's
                                        ; reservation from the map. Then we can clear the launched-job-uuids set.
                                         {:job-uuid->reserved-host (apply dissoc reservations launched-job-uuids)
                                          :launched-job-uuids #{}}))))

(defn rebalance
  "Takes a db, a list of pending job entities, a map of spare resources and params.
   Returns a list of pending job entities to run and a list of task entities to preempt

   category is :normal or :gpu, depending on which type of job we're working with"
  [db offer-cache pending-job-ents host->spare-resources rebalancer-reservation-atom
   {:keys [max-preemption category] :as params}]
  (let [timer (timers/start rebalance-duration)
        jobs-to-make-room-for (->> pending-job-ents
                                   (filter (partial util/job-allowed-to-start? db))
                                   (take max-preemption))
        init-state (init-state db (util/get-running-task-ents db) jobs-to-make-room-for host->spare-resources category)
        cotask-cache (atom (cache/lru-cache-factory {} :threshold (max 1 max-preemption)))]
    (log/debug "Jobs to make room for:" jobs-to-make-room-for)
    (loop [state init-state
           remaining-preemption max-preemption
           [pending-job-ent & jobs-to-make-room-for] jobs-to-make-room-for
           preemption-decisions []]
      (if (and pending-job-ent (pos? remaining-preemption))
        (let [[state' preemption-decision] (compute-next-state-and-preemption-decision db offer-cache state params pending-job-ent cotask-cache)]
          (if preemption-decision
            (recur state'
                   (dec remaining-preemption)
                   jobs-to-make-room-for
                   (conj preemption-decisions preemption-decision))
            (recur state'
                   remaining-preemption
                   jobs-to-make-room-for
                   preemption-decisions)))
        (do
          (timers/stop timer)
          (histograms/update! task-counts-to-preempt (count (mapcat :task preemption-decisions)))
          (histograms/update! job-counts-to-run (count preemption-decisions))
          (reserve-hosts! rebalancer-reservation-atom preemption-decisions)
          preemption-decisions)))))

(defn- prep-job-ent-for-printing
  [job-ent]
  (merge (select-keys job-ent 
                      [:db/id :job/uuid :job/user])
         (util/job-ent->resources job-ent)))

(defn- prep-task-ent-for-printing
  [task-ent]
  (let [job-ent (:job/_instance task-ent)]
    (-> task-ent
        (select-keys [:db/id :instance/task-id])
        (assoc :job (prep-job-ent-for-printing job-ent)))))



(defn rebalance!
  [conn driver offer-cache pending-job-ents host->spare-resources rebalancer-reservation-atom
   params]
  (try
    (log/info "Rebalancing...Params:" params)
    (let [db (mt/db conn)
          preemption-decisions (rebalance db offer-cache pending-job-ents host->spare-resources rebalancer-reservation-atom params)]
      (doseq [{job-ent-to-make-room-for :to-make-room-for
               task-ents-to-preempt :task} preemption-decisions]
        ;; Ensure that uuids are loaded in entity
        (:job/uuid job-ent-to-make-room-for)
        (doall (map :instance/task-id task-ents-to-preempt))

        (log/info "Preempting tasks to make room for waiting job"
                  {:to-make-room-for (prep-job-ent-for-printing job-ent-to-make-room-for)
                   :to-preempt (map prep-task-ent-for-printing task-ents-to-preempt)})
        
        ;; If a task has no id, it must be a synthetic task.
        ;; This means that on one iteration of (compute-next-state-and-preemption-decision),
        ;; the rebalancer decided to make room for a certain hypothetical task,
        ;; but on a subsequent iteration, it became clear that OTHER hypothetical tasks would be an even better outcome.
        ;; We shouldn't try to actually preempt tasks that were never scheduled.
        ;; TODO : We should probably move this filter into rebalance in the future
        ;;        however it is here now to allow us to audit how frequently a synthetic
        ;;        task is "preempted"
        (doseq [task-ent (filter :db/id task-ents-to-preempt)]
          (try
            @(d/transact
               conn
               ;; Make :instance/status and :instance/preempted? consistent to simplify the state machine.
               ;; We don't want to deal with {:instance/status :instance.status/running, :instance/preempted? true}
               ;; all over the place.
               (let [job-eid (:db/id (:job/_instance task-ent))
                     task-eid (:db/id task-ent)]
                 [[:generic/ensure task-eid :instance/status (d/entid db :instance.status/running)]
                  [:generic/atomic-inc job-eid :job/preemptions 1]
                  ;; The database can become inconsistent if we make multiple calls to :instance/update-state in a single
                  ;; transaction; see the comment in the definition of :instance/update-state for more details
                  [:instance/update-state task-eid :instance.status/failed [:reason/name :preempted-by-rebalancer]]
                  [:db/add task-eid :instance/reason [:reason/name :preempted-by-rebalancer]]
                  [:db/add task-eid :instance/preempted? true]]))
            (catch Throwable e
              (log/warn e "Failed to transact preemption")))
          (when-let [task-id (:instance/task-id task-ent)]
            (mesos/kill-task! driver {:value task-id})))))))



(def datomic-params [:max-preemption
                     :min-dru-diff
                     :min-utilization-threshold
                     :safe-dru-threshold])

(defn read-datomic-params
  [conn]
  (-<>>
   (d/pull (mt/db conn) ["*"] :rebalancer/config)
   (dissoc <> ":db/id" ":db/ident")
   (map-keys #(keyword (name (keyword %))))))

(defn update-datomic-params-from-config!
  [conn config]
  (let [recognized-params (select-keys config datomic-params)]
    (when (not-empty recognized-params)
      (log/info "Updating rebalancer params to" recognized-params)
      @(d/transact
         conn
         [(into
            {:db/id :rebalancer/config}
            (map-keys
              #(keyword "rebalancer.config" (name %))
              recognized-params))]))))

(defn start-rebalancer!
  [{:keys [config conn driver get-mesos-utilization offer-cache pending-jobs-atom
           rebalancer-reservation-atom trigger-chan view-incubating-offers]}]
  (binding [metrics-dru-scale (:dru-scale config)]
    (update-datomic-params-from-config! conn config)
    (util/chime-at-ch
      trigger-chan
      (fn trigger-rebalance-iteration []
        (log/info "Rebalance cycle starting")
        (let [{:keys [min-utilization-threshold] :as params} (read-datomic-params conn)
              utilization (get-mesos-utilization)
              host->spare-resources (->> (view-incubating-offers)
                                         (map (fn [v]
                                                [(:hostname v)
                                                 (select-keys (keywordize-keys (:resources v))
                                                              [:cpus :mem :gpus])]))
                                         (into {}))]
          (if (and (seq params)
                   min-utilization-threshold
                   (> utilization min-utilization-threshold))
            (let [{normal-pending-jobs :normal gpu-pending-jobs :gpu} @pending-jobs-atom]
              (rebalance! conn driver offer-cache normal-pending-jobs host->spare-resources
                          rebalancer-reservation-atom
                          (assoc params :category :normal
                                        :compute-pending-job-dru compute-pending-normal-job-dru))
              (rebalance! conn driver offer-cache gpu-pending-jobs host->spare-resources
                          rebalancer-reservation-atom
                          (assoc params :category :gpu
                                        :compute-pending-job-dru compute-pending-gpu-job-dru)))
            (log/info "Skipping rebalancing due to low cluster utilization"
                      {:mesos-utilization (str utilization)
                       :min-utilization-threshold (str min-utilization-threshold)}))))
      {:error-handler (fn [ex] (log/error ex "Rebalance failed"))})
    #(async/close! trigger-chan)))

(comment
  ; Useful function to simulate preemptions
  (defn update-task-by-name
    [name reason-name]
    (let [conn (d/connect "datomic:mem://mesos-jobs")
        running (ffirst (q '[:find ?status
                         :in $ ?ident
                         :where
                         [?status :db/ident ?ident]
                         ] (d/db conn) :instance.status/running))
        task-eid (ffirst (q '[:find ?inst
                         :in $ ?name ?status
                         :where
                         [?j :job/name ?name]
                         [?j :job/instance ?inst]
                         [?inst :instance/status ?status]
                         ] (d/db conn) name running))
        ]
    @(d/transact
        conn
        [;; The database can become inconsistent if we make multiple calls to :instance/update-state in a single
         ;; transaction; see the comment in the definition of :instance/update-state for more details
         [:instance/update-state task-eid :instance.status/failed [:reason/name reason-name]]
         [:db/add task-eid :instance/reason [:reason/name reason-name]]
         [:db/add task-eid :instance/preempted? true]
         ])))

  (update-task-by-name "sometask" :unknown)
  (update-task-by-name "sometask" :preempted-by-rebalancer)

  (let [conn (d/connect "datomic:mem://mesos-jobs")]
    (share/set-share! conn "default" :cpus 20.0 :mem 2500000.0))

  (let [conn (d/connect "datomic:mem://mesos-jobs")
        db (d/db conn)]
    @(d/transact conn [{:db/id :rebalancer/config
                        :rebalancer.config/max-preemption 64.0
                        :rebalancer.config/min-dru-diff 0.0000000001
                        :rebalancer.config/min-utilization-threshold 0.0
                        :rebalancer.config/safe-dru-threshold 0.0}])))
