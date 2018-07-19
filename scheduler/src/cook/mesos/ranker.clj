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
(ns cook.mesos.ranker
  (:require [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [cook.datomic :as datomic]
            [cook.mesos.dru :as dru]
            [cook.mesos.rebalancer :as rebalancer]
            [cook.mesos.share :as share]
            [cook.mesos.util :as util]
            [datomic.api :as d]
            [metrics.meters :as meters]
            [metrics.timers :as timers]
            [plumbing.core :as pc]))

(timers/deftimer [cook-mesos scheduler filter-offensive-jobs-duration])

(defn is-offensive?
  [max-memory-mb max-cpus job]
  (let [{memory-mb :mem
         cpus :cpus} (util/job-ent->resources job)]
    (or (> memory-mb max-memory-mb)
        (> cpus max-cpus))))

(defn filter-offensive-jobs
  "Base on the constraints on memory and cpus, given a list of job entities it
   puts the offensive jobs into offensive-job-ch asynchronously and returns
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
      ;; Put offensive jobs asynchronously such that it could return the
      ;; inoffensive jobs immediately.
      (async/go
        (when (seq offensive)
          (log/info "Found" (count offensive) "offensive jobs")
          (async/>! offensive-jobs-ch offensive)))
      inoffensive)))

(defn make-offensive-job-stifler
  "It returns an async channel which will be used to receive offensive jobs expected to be killed or aborted.
   It asynchronously pulls offensive jobs from the channel and abort these offensive jobs by marking job state as completed."
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
                (datomic/transact-with-retries conn
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
  ([unfiltered-db]
   (let [pending-job-ents (util/get-pending-job-ents unfiltered-db)
         pending-task-ents (map util/create-task-ent pending-job-ents)
         running-task-ents (util/get-running-task-ents unfiltered-db)]
     (sort-jobs-by-dru-category unfiltered-db pending-task-ents running-task-ents)))
  ([unfiltered-db pending-task-ents running-task-ents]
    ;; This function does not use the filtered db when it is not necessary in order to get better performance
    ;; The filtered db is not necessary when an entity could only arrive at a given state if it was already committed
    ;; e.g. running jobs or when it is always considered committed e.g. shares
    ;; The unfiltered db can also be used on pending job entities once the filtered db is used to limit
    ;; to only those jobs that have been committed.
   (let [job->category (comp util/categorize-job :job/_instance)
         category->pending-task-ents (group-by job->category pending-task-ents)
         category->running-task-ents (group-by job->category running-task-ents)
         user->dru-divisors (share/create-user->share-fn unfiltered-db nil)
         category->sort-jobs-by-dru-fn {:normal sort-normal-jobs-by-dru, :gpu sort-gpu-jobs-by-dru}]
     (letfn [(sort-jobs-by-dru-category-helper [[category sort-jobs-by-dru]]
               (let [pending-tasks (category->pending-task-ents category)
                     running-tasks (category->running-task-ents category)]
                 [category (sort-jobs-by-dru pending-tasks running-tasks user->dru-divisors)]))]
       (into {} (map sort-jobs-by-dru-category-helper) category->sort-jobs-by-dru-fn)))))

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
                      (pc/map-vals #(map util/job-ent->map %))
                      (pc/map-vals #(remove nil? %)))]
        (log/debug "Total number of pending jobs is:" (apply + (map count (vals jobs)))
                   "The first 20 pending normal jobs:" (take 20 (:normal jobs))
                   "The first 5 pending gpu jobs:" (take 5 (:gpu jobs)))
        jobs)
      (catch Throwable t
        (log/error t "Failed to rank jobs")
        (meters/mark! rank-jobs-failures)
        {}))))

(defn get-running-and-pending-tasks
  "Returns a map containing the running tasks, pending tasks, pending jobs and all tasks in the queue."
  [unfiltered-db]
  (let [pending-jobs (util/get-pending-job-ents unfiltered-db)
        pending-tasks (map util/create-task-ent pending-jobs)
        running-tasks (util/get-running-task-ents unfiltered-db)]
    {:all-tasks (concat pending-tasks running-tasks)
     :pending-jobs pending-jobs
     :pending-tasks pending-tasks
     :running-tasks running-tasks}))

(defn job->resources
  "Takes a job entity and returns a map of the resource usage of that job,
   specifically :cpus, :gpus (when available), and :mem."
  [job-ent]
  (select-keys (util/job-ent->resources job-ent) [:cpus :gpus :mem]))

(defn user->tasks-to-user->requested-resources
  "Converts user->tasks to user->requested-resources where resource usage is computed using job->resources."
  [user->tasks]
  (pc/map-vals
    (fn [tasks]
      (->> tasks
           (map :job/_instance)
           (map job->resources)
           (apply merge-with +)))
    user->tasks))

(defn compute-user->resource-weights
  "Computes the user->resource-weights based on the ratio of the user's share to the default share."
  [unfiltered-db users]
  (let [user->dru-divisors (share/create-user->share-fn unfiltered-db nil)
        default-dru-divisors (user->dru-divisors share/default-user)]
    (pc/map-from-keys #(merge-with / (user->dru-divisors %) default-dru-divisors) users)))

(defn compute-fair-share-resources
  "Returns a map containing the following:
   :ideal-running-job-ids which are the ids of all the jobs the fair scheduler thinks should be running,
   :user->allocated-resources which is a map summarizing the fair share resources allocated to each user."
  [total-resources user->requested-resources user->resource-weights]
  (let [default-resources (pc/map-from-keys (constantly 0.0) (keys total-resources))
        user->requested-resources (pc/map-vals #(merge default-resources %) user->requested-resources)]
    (loop [all-under-share-users #{}
           user->allocated-resources {}
           available-resources total-resources
           remaining-user->requested-resources user->requested-resources]
      (if (seq remaining-user->requested-resources)
        (let [resource-weights (select-keys
                                 (->> (select-keys user->resource-weights (keys remaining-user->requested-resources))
                                      vals
                                      (apply merge-with +))
                                 (keys available-resources))
              fair-share-resources-per-unit-weight (merge-with / available-resources resource-weights)
              user->fair-share-resources (pc/map-from-keys
                                           (fn [user]
                                             (->> (select-keys (user->resource-weights user) (keys available-resources))
                                                  (merge-with * fair-share-resources-per-unit-weight)))
                                           (keys remaining-user->requested-resources))
              under-share-users (filter (fn under-share? [user]
                                          (let [fair-share-resources (user->fair-share-resources user)
                                                requested-resources (remaining-user->requested-resources user)]
                                            (every? #(<= (get requested-resources %) (get fair-share-resources %))
                                                    (keys requested-resources))))
                                        (keys remaining-user->requested-resources))]
          (if (seq under-share-users)
            (let [under-share-user->allocated-resources (select-keys remaining-user->requested-resources under-share-users)
                  allocated-resources (apply merge-with + (vals under-share-user->allocated-resources))]
              (recur (into all-under-share-users under-share-users)
                     (merge user->allocated-resources under-share-user->allocated-resources)
                     (merge-with - available-resources allocated-resources)
                     (apply dissoc remaining-user->requested-resources under-share-users)))
            ;; find the closest fit user, allocated resources for her and continue
            (let [user->requested-resources->fit-rating (pc/map-from-keys
                                                          (fn compute-resource-fit-rating [user]
                                                            (let [requested-resources (user->requested-resources user)
                                                                  fair-share-resources (user->fair-share-resources user)]
                                                              (merge-with / requested-resources fair-share-resources)))
                                                          (keys remaining-user->requested-resources))
                  user->fit-rating (pc/map-vals #(reduce max (vals %)) user->requested-resources->fit-rating)
                  [user _] (reduce (fn [[u1 v1] [u2 v2]] (if (<= v1 v2) [u1 v1] [u2 v2]))
                                   (seq user->fit-rating))
                  allocated-resources (merge-with min (user->requested-resources user) (user->fair-share-resources user))]
              (recur all-under-share-users
                     (assoc user->allocated-resources user allocated-resources)
                     (merge-with - available-resources allocated-resources)
                     (dissoc remaining-user->requested-resources user)))))
        {:under-share-users all-under-share-users
         :user->allocated-resources user->allocated-resources}))))

(defn allocate-tasks
  "Given the user's fair share resources, return the map of user to job ids that should be scheduled.
   The scheduled jobs are traversed in sorted order and considered eligible for scheduling as long as
   the user's cumulative resource usage is under the user's fair share of resources."
  [user->sorted-tasks user->fair-share-resources under-share-users]
  (pc/map-from-keys
    (fn user->scheduled-job-ids [user]
      (if (contains? under-share-users user)
        (do
          (log/debug user "scheduling all" (count (user->sorted-tasks user)) "jobs of" user)
          (->> (user->sorted-tasks user)
               (map #(-> % :job/_instance :db/id))
               (into #{})))
        (let [fair-share-resources (user->fair-share-resources user)
              zero-resources (pc/map-vals (constantly 0) fair-share-resources)]
          (loop [scheduled-job-ids #{}
                 allocated-resources zero-resources
                 [task & remaining-tasks] (user->sorted-tasks user)]
            (if (and task
                     (every? #(< (get allocated-resources %) (get fair-share-resources %))
                             (keys fair-share-resources)))
              (recur (conj scheduled-job-ids (-> task :job/_instance :db/id))
                     (merge-with + allocated-resources (-> task :job/_instance job->resources))
                     remaining-tasks)
              (do
                (log/debug "skipping scheduling" (inc (count remaining-tasks)) "jobs of" user)
                scheduled-job-ids))))))
    (keys user->sorted-tasks)))

(defn fairly-sort-jobs-by-dru-category
  "Computes the fair schedule of jobs that should be running for a given user.
   Returns a map containing the following keys:
   :ideal-scheduled-job-ids is the set of job ids that should be running as per the
   fair share allocation, and
   :category->sorted-pending-jobs the pending jobs by category that should currently be scheduled as
   per the fair share allocation."
  [total-resources unfiltered-db]
  (let [{:keys [all-tasks pending-tasks running-tasks]} (get-running-and-pending-tasks unfiltered-db)
        user->tasks (group-by #(-> % :job/_instance :job/user) all-tasks)
        user->requested-resources (user->tasks-to-user->requested-resources user->tasks)
        user->resource-weights (compute-user->resource-weights unfiltered-db (keys user->tasks))
        {:keys [under-share-users user->allocated-resources]}
        (compute-fair-share-resources total-resources user->requested-resources user->resource-weights)
        task-comparator (util/same-user-task-comparator)
        user->sorted-tasks (pc/map-vals #(sort task-comparator %) user->tasks)
        user->scheduled-job-ids (allocate-tasks user->sorted-tasks user->allocated-resources under-share-users)
        scheduled-job-ids (reduce set/union #{} (vals user->scheduled-job-ids))
        scheduled-pending-tasks (filter #(contains? scheduled-job-ids (-> % :job/_instance :db/id)) pending-tasks)
        category->sorted-pending-jobs (sort-jobs-by-dru-category unfiltered-db scheduled-pending-tasks running-tasks)]
    (log/info "after fairness processing"
              {:pending-jobs {:scheduled (count scheduled-pending-tasks)
                              :total (count pending-tasks)}})
    {:category->sorted-pending-jobs category->sorted-pending-jobs
     :ideal-scheduled-job-ids scheduled-job-ids}))

(def total-resources-atom
  "The total resources available to the scheduler, initialized to zero resources."
  (atom {:cpus 0 :mem 0}))

(defn- retrieve-total-resources
  "Returns the total resources available to the scheduler.
   Throws an error if the total resources have not been initialized."
  []
  (let [total-resources @total-resources-atom]
    (when (= {:cpus 0 :mem 0} total-resources)
      (throw (ex-info "total-resources has not been initialized!" {:total-resources total-resources})))
    total-resources))

(defn fairly-sort-jobs-by-dru-category-wrapper
  "Returns a map from job category to a list of job entities, ordered by dru that should
   be scheduled as per the fair share allocation."
  [unfiltered-db]
  (let [total-resources (retrieve-total-resources)
        {:keys [category->sorted-pending-jobs ideal-scheduled-job-ids]}
        (fairly-sort-jobs-by-dru-category total-resources unfiltered-db)]
    ;; communicate the expected running jobs to the rebalancer
    (reset! rebalancer/non-preemptable-scored-task?-atom
            (fn non-preemptable-scored-task?
              [{:keys [task]}]
              (contains? ideal-scheduled-job-ids (-> task :job/_instance :db/id))))
    ;; return the category->sorted-pending-jobs
    category->sorted-pending-jobs))

(defn fairness-based-rank-jobs
  "Return a map of lists of job entities ordered by dru, keyed by category that should
   be scheduled as per the fair share allocation.
   It ranks the jobs by dru first and then apply several filters if provided."
  [unfiltered-db offensive-job-filter]
  (timers/time!
    rank-jobs-duration
    (try
      (->> (fairly-sort-jobs-by-dru-category-wrapper unfiltered-db)
           ;; Apply the offensive job filter first before taking.
           (pc/map-vals offensive-job-filter)
           (pc/map-vals #(map util/job-ent->map %))
           (pc/map-vals #(remove nil? %)))
      (catch Throwable t
        (log/error t "Failed to rank jobs")
        (meters/mark! rank-jobs-failures)
        {}))))

(defn start-jobs-prioritizer!
  [conn pending-jobs-atom task-constraints trigger-chan rank-jobs-fn]
  (let [offensive-jobs-ch (make-offensive-job-stifler conn)
        offensive-job-filter (partial filter-offensive-jobs task-constraints offensive-jobs-ch)]
    (util/chime-at-ch trigger-chan
                      (fn rank-jobs-event []
                        (reset! pending-jobs-atom
                                (rank-jobs-fn (d/db conn) offensive-job-filter))))))
