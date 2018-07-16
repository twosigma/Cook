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
            [clojure.tools.logging :as log]
            [cook.datomic :as datomic]
            [cook.mesos.dru :as dru]
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
        user->dru-divisors (share/create-user->share-fn unfiltered-db nil)
        category->sort-jobs-by-dru-fn {:normal sort-normal-jobs-by-dru, :gpu sort-gpu-jobs-by-dru}]
    (letfn [(sort-jobs-by-dru-category-helper [[category sort-jobs-by-dru]]
              (let [pending-tasks (category->pending-task-ents category)
                    running-tasks (category->running-task-ents category)]
                [category (sort-jobs-by-dru pending-tasks running-tasks user->dru-divisors)]))]
      (into {} (map sort-jobs-by-dru-category-helper) category->sort-jobs-by-dru-fn))))

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

(defn start-jobs-prioritizer!
  [conn pending-jobs-atom task-constraints trigger-chan]
  (let [offensive-jobs-ch (make-offensive-job-stifler conn)
        offensive-job-filter (partial filter-offensive-jobs task-constraints offensive-jobs-ch)]
    (util/chime-at-ch trigger-chan
                      (fn rank-jobs-event []
                        (reset! pending-jobs-atom
                                (rank-jobs (d/db conn) offensive-job-filter))))))
