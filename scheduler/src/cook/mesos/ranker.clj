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
  (:require [cook.mesos.dru :as dru]
            [cook.mesos.share :as share]
            [cook.mesos.util :as util]
            [plumbing.core :as pc]))

;; TODO: Look into data.avl to store sets and use transients 
(defn update-rank-state-helper
  [rank-state update-fn task]
  (let [job (:job/_instance task)
        category (util/categorize-job job)
        user (:job/user job)]
    (-> rank-state
        (update-in [:category->user->sorted-tasks category user]
                   #(update-fn % task)))))

(defn add-task
  "Adds task to rank-state while correctly maintaining sorted order.
   Returns updated rank-state"
  [rank-state task]
  (update-rank-state-helper rank-state 
                            (fnil conj (sorted-set-by (util/same-user-task-comparator)))
                            task))

(defn remove-task
  "Adds task to rank-state while correctly maintaining sorted order.
   Returns updated rank-state"
  [rank-state task]
  (update-rank-state-helper rank-state 
                            (fnil disj (sorted-set-by (util/same-user-task-comparator)))
                            task))

(def category->sort-jobs-by-dru-fn {:normal dru/sorted-task-scored-task-pairs 
                                    :gpu dru/sorted-task-cumulative-gpu-score-pairs})

(defn sort-jobs-by-dru-rank-state
  "Sort tasks by dru and filter to waiting jobs.
   returns map of categories to sorted jobs"
  [{:keys [category->user->sorted-tasks] :as rank-state} 
   user->dru-divisors]
  (letfn [(sort-jobs-by-dru-category-helper 
            [[category user->sorted-tasks]]
            (let [sort-jobs-by-dru-fn (category->sort-jobs-by-dru-fn category)]
              [category 
               (->> user->sorted-tasks
                    (sort-jobs-by-dru-fn user->dru-divisors)
                    (filter (fn filter-to-waiting [[task _]] 
                              (= (get-in task [:job/_instance :job/state]) :job.state/waiting)))
                    (map (fn get-job [[task _]] 
                           (:job/_instance task))))]))]
    (into {} (map sort-jobs-by-dru-category-helper) category->user->sorted-tasks)))

(defn rank-jobs
  "Returns map where keys are categories of jobs and vals are seq of jobs waiting
   to be scheduled in order of Cook's desire to schedule it"
  [db rank-state]
  (let [user->dru-divisors (share/create-user->share-fn db nil)]
    (sort-jobs-by-dru-rank-state rank-state user->dru-divisors)))
 

