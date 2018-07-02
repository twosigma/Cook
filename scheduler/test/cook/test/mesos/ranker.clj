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
(ns cook.test.mesos.ranker
  (:use [clojure.test])
  (:require [cook.mesos.dru :as dru]
            [cook.mesos.share :as share]
            [cook.mesos.util :as util]
            [cook.test.testutil :refer (restore-fresh-database! create-dummy-group create-dummy-job create-dummy-instance init-offer-cache poll-until)]
            [datomic.api :as d :refer (q db)]))

;; TODO: Look into data.avl to store sets and use transients
(defn- update-rank-state-helper
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

(defn- sort-jobs-by-dru-rank-state
  "Sort tasks by dru and filter to waiting jobs.
   returns map of categories to sorted jobs"
  [{:keys [category->user->sorted-tasks]}
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

(deftest test-rank-state-update
  (let [uri "datomic:mem://test-rank-state-update"
        conn (restore-fresh-database! uri) 
        job-1 (create-dummy-job conn :user "a")
        job-2 (create-dummy-job conn :user "a")
        job-3 (create-dummy-job conn :user "a" :priority 100)
        job-4 (create-dummy-job conn :user "b")
        job-5 (create-dummy-job conn :user "c")
        job->dummy-task (fn job->dummy-task [job-ent-id]
                          (util/create-task-ent (d/touch (d/entity (d/db conn) job-ent-id))))
        dummy-task-1 (job->dummy-task job-1)
        dummy-task-2 (job->dummy-task job-2)
        dummy-task-3 (job->dummy-task job-3)
        dummy-task-4 (job->dummy-task job-4)
        dummy-task-5 (job->dummy-task job-5)]
    (testing "add / remove job test"
      (let [rank-state {:category->user->sorted-tasks {}}
            waiting-rank-state (fn waiting-rank-state 
                                 [user->sorted]
                                 {:category->user->sorted-tasks user->sorted})
            rank-state (remove-task rank-state dummy-task-1)
            _ (is (= rank-state
                     (waiting-rank-state {:normal {"a" #{}}})))

            rank-state (add-task rank-state dummy-task-1)
            _ (is (= rank-state
                     (waiting-rank-state {:normal {"a" #{dummy-task-1}}})))

            rank-state (add-task rank-state dummy-task-2)
            _ (is (= rank-state
                     (waiting-rank-state {:normal {"a" #{dummy-task-1 dummy-task-2}}}))) 

            rank-state (add-task rank-state dummy-task-3)
            _ (is (= rank-state
                     (waiting-rank-state {:normal {"a" #{dummy-task-3 dummy-task-1 dummy-task-2}}})))

            rank-state (remove-task rank-state dummy-task-1)
            _ (is (= rank-state
                     (waiting-rank-state {:normal {"a" #{dummy-task-3 dummy-task-2}}})))

            rank-state (add-task rank-state dummy-task-1)
            _ (is (= rank-state
                     (waiting-rank-state {:normal {"a" #{dummy-task-3 dummy-task-1 dummy-task-2}}})))

            rank-state (add-task rank-state dummy-task-4)
            _ (is (= rank-state
                     (waiting-rank-state {:normal {"a" #{dummy-task-3 dummy-task-1 dummy-task-2}
                                                   "b" #{dummy-task-4}}})))

            rank-state (add-task rank-state (job->dummy-task job-5))
            _ (is (= rank-state
                     (waiting-rank-state {:normal {"a" #{dummy-task-3 dummy-task-1 dummy-task-2}
                                                   "b" #{dummy-task-4}
                                                   "c" #{dummy-task-5}}})))]))))

(deftest test-rank-jobs
  (let [uri "datomic:mem://test-rank-jobs"
        conn (restore-fresh-database! uri) 
        job-1 (create-dummy-job conn :user "a")
        job-2 (create-dummy-job conn :user "a")
        job-3 (create-dummy-job conn :user "a" :priority 100)
        job-4 (create-dummy-job conn :user "b")
        job-5 (create-dummy-job conn :user "c")
        job->dummy-task (fn job->dummy-task [job-ent-id]
                          (util/create-task-ent (d/touch (d/entity (d/db conn) job-ent-id))))
        dummy-task-1 (job->dummy-task job-1)
        dummy-task-2 (job->dummy-task job-2)
        dummy-task-3 (job->dummy-task job-3)
        dummy-task-4 (job->dummy-task job-4)
        dummy-task-5 (job->dummy-task job-5)
        dummy-tasks [dummy-task-1 dummy-task-2 dummy-task-3 
                     dummy-task-4 dummy-task-5]
        rank-state {:category->user->sorted-tasks {}}
        rank-state (reduce add-task rank-state dummy-tasks)]
    (is (= (rank-jobs (d/db conn) rank-state)
           {:normal (map :job/_instance [dummy-task-3 dummy-task-4 dummy-task-5 dummy-task-1 dummy-task-2])}))))
