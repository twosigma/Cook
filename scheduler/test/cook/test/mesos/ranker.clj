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
  (:require [cook.mesos.ranker :as ranker]
            [cook.mesos.share :as share]
            [cook.mesos.util :as util]
            [cook.test.testutil :refer (restore-fresh-database! create-dummy-group create-dummy-job create-dummy-instance init-offer-cache poll-until)]
            [datomic.api :as d :refer (q db)]
            [plumbing.core :as pc]))


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
            rank-state (ranker/remove-task rank-state dummy-task-1)
            _ (is (= rank-state
                     (waiting-rank-state {:normal {"a" #{}}})))

            rank-state (ranker/add-task rank-state dummy-task-1)
            _ (is (= rank-state
                     (waiting-rank-state {:normal {"a" #{dummy-task-1}}})))

            rank-state (ranker/add-task rank-state dummy-task-2)
            _ (is (= rank-state
                     (waiting-rank-state {:normal {"a" #{dummy-task-1 dummy-task-2}}}))) 

            rank-state (ranker/add-task rank-state dummy-task-3)
            _ (is (= rank-state
                     (waiting-rank-state {:normal {"a" #{dummy-task-3 dummy-task-1 dummy-task-2}}})))

            rank-state (ranker/remove-task rank-state dummy-task-1)
            _ (is (= rank-state
                     (waiting-rank-state {:normal {"a" #{dummy-task-3 dummy-task-2}}})))

            rank-state (ranker/add-task rank-state dummy-task-1)
            _ (is (= rank-state
                     (waiting-rank-state {:normal {"a" #{dummy-task-3 dummy-task-1 dummy-task-2}}})))

            rank-state (ranker/add-task rank-state dummy-task-4)
            _ (is (= rank-state
                     (waiting-rank-state {:normal {"a" #{dummy-task-3 dummy-task-1 dummy-task-2}
                                                   "b" #{dummy-task-4}}})))

            rank-state (ranker/add-task rank-state (job->dummy-task job-5))
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
        rank-state (reduce ranker/add-task rank-state dummy-tasks)]
    (is (= (ranker/rank-jobs (d/db conn) rank-state)
           {:normal (map :job/_instance [dummy-task-3 dummy-task-4 dummy-task-5 dummy-task-1 dummy-task-2])}))))
