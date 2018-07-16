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

(ns cook.test.benchmark
  (:use clojure.test)
  (:require [cook.mesos.dru :as dru]
            [cook.mesos.ranker :as ranker]
            [cook.mesos.share :as share]
            [cook.mesos.util :as util]
            [cook.test.testutil :refer (restore-fresh-database! create-dummy-group create-dummy-job create-dummy-instance init-offer-cache poll-until)]
            [criterium.core :as cc]
            [datomic.api :as d]))

(defn create-running-job
  [conn host & args]
  (let [job (apply create-dummy-job (cons conn args))
        inst (create-dummy-instance conn job :instance-status :instance.status/running :hostname host)]
    [job inst]))

(deftest ^:benchmark bench-rank-jobs
  (let [uri "datomic:mem://bench-rank-jobs"
        conn (restore-fresh-database! uri)
        ;; Cheap way to have a non-uniform distribution of users
        pick-user (fn [] (first (shuffle ["a" "a" "a" "a" "b" "b" "c" "c" "d" "e" "f"])))]
    (dotimes [_ 50000]
      (create-dummy-job conn :user (pick-user) :ncpus (inc (rand-int 20)) :memory (inc (rand-int 100000))))
    (dotimes [_ 10000]
      (create-running-job conn "abc" :user (pick-user) :job-state :job.state/running))
    (testing "rank-jobs"
      (let [db (d/db conn)
            task-constraints {:memory-gb 100 :cpus 30}
            offensive-jobs-ch (ranker/make-offensive-job-stifler conn)
            offensive-job-filter (partial ranker/filter-offensive-jobs task-constraints offensive-jobs-ch)]
        (println "============ rank-jobs timing ============")
        (cc/quick-bench (ranker/rank-jobs db offensive-job-filter))))
    (testing "rank-jobs minus offensive-job-filter"
      (let [db (d/db conn)
            offensive-job-filter identity]
        (println "============ rank-jobs minus offensive-job-filter timing ============")
        (cc/quick-bench (ranker/rank-jobs db offensive-job-filter))))
    (testing "sort-jobs-by-dru-helper"
      (let [db (d/db conn)
            pending-task-ents (util/get-pending-job-ents db)
            running-task-ents (util/get-running-task-ents db)
            sort-task-scored-task-pairs dru/sorted-task-scored-task-pairs
            user->dru-divisors (share/create-user->share-fn db nil)]
        (do
          (println "============ sort-jobs-by-dru timing ============")
          (cc/quick-bench (ranker/sort-jobs-by-dru-helper pending-task-ents
                                                          running-task-ents
                                                          user->dru-divisors
                                                          sort-task-scored-task-pairs
                                                          ranker/sort-jobs-hierarchy-duration))
          nil)))))
