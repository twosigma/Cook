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
(ns cook.test.scheduler.dru
  (:require [clojure.test :refer :all]
            [cook.caches :as caches]
            [cook.queries :as queries]
            [cook.scheduler.dru :as dru]
            [cook.scheduler.share :as share]
            [cook.test.postgres]
            [cook.test.testutil :refer [create-dummy-instance create-dummy-job restore-fresh-database! setup]]
            [cook.tools :as util]
            [datomic.api :as d :refer [db q]]
            [plumbing.core :refer [map-vals]]))

(use-fixtures :once cook.test.postgres/with-pg-db)

(deftest test-compute-task-scored-task-pairs
  (testing "return empty set on input empty set"
    (is (= []
           (dru/compute-task-scored-task-pairs {:mem 25.0 :cpus 25.0} '()))))

  (testing "sort tasks from same user"
    (let [datomic-uri "datomic:mem://test-score-tasks"
          conn (restore-fresh-database! datomic-uri)
          job1 (create-dummy-job conn :user "ljin" :memory 10.0 :ncpus 10.0)
          job2 (create-dummy-job conn :user "ljin" :memory 5.0  :ncpus 5.0)
          job3 (create-dummy-job conn :user "ljin" :memory 15.0 :ncpus 25.0)
          job4 (create-dummy-job conn :user "ljin" :memory 25.0 :ncpus 15.0)
          task1 (create-dummy-instance conn job1 :instance-status :instance.status/running)
          task2 (create-dummy-instance conn job2 :instance-status :instance.status/running)
          task3 (create-dummy-instance conn job3 :instance-status :instance.status/running)
          task4 (create-dummy-instance conn job4 :instance-status :instance.status/running)
          task-ent1 (d/entity (d/db conn) task1)
          task-ent2 (d/entity (d/db conn) task2)
          task-ent3 (d/entity (d/db conn) task3)
          task-ent4 (d/entity (d/db conn) task4)
          tasks [task-ent1 task-ent2 task-ent3 task-ent4]]
      (let [scored-task1 (dru/->ScoredTask task-ent1 0.4 10.0 10.0)
            scored-task2 (dru/->ScoredTask task-ent2 0.6 5.0 5.0)
            scored-task3 (dru/->ScoredTask task-ent3 1.6 15.0 25.0)
            scored-task4 (dru/->ScoredTask task-ent4 2.2 25.0 15.0)]
        (is (= [[task-ent1 scored-task1]
                [task-ent2 scored-task2]
                [task-ent3 scored-task3]
                [task-ent4 scored-task4]]
               (dru/compute-task-scored-task-pairs {:mem 25.0 :cpus 25.0} tasks)))))))

(deftest test-init-dru-divisors
  (testing "compute dru divisors for users with different shares"
    (let [datomic-uri "datomic:mem://test-init-dru-divisors"
          conn (restore-fresh-database! datomic-uri)
          job1 (create-dummy-job conn :user "ljin" :memory 10.0 :ncpus 10.0)
          job2 (create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)
          job3 (create-dummy-job conn :user "sunil" :memory 10.0 :ncpus 10.0)
          task1 (create-dummy-instance conn job1 :instance-status :instance.status/running)
          task2 (create-dummy-instance conn job2 :instance-status :instance.status/running)
          db (d/db conn)
          running-task-ents (util/get-running-task-ents db)
          pending-job-ents [(d/entity db job3)]]
      (let [_ (share/set-share! conn "default" nil
                                "Raising limits for new cluster"
                                :mem 25.0 :cpus 25.0 :gpus 1.0)
            _ (share/set-share! conn "wzhao" nil
                                "Tends to use too much stuff"
                                :mem 10.0 :cpus 10.0)
            db (d/db conn)]
        (is (= {"ljin" {:mem 25.0 :cpus 25.0 :gpus 1.0}
                "wzhao" {:mem 10.0 :cpus 10.0 :gpus 1.0}
                "sunil" {:mem 25.0 :cpus 25.0 :gpus 1.0}}
               (dru/init-user->dru-divisors db running-task-ents pending-job-ents nil)))))))

(deftest test-sorted-task-scored-task-pairs
  (let [datomic-uri "datomic:mem://test-sorted-task-scored-task-pairs"
        conn (restore-fresh-database! datomic-uri)
        jobs [(create-dummy-job conn :user "ljin" :memory 10.0 :ncpus 10.0)
              (create-dummy-job conn :user "ljin" :memory 5.0  :ncpus 5.0)
              (create-dummy-job conn :user "ljin" :memory 15.0 :ncpus 25.0)
              (create-dummy-job conn :user "ljin" :memory 25.0 :ncpus 15.0)
              (create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)
              (create-dummy-job conn :user "sunil" :memory 10.0 :ncpus 10.0)]
        tasks (doseq [job jobs]
                (create-dummy-instance conn job :instance-status :instance.status/running))
        db (d/db conn)
        task-ents (util/get-running-task-ents db)]
    (let [share {:mem 10.0 :cpus 10.0}
          ordered-drus [1.0 1.0 1.0 1.5 4.0 5.5]]
      (testing "dru order correct"
        (is (= ordered-drus
               (map (comp :dru second)
                    (dru/sorted-task-scored-task-pairs
                      {"ljin" share "wzhao" share "sunil" share}
                      "no-pool"
                      (map-vals (partial sort-by identity (util/same-user-task-comparator))
                                (group-by util/task-ent->user task-ents)))))))
      ;; Check that the order of users doesn't affect dru order
      (testing "order of users doesn't affect dru order"
        (is (= (dru/sorted-task-scored-task-pairs
                 {"ljin" share "wzhao" share "sunil" share}
                 "no-pool"
                 (map-vals (partial sort-by identity (util/same-user-task-comparator))
                           (group-by util/task-ent->user task-ents)))
               (dru/sorted-task-scored-task-pairs
                 {"ljin" share "wzhao" share "sunil" share}
                 "no-pool"
                 (->> task-ents
                      (group-by util/task-ent->user)
                      (map-vals (partial sort-by identity (util/same-user-task-comparator)))
                      seq
                      shuffle
                      (into {})))))))))

;; This test makes sure that running and queued jobs are sorted with the right
;; implcit order, in particular, it makes sure the codepath of create-task-ent
;; created fake instances for waiting jobs sort in the right order with respect
;; to actual running instances by task->feature-vector
(deftest test-sorted-task-scored-task-pairs-with-running
  (setup)
  (let [datomic-uri "datomic:mem://test-sorted-task-scored-task-pairs-order"
        conn (restore-fresh-database! datomic-uri)
        job1 (create-dummy-job conn :user "ljin" :memory 10.0 :ncpus 10.0 :name "1")
        job2 (create-dummy-job conn :user "ljin" :memory 20.0 :ncpus 20.0 :name "2" :job-state :job.state/running)
        job3 (create-dummy-job conn :user "ljin" :memory 40.0 :ncpus 40.0 :name "3")
        job4 (create-dummy-job conn :user "ljin" :memory 80.0 :ncpus 80.0 :name "4" :job-state :job.state/running)
        job5 (create-dummy-job conn :user "ljin" :memory 160.0 :ncpus 160.0 :name "5")
        _ (create-dummy-instance conn job4 :instance-status :instance.status/running)
        _ (create-dummy-instance conn job2 :instance-status :instance.status/running)
        db (d/db conn)

        pending-task-ents (->> (queries/get-pending-job-ents db)
                               (map util/create-task-ent))
        running-task-ents (util/get-running-task-ents db)
        tasks (into (vec running-task-ents) pending-task-ents)]

    (let [share {:mem 10.0 :cpus 10.0}
          ; Queue should be job4, job2, becasue they're running, then 1 3 5.
          ; DRU:     8.0 10.0 11.0 15.0 31.0
          ordered-drus [8.0 10.0 11.0 15.0 31.0]]
      (testing "dru order correct"
        (is (= ordered-drus
               (map (comp :dru second)
                    (dru/sorted-task-scored-task-pairs
                      {"ljin" share}
                      "no-pool"
                      (map-vals (partial sort-by identity (util/same-user-task-comparator))
                                (group-by util/task-ent->user tasks)))))))
      ; Test that all 5 jobs we hit are in the cache.
      (is (= 5 (-> caches/task-ent->user-cache
                   .asMap
                   .size))))))


(deftest test-compute-sorted-task-cumulative-gpu-score-pairs
  (testing "return empty set on input empty set"
    (is (= []
           (dru/compute-sorted-task-cumulative-gpu-score-pairs 25.0 '()))))

  (testing "sort tasks from same user"
    (let [datomic-uri "datomic:mem://test-score-tasks"
          conn (restore-fresh-database! datomic-uri)
          job1 (create-dummy-job conn :user "ljin" :gpus 10.0)
          job2 (create-dummy-job conn :user "ljin" :gpus 5.0)
          job3 (create-dummy-job conn :user "ljin" :gpus 25.0)
          job4 (create-dummy-job conn :user "ljin" :gpus 15.0)
          task1 (create-dummy-instance conn job1 :instance-status :instance.status/running)
          task2 (create-dummy-instance conn job2 :instance-status :instance.status/running)
          task3 (create-dummy-instance conn job3 :instance-status :instance.status/running)
          task4 (create-dummy-instance conn job4 :instance-status :instance.status/running)
          task-ent1 (d/entity (d/db conn) task1)
          task-ent2 (d/entity (d/db conn) task2)
          task-ent3 (d/entity (d/db conn) task3)
          task-ent4 (d/entity (d/db conn) task4)
          tasks [task-ent1 task-ent2 task-ent3 task-ent4]]
      (is (= [[task-ent1 1.0] [task-ent2 1.5] [task-ent3 4.0] [task-ent4 5.5]]
             (dru/compute-sorted-task-cumulative-gpu-score-pairs 10.0 tasks))))))

(deftest test-sorted-task-cumulative-gpu-score-pairs
  (testing "dru order correct"
    (let [datomic-uri "datomic:mem://test-sorted-task-cumulative-gpu-score-pairs"
          conn (restore-fresh-database! datomic-uri)
          jobs [(create-dummy-job conn :user "ljin" :gpus 10.0)
                (create-dummy-job conn :user "ljin" :gpus 5.0)
                (create-dummy-job conn :user "ljin" :gpus 25.0)
                (create-dummy-job conn :user "ljin" :gpus 15.0)
                (create-dummy-job conn :user "wzhao" :gpus 10.0)
                (create-dummy-job conn :user "sunil" :gpus 10.0)]
          tasks (doseq [job jobs]
                  (create-dummy-instance conn job :instance-status :instance.status/running))
          db (d/db conn)
          task-ents (util/get-running-task-ents db)]
      (let [expected-result [["wzhao" 1.0]
                             ["ljin" 2.0]
                             ["ljin" 3.0]
                             ["sunil" 4.0]
                             ["ljin" 8.0]
                             ["ljin" 11.0]]]
        (is (= expected-result
               (map
                 (fn [[task gpu-score]] [(get-in task [:job/_instance :job/user]) gpu-score])
                 (dru/sorted-task-cumulative-gpu-score-pairs
                   {"ljin"  {:gpus 5.0}
                    "wzhao" {:gpus 10.0}
                    "sunil" {:gpus 2.5}}
                   "no-pool"
                   (map-vals (partial sort-by identity (util/same-user-task-comparator))
                             (group-by util/task-ent->user task-ents))))))))))

(comment (run-tests))
