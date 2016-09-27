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
(ns cook.test.mesos.rebalancer
  (:use clojure.test)
  (:require [cook.mesos :as mesos]
            [cook.mesos.util :as util]
            [cook.mesos.dru :as dru]
            [cook.mesos.rebalancer :as rebalancer :refer (->State)]
            [cook.test.mesos.schema :as schema]
            [datomic.api :as d :refer (q)]
            [cook.mesos.share :as share]
            [clojure.test.check.generators :as gen]
            [clojure.data.priority-map :as pm]))

(deftest test-init-state
  (testing "test1"
    (let [datomic-uri "datomic:mem://test-init-state"
          conn (schema/restore-fresh-database! datomic-uri)
          job1 (schema/create-dummy-job conn :user "ljin" :memory 10.0 :ncpus 10.0)
          job2 (schema/create-dummy-job conn :user "ljin" :memory 5.0  :ncpus 5.0)
          job3 (schema/create-dummy-job conn :user "ljin" :memory 15.0 :ncpus 25.0)
          job4 (schema/create-dummy-job conn :user "ljin" :memory 25.0 :ncpus 15.0)
          job5 (schema/create-dummy-job conn :user "wzhao" :memory 8.0 :ncpus 8.0)
          job6 (schema/create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)
          job7 (schema/create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)
          job8 (schema/create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)

          task1 (schema/create-dummy-instance conn job1 :instance-status :instance.status/running)
          task2 (schema/create-dummy-instance conn job2 :instance-status :instance.status/running)
          task3 (schema/create-dummy-instance conn job3 :instance-status :instance.status/running)
          task4 (schema/create-dummy-instance conn job4 :instance-status :instance.status/running)
          task5 (schema/create-dummy-instance conn job5 :instance-status :instance.status/running)
          task6 (schema/create-dummy-instance conn job6 :instance-status :instance.status/running)
          task7 (schema/create-dummy-instance conn job7 :instance-status :instance.status/running)
          task8 (schema/create-dummy-instance conn job8 :instance-status :instance.status/running)

          task-ent1 (d/entity (d/db conn) task1)
          task-ent2 (d/entity (d/db conn) task2)
          task-ent3 (d/entity (d/db conn) task3)
          task-ent4 (d/entity (d/db conn) task4)
          task-ent5 (d/entity (d/db conn) task5)
          task-ent6 (d/entity (d/db conn) task6)
          task-ent7 (d/entity (d/db conn) task7)
          task-ent8 (d/entity (d/db conn) task8)

          tasks (shuffle [task-ent1 task-ent2 task-ent3 task-ent4
                           task-ent5 task-ent6 task-ent7 task-ent8])]
      (let [_ (share/set-share! conn "default" :mem 25.0 :cpus 25.0 :gpus 1.0)
            scored-task1 (dru/->ScoredTask task-ent1 0.4 10.0 10.0)
            scored-task2 (dru/->ScoredTask task-ent2 0.6 5.0 5.0)
            scored-task3 (dru/->ScoredTask task-ent3 1.6 15.0 25.0)
            scored-task4 (dru/->ScoredTask task-ent4 2.2 25.0 15.0)
            scored-task5 (dru/->ScoredTask task-ent5 0.32 8.0 8.0)
            scored-task6 (dru/->ScoredTask task-ent6 0.72 10.0 10.0)
            scored-task7 (dru/->ScoredTask task-ent7 1.12 10.0 10.0)
            scored-task8 (dru/->ScoredTask task-ent8 1.52 10.0 10.0)
            db (d/db conn)
            running-tasks (util/get-running-task-ents db)
            pending-jobs []
            {:keys [task->scored-task user->sorted-running-task-ents]}
            (rebalancer/init-state db running-tasks pending-jobs {} :normal)]
        (is (= [task-ent4 task-ent3 task-ent8 task-ent7
                task-ent6 task-ent2 task-ent1 task-ent5]
               (keys task->scored-task)))
        (is (= [scored-task4 scored-task3 scored-task8 scored-task7
                scored-task6 scored-task2 scored-task1 scored-task5]
               (vals task->scored-task)))
        (is (= [task-ent1 task-ent2 task-ent3 task-ent4]
               (seq (get user->sorted-running-task-ents "ljin"))))
        (is (= [task-ent5 task-ent6 task-ent7 task-ent8]
               (seq (get user->sorted-running-task-ents "wzhao"))))))))

(deftest test-pending-normal-job-dru
  (testing "test1"
    (let [datomic-uri "datomic:mem://test-rebalancer/compute-pending-normal-job-dru"
          conn (schema/restore-fresh-database! datomic-uri)
          job1 (schema/create-dummy-job conn :name "job1" :user "ljin" :memory 10.0 :ncpus 10.0)
          job2 (schema/create-dummy-job conn :name "job2" :user "ljin" :memory 5.0  :ncpus 5.0)
          job3 (schema/create-dummy-job conn :name "job3" :user "ljin" :memory 15.0 :ncpus 25.0)
          job4 (schema/create-dummy-job conn :name "job4"  :user "ljin" :memory 25.0 :ucpus 15.0)
          job5 (schema/create-dummy-job conn :name "job5" :user "wzhao" :memory 8.0 :ncpus 8.0)
          job6 (schema/create-dummy-job conn :name "job6" :user "wzhao" :memory 10.0 :ncpus 10.0)
          job7 (schema/create-dummy-job conn :name "job7" :user "wzhao" :memory 10.0 :ncpus 10.0)
          job8 (schema/create-dummy-job conn :name "job8" :user "wzhao" :memory 10.0 :ncpus 10.0)

          job9 (schema/create-dummy-job conn :name "job9" :user "wzhao" :memory 10.0 :ncpus 10.0)
          job10 (schema/create-dummy-job conn :name "job10" :user "sunil" :memory 20.0 :ncpus 20.0)
          job11 (schema/create-dummy-job conn :name "job11" :user "ljin" :memory 10.0 :ucpus 10.0)

          task1 (schema/create-dummy-instance conn job1 :instance-status :instance.status/running)
          task2 (schema/create-dummy-instance conn job2 :instance-status :instance.status/running)
          task3 (schema/create-dummy-instance conn job3 :instance-status :instance.status/running)
          task4 (schema/create-dummy-instance conn job4 :instance-status :instance.status/running)
          task5 (schema/create-dummy-instance conn job5 :instance-status :instance.status/running)
          task6 (schema/create-dummy-instance conn job6 :instance-status :instance.status/running)
          task7 (schema/create-dummy-instance conn job7 :instance-status :instance.status/running)
          task8 (schema/create-dummy-instance conn job8 :instance-status :instance.status/running)

          task-ent1 (d/entity (d/db conn) task1)
          task-ent2 (d/entity (d/db conn) task2)
          task-ent3 (d/entity (d/db conn) task3)
          task-ent4 (d/entity (d/db conn) task4)
          task-ent5 (d/entity (d/db conn) task5)
          task-ent6 (d/entity (d/db conn) task6)
          task-ent7 (d/entity (d/db conn) task7)
          task-ent8 (d/entity (d/db conn) task8)

          _ (share/set-share! conn "default" :mem 25.0 :cpus 25.0 :gpus 1.0)

          db (d/db conn)
          running-tasks (util/get-running-task-ents db)
          pending-jobs (map #(d/entity db %) [job9 job10 job11])
          state (rebalancer/init-state db running-tasks pending-jobs {} :normal)]
      (is (= 1.92 (rebalancer/compute-pending-normal-job-dru state (d/entity db job9))))
      (is (= 0.8 (rebalancer/compute-pending-normal-job-dru state (d/entity db job10))))
      (is (= 2.6 (rebalancer/compute-pending-normal-job-dru state (d/entity db job11)))))))

(deftest test-pending-gpu-job-dru
  (let [datomic-uri "datomic:mem://test-rebalancer/compute-pending-normal-job-dru"
        conn (schema/restore-fresh-database! datomic-uri)
        job1 (schema/create-dummy-job conn :name "job1" :user "ljin" :memory 10.0 :ncpus 10.0 :gpus 1.0)
        job2 (schema/create-dummy-job conn :name "job2" :user "ljin" :memory 5.0  :ncpus 5.0 :gpus 1.0)
        job3 (schema/create-dummy-job conn :name "job3" :user "ljin" :memory 15.0 :ncpus 25.0 :gpus 1.0)
        job4 (schema/create-dummy-job conn :name "job4"  :user "ljin" :memory 25.0 :ucpus 15.0 :gpus 1.0)
        job5 (schema/create-dummy-job conn :name "job5" :user "wzhao" :memory 8.0 :ncpus 8.0 :gpus 1.0)
        job6 (schema/create-dummy-job conn :name "job6" :user "wzhao" :memory 10.0 :ncpus 10.0 :gpus 1.0)
        job7 (schema/create-dummy-job conn :name "job7" :user "wzhao" :memory 10.0 :ncpus 10.0 :gpus 1.0)
        job8 (schema/create-dummy-job conn :name "job8" :user "wzhao" :memory 10.0 :ncpus 10.0 :gpus 1.0)

        job9 (schema/create-dummy-job conn :name "job9" :user "wzhao" :memory 10.0 :ncpus 10.0 :gpus 1.0)
        job10 (schema/create-dummy-job conn :name "job10" :user "sunil" :memory 20.0 :ncpus 20.0 :gpus 1.0)
        job11 (schema/create-dummy-job conn :name "job11" :user "ljin" :memory 10.0 :ucpus 10.0 :gpus 2.0)

        task1 (schema/create-dummy-instance conn job1 :instance-status :instance.status/running)
        task2 (schema/create-dummy-instance conn job2 :instance-status :instance.status/running)
        task3 (schema/create-dummy-instance conn job3 :instance-status :instance.status/running)
        task4 (schema/create-dummy-instance conn job4 :instance-status :instance.status/running)
        task5 (schema/create-dummy-instance conn job5 :instance-status :instance.status/running)
        task6 (schema/create-dummy-instance conn job6 :instance-status :instance.status/running)
        task7 (schema/create-dummy-instance conn job7 :instance-status :instance.status/running)
        task8 (schema/create-dummy-instance conn job8 :instance-status :instance.status/running)

        task-ent1 (d/entity (d/db conn) task1)
        task-ent2 (d/entity (d/db conn) task2)
        task-ent3 (d/entity (d/db conn) task3)
        task-ent4 (d/entity (d/db conn) task4)
        task-ent5 (d/entity (d/db conn) task5)
        task-ent6 (d/entity (d/db conn) task6)
        task-ent7 (d/entity (d/db conn) task7)
        task-ent8 (d/entity (d/db conn) task8)

        _ (share/set-share! conn "default" :mem 25.0 :cpus 25.0 :gpus 1.0)

        db (d/db conn)
        running-tasks (util/get-running-task-ents db)
        pending-jobs (map #(d/entity db %) [job9 job10 job11])
        state (rebalancer/init-state db running-tasks pending-jobs {} :gpu)]
    (is (= (rebalancer/compute-pending-gpu-job-dru state (d/entity db job2))
           (rebalancer/compute-pending-gpu-job-dru state (d/entity db job6))))
    (is (= 5.0 (rebalancer/compute-pending-gpu-job-dru state (d/entity db job9))))
    (is (= 1.0 (rebalancer/compute-pending-gpu-job-dru state (d/entity db job10))))
    (is (= 6.0 (rebalancer/compute-pending-gpu-job-dru state (d/entity db job11))))))

(deftest test-compute-preemption-decision
  (testing "test1"
    (let [datomic-uri "datomic:mem://test-compute-preemption-decision"
          conn (schema/restore-fresh-database! datomic-uri)
          job1 (schema/create-dummy-job conn :user "ljin" :memory 10.0 :ncpus 10.0)
          job2 (schema/create-dummy-job conn :user "ljin" :memory 5.0  :ncpus 5.0)
          job3 (schema/create-dummy-job conn :user "ljin" :memory 15.0 :ncpus 25.0)
          job4 (schema/create-dummy-job conn :user "ljin" :memory 25.0 :ncpus 15.0)
          job5 (schema/create-dummy-job conn :user "wzhao" :memory 8.0 :ncpus 8.0)
          job6 (schema/create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)
          job7 (schema/create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)
          job8 (schema/create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)

          job9 (schema/create-dummy-job conn :user "wzhao" :memory 15.0 :ncpus 15.0)
          job10 (schema/create-dummy-job conn :user "sunil" :memory 15.0 :ncpus 15.0)
          job11 (schema/create-dummy-job conn :user "ljin" :memory 15.0 :ncpus 15.0)
          job12 (schema/create-dummy-job conn :user "sunil" :memory 40.0 :ncpus 40.0)
          job13 (schema/create-dummy-job conn :user "sunil" :memory 45.0 :ncpus 45.0)
          job14 (schema/create-dummy-job conn :user "sunil" :memory 80.0 :ncpus 80.0)

          task1 (schema/create-dummy-instance conn
                                             job1
                                             :instance-status :instance.status/running
                                             :hostname "hostA")
          task2 (schema/create-dummy-instance conn
                                             job2
                                             :instance-status :instance.status/running
                                             :hostname "hostA")
          task3 (schema/create-dummy-instance conn
                                             job3
                                             :instance-status :instance.status/running
                                             :hostname "hostB")
          task4 (schema/create-dummy-instance conn
                                             job4
                                             :instance-status :instance.status/running
                                             :hostname "hostB")
          task5 (schema/create-dummy-instance conn
                                             job5
                                             :instance-status :instance.status/running
                                             :hostname "hostA")
          task6 (schema/create-dummy-instance conn
                                             job6
                                             :instance-status :instance.status/running
                                             :hostname "hostB")
          task7 (schema/create-dummy-instance conn
                                             job7
                                             :instance-status :instance.status/running
                                             :hostname "hostA")
          task8 (schema/create-dummy-instance conn
                                             job8
                                             :instance-status :instance.status/running
                                             :hostname "hostB")

          task-ent1 (d/entity (d/db conn) task1)
          task-ent2 (d/entity (d/db conn) task2)
          task-ent3 (d/entity (d/db conn) task3)
          task-ent4 (d/entity (d/db conn) task4)
          task-ent5 (d/entity (d/db conn) task5)
          task-ent6 (d/entity (d/db conn) task6)
          task-ent7 (d/entity (d/db conn) task7)
          task-ent8 (d/entity (d/db conn) task8)

          _ (share/set-share! conn "default" :mem 25.0 :cpus 25.0 :gpus 1.0)

          db (d/db conn)
          running-tasks (util/get-running-task-ents db)
          pending-jobs (map #(d/entity db %) [job9 job10 job11 job12 job13 job14])
          {:keys [task->scored-task user->sorted-running-task-ents user->dru-divisors]}
          (rebalancer/init-state db running-tasks pending-jobs {} :normal)]

      (comment all-decisions-without-spare-resources
        {"hostA" [{:dru 1.12 :task [task-ent7] :mem 10.0 :cpus 10.0}]
         "hostB" [{:dru 2.2 :task [task-ent4] :mem 25.0 :cpus 15.0}
                  {:dru 1.6 :task [task-ent4 task-ent3] :mem 40.0 :cpus 40.0}
                  {:dru 1.52 :task [task-ent4 task-ent3 task-ent8] :mem 50.0 :cpus 50.0}]})

      (is (= {:hostname "hostB" :dru 2.2 :task [task-ent4] :mem 25.0 :cpus 15.0 :gpus 0.0}
             (rebalancer/compute-preemption-decision (->State task->scored-task
                                                            user->sorted-running-task-ents
                                                            {}
                                                            user->dru-divisors
                                                            rebalancer/compute-pending-normal-job-dru)
                                                   {:min-dru-diff 0.05 :safe-dru-threshold 1.0}
                                                   (d/entity db job9))))

      (is (= {:hostname "hostB" :dru Double/MAX_VALUE :task nil :mem 15.0 :cpus 15.0 :gpus 0.0}
             (rebalancer/compute-preemption-decision (->State task->scored-task
                                                            user->sorted-running-task-ents
                                                            {"hostB" {:mem 15.0 :cpus 15.0}}
                                                            user->dru-divisors
                                                            rebalancer/compute-pending-normal-job-dru)
                                                   {:min-dru-diff 0.5 :safe-dru-threshold 1.0}
                                                   (d/entity db job9))))

      (is (= {:hostname "hostA" :dru Double/MAX_VALUE :task nil :mem 20.0 :cpus 20.0 :gpus 0.0}
             (rebalancer/compute-preemption-decision (->State task->scored-task
                                                            user->sorted-running-task-ents
                                                            {"hostA" {:mem 20.0 :cpus 20.0}
                                                             "hostB" {:mem 10.0 :cpus 10.0}}
                                                            user->dru-divisors
                                                            rebalancer/compute-pending-normal-job-dru)
                                                   {:min-dru-diff 0.5 :safe-dru-threshold 1.0}
                                                   (d/entity db job9))))

      (is (= {:hostname "hostB" :dru 2.2 :task [task-ent4] :mem 35.0 :cpus 25.0 :gpus 0.0}
             (rebalancer/compute-preemption-decision (->State task->scored-task
                                                            user->sorted-running-task-ents
                                                            {"hostA" {:mem 10.0 :cpus 10.0}
                                                             "hostB" {:mem 10.0 :cpus 10.0}}
                                                            user->dru-divisors
                                                            rebalancer/compute-pending-normal-job-dru)
                                                   {:min-dru-diff 0.0 :safe-dru-threshold 1.0}
                                                   (d/entity db job9))))

      (is (= {:hostname "hostB" :dru 2.2 :task [task-ent4] :mem 25.0 :cpus 15.0 :gpus 0.0}
             (rebalancer/compute-preemption-decision (->State task->scored-task
                                                            user->sorted-running-task-ents
                                                            {}
                                                            user->dru-divisors
                                                            rebalancer/compute-pending-normal-job-dru)
                                                   {:min-dru-diff 0.5 :safe-dru-threshold 1.0}
                                                   (d/entity db job10))))

      (is (= nil
             (rebalancer/compute-preemption-decision (->State task->scored-task
                                                            user->sorted-running-task-ents
                                                            {}
                                                            user->dru-divisors
                                                            rebalancer/compute-pending-normal-job-dru)
                                                   {:min-dru-diff 0.5 :safe-dru-threshold 1.0}
                                                   (d/entity db job11))))

      (is (= nil
             (rebalancer/compute-preemption-decision (->State task->scored-task
                                                            user->sorted-running-task-ents
                                                            {}
                                                            user->dru-divisors
                                                            rebalancer/compute-pending-normal-job-dru)
                                                   {:min-dru-diff 0.0 :safe-dru-threshold 1.0}
                                                   (d/entity db job12))))

      (is (= {:hostname "hostA" :dru Double/MAX_VALUE :task nil :mem 40.0 :cpus 40.0 :gpus 0.0}
             (rebalancer/compute-preemption-decision (->State task->scored-task
                                                            user->sorted-running-task-ents
                                                            {"hostA" {:mem 40.0 :cpus 40.0}}
                                                            user->dru-divisors
                                                            rebalancer/compute-pending-normal-job-dru)
                                                   {:min-dru-diff 0.5 :safe-dru-threshold 1.0}
                                                   (d/entity db job12))))

      (is (= nil
             (rebalancer/compute-preemption-decision (->State task->scored-task
                                                            user->sorted-running-task-ents
                                                            {"hostA" {:mem 35.0 :cpus 35.0}}
                                                            user->dru-divisors
                                                            rebalancer/compute-pending-normal-job-dru)
                                                   {:min-dru-diff 0.0 :safe-dru-threshold 1.0}
                                                   (d/entity db job12))))

      (is (= {:hostname "hostB" :dru 2.2 :task [task-ent4] :mem 55.0 :cpus 45.0 :gpus 0.0}
             (rebalancer/compute-preemption-decision (->State task->scored-task
                                                            user->sorted-running-task-ents
                                                            {"hostA" {:mem 35.0 :cpus 35.0}
                                                             "hostB" {:mem 30.0 :cpus 30.0}}
                                                            user->dru-divisors
                                                            rebalancer/compute-pending-normal-job-dru)
                                                   {:min-dru-diff 0.5 :safe-dru-threshold 1.0}
                                                   (d/entity db job12))))

      (is (= nil
             (rebalancer/compute-preemption-decision (->State task->scored-task
                                                            user->sorted-running-task-ents
                                                            {}
                                                            user->dru-divisors
                                                            rebalancer/compute-pending-normal-job-dru)
                                                   {:min-dru-diff 0.5 :safe-dru-threshold 1.0}
                                                   (d/entity db job13))))

      (is (= nil
             (rebalancer/compute-preemption-decision (->State task->scored-task
                                                            user->sorted-running-task-ents
                                                            {}
                                                            user->dru-divisors
                                                            rebalancer/compute-pending-normal-job-dru)
                                                   {:min-dru-diff 2.0 :safe-dru-threshold 1.0}
                                                   (d/entity db job13))))

      (is (= nil
             (rebalancer/compute-preemption-decision (->State task->scored-task
                                                            user->sorted-running-task-ents
                                                            {}
                                                            user->dru-divisors
                                                            rebalancer/compute-pending-normal-job-dru)
                                                   {:min-dru-diff 0.5 :safe-dru-threshold 1.0}
                                                   (d/entity db job14)))))))
(deftest test-next-state
  (testing "test1"
    (let [datomic-uri "datomic:mem://test-next-state"
          conn (schema/restore-fresh-database! datomic-uri)
          job1 (schema/create-dummy-job conn :user "ljin" :memory 10.0 :ncpus 10.0)
          job2 (schema/create-dummy-job conn :user "ljin" :memory 5.0  :ncpus 5.0)
          job3 (schema/create-dummy-job conn :user "ljin" :memory 15.0 :ncpus 25.0)
          job4 (schema/create-dummy-job conn :user "ljin" :memory 25.0 :ncpus 15.0)
          job5 (schema/create-dummy-job conn :user "wzhao" :memory 8.0 :ncpus 8.0)
          job6 (schema/create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)
          job7 (schema/create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)
          job8 (schema/create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)

          job9 (schema/create-dummy-job conn :user "wzhao" :memory 15.0 :ncpus 15.0)
          job10 (schema/create-dummy-job conn :user "sunil" :memory 15.0 :ncpus 15.0)
          job11 (schema/create-dummy-job conn :user "ljin" :memory 15.0 :ncpus 15.0)
          job12 (schema/create-dummy-job conn :user "sunil" :memory 40.0 :ncpus 40.0)
          job13 (schema/create-dummy-job conn :user "sunil" :memory 45.0 :ncpus 45.0)
          job14 (schema/create-dummy-job conn :user "sunil" :memory 80.0 :ncpus 80.0)

          task1 (schema/create-dummy-instance conn
                                             job1
                                             :instance-status :instance.status/running
                                             :hostname "hostA")
          task2 (schema/create-dummy-instance conn
                                             job2
                                             :instance-status :instance.status/running
                                             :hostname "hostA")
          task3 (schema/create-dummy-instance conn
                                             job3
                                             :instance-status :instance.status/running
                                             :hostname "hostB")
          task4 (schema/create-dummy-instance conn
                                             job4
                                             :instance-status :instance.status/running
                                             :hostname "hostB")
          task5 (schema/create-dummy-instance conn
                                             job5
                                             :instance-status :instance.status/running
                                             :hostname "hostA")
          task6 (schema/create-dummy-instance conn
                                             job6
                                             :instance-status :instance.status/running
                                             :hostname "hostB")
          task7 (schema/create-dummy-instance conn
                                             job7
                                             :instance-status :instance.status/running
                                             :hostname "hostA")
          task8 (schema/create-dummy-instance conn
                                             job8
                                             :instance-status :instance.status/running
                                             :hostname "hostB")
          _ (share/set-share! conn "default" :mem 25.0 :cpus 25.0 :gpus 1.0)
          db (d/db conn)

          job-ent9 (d/entity db job9)
          job-ent10 (d/entity db job10)
          job-ent12 (d/entity db job12)

          task-ent1 (d/entity db task1)
          task-ent2 (d/entity db task2)
          task-ent3 (d/entity db task3)
          task-ent4 (d/entity db task4)
          task-ent5 (d/entity db task5)
          task-ent6 (d/entity db task6)
          task-ent7 (d/entity db task7)
          task-ent8 (d/entity db task8)

          running-task-ents (util/get-running-task-ents db)
          pending-job-ents (map #(d/entity db %) [job9 job10 job11 job12 job13 job14])
          host->spare-resources {"hostA" {:mem 50.0 :cpus 50.0}}
          user->dru-divisors {"ljin" {:mem 25.0 :cpus 25.0 :gpus 1.0} "wzhao" {:mem 25.0 :cpus 25.0 :gpus 1.0} "sunil" {:mem 25.0 :cpus 25.0 :gpus 1.0}}
          state (rebalancer/init-state db running-task-ents pending-job-ents host->spare-resources :normal)]
      (let [task-ent9 {:job/_instance job-ent9
                       :instance/hostname "hostB"
                       :instance/status :instance.status/running}
            user->sorted-running-task-ents' {"ljin" (into (sorted-set-by (util/same-user-task-comparator-penalize-backfill)) [task-ent1 task-ent2 task-ent3 task-ent4])
                                               "wzhao" (into (sorted-set-by (util/same-user-task-comparator-penalize-backfill)) [task-ent5 task-ent7 task-ent9])}
            host->spare-resources' {"hostA" {:mem 50.0 :cpus 50.0} "hostB" {:mem 5.0 :cpus 5.0 :gpus 0.0}}]
        (let [{task->scored-task'' :task->scored-task
               user->sorted-running-task-ents'' :user->sorted-running-task-ents
               host->spare-resources'' :host->spare-resources
               user->dru-divisors'' :user->dru-divisors}
              (rebalancer/next-state state job-ent9 {:hostname "hostB" :task [task-ent6 task-ent8] :mem 20.0 :cpus 20.0 :gpus 0.0})]
          (is (= user->sorted-running-task-ents' user->sorted-running-task-ents''))
          (is (= host->spare-resources' host->spare-resources''))
          (is (= user->dru-divisors user->dru-divisors''))
          (is (= [task-ent4 task-ent3 task-ent9 task-ent7 task-ent2 task-ent1 task-ent5]
                 (keys task->scored-task'')))
          (is (= [(dru/->ScoredTask task-ent4 2.2 25.0 15.0)
                  (dru/->ScoredTask task-ent3 1.6 15.0 25.0)
                  (dru/->ScoredTask task-ent9 1.32 15.0 15.0)
                  (dru/->ScoredTask task-ent7 0.72 10.0 10.0)
                  (dru/->ScoredTask task-ent2 0.6 5.0 5.0)
                  (dru/->ScoredTask task-ent1 0.4 10.0 10.0)
                  (dru/->ScoredTask task-ent5 0.32 8.0 8.0)]
                 (vals task->scored-task'')))))

      (let [task-ent10 {:job/_instance job-ent10
                        :instance/hostname "hostA"
                        :instance/status :instance.status/running}
            user->sorted-running-task-ents' {"ljin" (into (sorted-set-by (util/same-user-task-comparator-penalize-backfill)) [task-ent1 task-ent3 task-ent4])
                                               "wzhao" (into (sorted-set-by (util/same-user-task-comparator-penalize-backfill)) [task-ent5 task-ent6 task-ent8])
                                               "sunil" (into (sorted-set-by (util/same-user-task-comparator-penalize-backfill)) [task-ent10])}
            host->spare-resources' {"hostA" {:mem 50.0 :cpus 50.0 :gpus 0.0}}]
        (let [{task->scored-task'' :task->scored-task
               user->sorted-running-task-ents'' :user->sorted-running-task-ents
               host->spare-resources'' :host->spare-resources
               user->dru-divisors'' :user->dru-divisors}
              (rebalancer/next-state state job-ent10 {:hostname "hostA" :task [task-ent2 task-ent7] :mem 65.0 :cpus 65.0})]
          (is (= user->sorted-running-task-ents' user->sorted-running-task-ents''))
          (is (= host->spare-resources' host->spare-resources''))
          (is (= [task-ent4 task-ent3 task-ent8 task-ent6 task-ent10 task-ent1 task-ent5]
                 (keys task->scored-task'')))
          (is (= [(dru/->ScoredTask task-ent4 2.0 25.0 15.0)
                  (dru/->ScoredTask task-ent3 1.4 15.0 25.0)
                  (dru/->ScoredTask task-ent8 1.12 10.0 10.0)
                  (dru/->ScoredTask task-ent6 0.72 10.0 10.0)
                  (dru/->ScoredTask task-ent10 0.6 15.0 15.0)
                  (dru/->ScoredTask task-ent1 0.4 10.0 10.0)
                  (dru/->ScoredTask task-ent5 0.32 8.0 8.0)]
                 (vals task->scored-task'')))))

      (let [task-ent12 {:job/_instance job-ent12
                        :instance/hostname "hostA"
                        :instance/status :instance.status/running}
            user->sorted-running-task-ents' {"ljin" (into (sorted-set-by (util/same-user-task-comparator-penalize-backfill)) [task-ent1 task-ent2 task-ent3 task-ent4])
                                               "wzhao" (into (sorted-set-by (util/same-user-task-comparator-penalize-backfill)) [task-ent5 task-ent6 task-ent7 task-ent8])
                                               "sunil" (into (sorted-set-by (util/same-user-task-comparator-penalize-backfill)) [task-ent12])}
            host->spare-resources' {"hostA" {:mem 10.0 :cpus 10.0 :gpus 0.0}}]

        (let [{task->scored-task'' :task->scored-task
               user->sorted-running-task-ents'' :user->sorted-running-task-ents
               host->spare-resources'' :host->spare-resources
               user->dru-divisors'' :user->dru-divisors}
              (rebalancer/next-state state job-ent12 {:hostname "hostA" :task [] :mem 50.0 :cpus 50.0})]
          (is (= user->sorted-running-task-ents' user->sorted-running-task-ents''))
          (is (= host->spare-resources' host->spare-resources''))
          (is (= [task-ent4 task-ent12 task-ent3 task-ent8 task-ent7 task-ent6 task-ent2 task-ent1 task-ent5]
                 (keys task->scored-task'')))
          (is (= [(dru/->ScoredTask task-ent4 2.2 25.0 15.0)
                  (dru/->ScoredTask task-ent12 1.6 40.0 40.0)
                  (dru/->ScoredTask task-ent3 1.6 15.0 25.0)
                  (dru/->ScoredTask task-ent8 1.52 10.0 10.0)
                  (dru/->ScoredTask task-ent7 1.12 10.0 10.0)
                  (dru/->ScoredTask task-ent6 0.72 10.0 10.0)
                  (dru/->ScoredTask task-ent2 0.6 5.0 5.0)
                  (dru/->ScoredTask task-ent1 0.4 10.0 10.0)
                  (dru/->ScoredTask task-ent5 0.32 8.0 8.0)]
                 (vals task->scored-task''))))))))

(deftest test-rebalance
  (testing "test1"
    (let [datomic-uri "datomic:mem://test-rebalance"
          conn (schema/restore-fresh-database! datomic-uri)
          job1 (schema/create-dummy-job conn :user "ljin" :memory 10.0 :ncpus 10.0)
          job2 (schema/create-dummy-job conn :user "ljin" :memory 5.0  :ncpus 5.0)
          job3 (schema/create-dummy-job conn :user "ljin" :memory 15.0 :ncpus 25.0)
          job4 (schema/create-dummy-job conn :user "ljin" :memory 25.0 :ncpus 15.0)
          job5 (schema/create-dummy-job conn :user "wzhao" :memory 8.0 :ncpus 8.0)
          job6 (schema/create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)
          job7 (schema/create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)
          job8 (schema/create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)

          job9 (schema/create-dummy-job conn :user "wzhao" :memory 15.0 :ncpus 15.0)
          job10 (schema/create-dummy-job conn :user "sunil" :memory 15.0 :ncpus 15.0)
          job11 (schema/create-dummy-job conn :user "ljin" :memory 15.0 :ncpus 15.0)
          job12 (schema/create-dummy-job conn :user "sunil" :memory 15.0 :ncpus 15.0)
          job13 (schema/create-dummy-job conn :user "sunil" :memory 15.0 :ncpus 15.0)
          job14 (schema/create-dummy-job conn :user "sunil" :memory 15.0 :ncpus 15.0)

          task1 (schema/create-dummy-instance conn
                                             job1
                                             :instance-status :instance.status/running
                                             :hostname "hostA")
          task2 (schema/create-dummy-instance conn
                                             job2
                                             :instance-status :instance.status/running
                                             :hostname "hostA")
          task3 (schema/create-dummy-instance conn
                                             job3
                                             :instance-status :instance.status/running
                                             :hostname "hostB")
          task4 (schema/create-dummy-instance conn
                                             job4
                                             :instance-status :instance.status/running
                                             :hostname "hostB")
          task5 (schema/create-dummy-instance conn
                                             job5
                                             :instance-status :instance.status/running
                                             :hostname "hostA")
          task6 (schema/create-dummy-instance conn
                                             job6
                                             :instance-status :instance.status/running
                                             :hostname "hostB")
          task7 (schema/create-dummy-instance conn
                                             job7
                                             :instance-status :instance.status/running
                                             :hostname "hostA")
          task8 (schema/create-dummy-instance conn
                                             job8
                                             :instance-status :instance.status/running
                                             :hostname "hostB")
          _ (share/set-share! conn "default" :mem 25.0 :cpus 25.0 :gpus 1.0)

          db (d/db conn)

          task-ent1 (d/entity db task1)
          task-ent2 (d/entity db task2)
          task-ent3 (d/entity db task3)
          task-ent4 (d/entity db task4)
          task-ent5 (d/entity db task5)
          task-ent6 (d/entity db task6)
          task-ent7 (d/entity db task7)
          task-ent8 (d/entity db task8)

          job9 (schema/create-dummy-job conn :user "wzhao" :memory 5.0 :ncpus 5.0)
          job10 (schema/create-dummy-job conn :user "wzhao" :memory 5.0 :ncpus 5.0)
          job11 (schema/create-dummy-job conn :user "wzhao" :memory 5.0 :ncpus 5.0)
          job12 (schema/create-dummy-job conn :user "wzhao" :memory 5.0 :ncpus 5.0)
          job13 (schema/create-dummy-job conn :user "wzhao" :memory 5.0 :ncpus 5.0)
          job14 (schema/create-dummy-job conn :user "wzhao" :memory 5.0 :ncpus 5.0)
          job15 (schema/create-dummy-job conn :user "wzhao" :memory 5.0 :ncpus 5.0)
          job16 (schema/create-dummy-job conn :user "wzhao" :memory 5.0 :ncpus 5.0)
          job17 (schema/create-dummy-job conn :user "wzhao" :memory 5.0 :ncpus 5.0)
          job18 (schema/create-dummy-job conn :user "wzhao" :memory 5.0 :ncpus 5.0)

          job19 (schema/create-dummy-job conn :user "sunil" :memory 5.0 :ncpus 5.0)
          job20 (schema/create-dummy-job conn :user "sunil" :memory 5.0 :ncpus 5.0)
          job21 (schema/create-dummy-job conn :user "sunil" :memory 5.0 :ncpus 5.0)
          job22 (schema/create-dummy-job conn :user "sunil" :memory 5.0 :ncpus 5.0)
          job23 (schema/create-dummy-job conn :user "sunil" :memory 5.0 :ncpus 5.0)
          job24 (schema/create-dummy-job conn :user "sunil" :memory 5.0 :ncpus 5.0)
          job25 (schema/create-dummy-job conn :user "sunil" :memory 5.0 :ncpus 5.0)
          job26 (schema/create-dummy-job conn :user "sunil" :memory 5.0 :ncpus 5.0)
          job27 (schema/create-dummy-job conn :user "sunil" :memory 5.0 :ncpus 5.0)
          job28 (schema/create-dummy-job conn :user "sunil" :memory 5.0 :ncpus 5.0)]

      (comment
        {task-ent1 0.4
         task-ent2 0.6
         task-ent3 1.6
         task-ent4 2.2
         task-ent5 0.32
         task-ent6 0.72
         task-ent7 1.12
         task-ent8 1.52})

      (let [db (d/db conn)
            pending-job-ents (map #(d/entity db %) [job9 job10 job11 job12 job13
                                                    job14 job15 job16 job17 job18])
            params {:max-preemption 128 :safe-dru-threshold 1.0 :min-dru-diff 0.0 :category :normal}]
        (let [[pending-job-ents-to-run task-ents-to-preempt] (rebalancer/rebalance db pending-job-ents {} params)]
          (is (= (map #(d/entity db %) [job9 job10 job11])
                 pending-job-ents-to-run))
          (is (= [task-ent4]
                 task-ents-to-preempt)))
        (let [[pending-job-ents-to-run task-ents-to-preempt]
              (rebalancer/rebalance db pending-job-ents {"hostB" {:mem 0.0 :cpus 10.0}} params)]
          (is (= (map #(d/entity db %) [job9 job10 job11 job12 job13])
                 pending-job-ents-to-run))
          (is (= [task-ent4]
                 task-ents-to-preempt))))

      (let [db (d/db conn)
            pending-job-ents (map #(d/entity db %) [job19 job20 job21 job22 job23
                                                    job24 job25 job26 job27 job28])
            params {:max-preemption 128 :safe-dru-threshold 1.0 :min-dru-diff 0.0 :category :normal}]
        (let [[pending-job-ents-to-run task-ents-to-preempt] (rebalancer/rebalance db pending-job-ents {} params)]
          (is (= (map #(d/entity db %) [job19 job20 job21 job22 job23
                                        job24 job25 job26])
                 pending-job-ents-to-run))
          (is (= [task-ent4 task-ent3]
                 task-ents-to-preempt))))

      (let [db (d/db conn)
            pending-job-ents (map #(d/entity db %) [job19 job20 job21 job22 job23
                                                    job24 job25 job26 job27 job28])
            params {:max-preemption 128 :safe-dru-threshold 1.0 :min-dru-diff 0.0 :category :normal}]
        (let [[pending-job-ents-to-run task-ents-to-preempt] (rebalancer/rebalance db pending-job-ents {"hostB" {:cpus 25.0 :mem 25.0}} params)]
          (is (= (map #(d/entity db %) [job19 job20 job21 job22 job23
                                        job24 job25 job26])
                 pending-job-ents-to-run))
          (is (= [task-ent4]
                 task-ents-to-preempt))))

      (let [_ (share/set-share! conn "sunil" :mem 50.0 :cpus 50.0)
            db (d/db conn)
            pending-job-ents (map #(d/entity db %) [job19 job20 job21 job22 job23
                                                    job24 job25 job26 job27 job28])
            params {:max-preemption 128 :safe-dru-threshold 1.0 :min-dru-diff 0.0 :category :normal}]
        (let [[pending-job-ents-to-run task-ents-to-preempt] (rebalancer/rebalance db pending-job-ents {} params)]
          (is (= (map #(d/entity db %) [job19 job20 job21 job22 job23
                                        job24 job25 job26 job27 job28])
                 pending-job-ents-to-run))
          (is (= [task-ent4 task-ent3 task-ent8]
                 task-ents-to-preempt)))))))

(deftest ^:integration test-rebalance2
  (testing "rebalance prop test"
    (let [datomic-uri "datomic:mem://test-rebalance2"
          running-user-gen (gen/elements ["ljin", "sunil", "wzhao", "abolin", "dgrnbrg", "palaitis", "sdelger", "wyegelwe"])
          pending-user-gen (gen/elements ["qiliu", "bwignall", "jshen"])
          host-gen (gen/elements ["hostA", "hostB", "hostC", "hostD", "hostE", "hostF", "hostG", "hostH",
                                  "hostI", "hostJ", "hostK", "hostL", "hostM", "hostN", "hostO", "hostP"])
          mem-gen (gen/choose 1024 81920)
          cpus-gen (gen/choose 1 8)
          running-job-gen  (gen/tuple running-user-gen mem-gen cpus-gen)
          pending-job-gen  (gen/tuple pending-user-gen mem-gen cpus-gen)
          ]
      (let [conn (schema/restore-fresh-database! datomic-uri)
            _ (share/set-share! conn "default" :mem 1024.0 :cpus Double/MAX_VALUE :gpus 1.0)
            running-tasks-sample-size 10240
            pending-jobs-sample-size 1024

            _ (doseq [x (range running-tasks-sample-size)]
                (let [[[user mem cpus]] (gen/sample running-job-gen 1)
                      [host] (gen/sample host-gen 1)
                      job-eid (schema/create-dummy-job conn :user user :memory mem :cpus cpus)
                      task-eid (schema/create-dummy-instance conn job-eid :instance-status :instance.status/running :hostname host)]))

            _ (doseq [x (range pending-jobs-sample-size)]
                (let [[[user mem cpus]] (gen/sample pending-job-gen 1)
                      job-eid (schema/create-dummy-job conn :user user :memory mem :cpus cpus)]))

            db (d/db conn)

            pending-job-ents (util/get-pending-job-ents db)
            [pending-job-ents-to-run task-ents-to-preempt] (time (rebalancer/rebalance db pending-job-ents {} {:max-preemption 128 :safe-dru-threshold 1.0 :min-dru-diff 0.5 :category :normal}))]))))


(deftest test-update-datomic-params-via-config!
  (let [datomic-uri "datomic:mem://test-init-state"
        conn (schema/restore-fresh-database! datomic-uri)
        all-params {:min-utilization-threshold 0.75
                    :safe-dru-threshold 1.0
                    :min-dru-diff 0.5
                    :max-preemption 64.0}
        updated-params {:min-dru-diff 0.75 :max-preemption 128.0}
        merged-params (merge all-params updated-params)]

    (testing "no config"
      (rebalancer/update-datomic-params-from-config! conn nil)
      (is (= (rebalancer/read-datomic-params conn) {})))

    (testing "all config params specified"
      (rebalancer/update-datomic-params-from-config! conn all-params)
      (is (= (rebalancer/read-datomic-params conn) all-params))

    (testing "partial config"
      (rebalancer/update-datomic-params-from-config! conn updated-params)
      (is (= (rebalancer/read-datomic-params conn) merged-params)))

    (testing "unrecognized config params discarded"
      (rebalancer/update-datomic-params-from-config! conn {:foo "bar" :ding 2})
      (is (= (rebalancer/read-datomic-params conn) merged-params))))))


(comment (run-tests))
