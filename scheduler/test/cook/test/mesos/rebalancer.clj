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
  (:require [clojure.core.cache :as cache]
            [clojure.test.check.generators :as gen]
            [cook.mesos.dru :as dru]
            [cook.mesos.rebalancer :as rebalancer :refer (->State)]
            [cook.mesos.scheduler :as sched]
            [cook.mesos.share :as share]
            [cook.mesos.util :as util]
            [cook.test.testutil :refer (restore-fresh-database! create-dummy-job create-dummy-instance
                                                                create-dummy-group init-agent-attributes-cache)]
            [datomic.api :as d :refer (q)])
  (:import (com.netflix.fenzo SimpleAssignmentResult TaskRequest)))

(defn create-running-job
  [conn host & args]
  (let [job (apply create-dummy-job (cons conn args))
        inst (create-dummy-instance conn job :instance-status :instance.status/running :hostname host)]
  [job inst]))

(defn- update-agent-attributes-cache!
  [agent-attributes-cache-atom slave-id props]
  (swap! agent-attributes-cache-atom (fn [c]
                                       (if (cache/has? c slave-id)
                                         (cache/hit c slave-id)
                                         (cache/miss c slave-id props)))))

(defn create-and-cache-running-job
  [conn hostname agent-attributes-cache hostname->props & args]
  (let [job (apply create-dummy-job (cons conn args))
        inst (create-dummy-instance conn job :instance-status :instance.status/running :hostname hostname)
        slave-id (:instance/slave-id (d/entity (d/db conn) inst))]
  (update-agent-attributes-cache! agent-attributes-cache slave-id (get hostname->props hostname))
  [job inst]))

(deftest test-init-state
  (testing "test1"
    (let [datomic-uri "datomic:mem://test-init-state"
          conn (restore-fresh-database! datomic-uri)
          job1 (create-dummy-job conn :user "ljin" :memory 10.0 :ncpus 10.0)
          job2 (create-dummy-job conn :user "ljin" :memory 5.0  :ncpus 5.0)
          job3 (create-dummy-job conn :user "ljin" :memory 15.0 :ncpus 25.0)
          job4 (create-dummy-job conn :user "ljin" :memory 25.0 :ncpus 15.0)
          job5 (create-dummy-job conn :user "wzhao" :memory 8.0 :ncpus 8.0)
          job6 (create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)
          job7 (create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)
          job8 (create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)

          task1 (create-dummy-instance conn job1 :instance-status :instance.status/running)
          task2 (create-dummy-instance conn job2 :instance-status :instance.status/running)
          task3 (create-dummy-instance conn job3 :instance-status :instance.status/running)
          task4 (create-dummy-instance conn job4 :instance-status :instance.status/running)
          task5 (create-dummy-instance conn job5 :instance-status :instance.status/running)
          task6 (create-dummy-instance conn job6 :instance-status :instance.status/running)
          task7 (create-dummy-instance conn job7 :instance-status :instance.status/running)
          task8 (create-dummy-instance conn job8 :instance-status :instance.status/running)

          task-ent1 (d/entity (d/db conn) task1)
          task-ent2 (d/entity (d/db conn) task2)
          task-ent3 (d/entity (d/db conn) task3)
          task-ent4 (d/entity (d/db conn) task4)
          task-ent5 (d/entity (d/db conn) task5)
          task-ent6 (d/entity (d/db conn) task6)
          task-ent7 (d/entity (d/db conn) task7)
          task-ent8 (d/entity (d/db conn) task8)]
      (let [_ (share/set-share! conn "default" nil
                                "Limits for new cluster"
                                :mem 25.0 :cpus 25.0 :gpus 1.0)
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
            pool-ent {:pool/name "no-pool"
                      :pool/dru-mode :pool.dru-mode/default}
            {:keys [task->scored-task user->sorted-running-task-ents]}
            (rebalancer/init-state db running-tasks pending-jobs {} pool-ent)]
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

(deftest test-compute-pending-default-job-dru
  (testing "test1"
    (let [datomic-uri "datomic:mem://test-compute-pending-default-job-dru"
          conn (restore-fresh-database! datomic-uri)
          job1 (create-dummy-job conn :name "job1" :user "ljin" :memory 10.0 :ncpus 10.0)
          job2 (create-dummy-job conn :name "job2" :user "ljin" :memory 5.0 :ncpus 5.0)
          job3 (create-dummy-job conn :name "job3" :user "ljin" :memory 15.0 :ncpus 25.0)
          job4 (create-dummy-job conn :name "job4" :user "ljin" :memory 25.0 :ucpus 15.0)
          job5 (create-dummy-job conn :name "job5" :user "wzhao" :memory 8.0 :ncpus 8.0)
          job6 (create-dummy-job conn :name "job6" :user "wzhao" :memory 10.0 :ncpus 10.0)
          job7 (create-dummy-job conn :name "job7" :user "wzhao" :memory 10.0 :ncpus 10.0)
          job8 (create-dummy-job conn :name "job8" :user "wzhao" :memory 10.0 :ncpus 10.0)

          job9 (create-dummy-job conn :name "job9" :user "wzhao" :memory 10.0 :ncpus 10.0)
          job10 (create-dummy-job conn :name "job10" :user "sunil" :memory 20.0 :ncpus 20.0)
          job11 (create-dummy-job conn :name "job11" :user "ljin" :memory 10.0 :ucpus 10.0)

          _ (create-dummy-instance conn job1 :instance-status :instance.status/running)
          _ (create-dummy-instance conn job2 :instance-status :instance.status/running)
          _ (create-dummy-instance conn job3 :instance-status :instance.status/running)
          _ (create-dummy-instance conn job4 :instance-status :instance.status/running)
          _ (create-dummy-instance conn job5 :instance-status :instance.status/running)
          _ (create-dummy-instance conn job6 :instance-status :instance.status/running)
          _ (create-dummy-instance conn job7 :instance-status :instance.status/running)
          _ (create-dummy-instance conn job8 :instance-status :instance.status/running)

          _ (share/set-share! conn "default" nil
                              "limits for new cluster"
                              :mem 25.0 :cpus 25.0 :gpus 1.0)

          db (d/db conn)
          running-tasks (util/get-running-task-ents db)
          pending-jobs (map #(d/entity db %) [job9 job10 job11])
          pool-ent {:pool/name "no-pool"
                    :pool/dru-mode :pool.dru-mode/default}
          state (rebalancer/init-state db running-tasks pending-jobs {} pool-ent)]
      (is (= 1.92 (rebalancer/compute-pending-default-job-dru state "no-pool" (d/entity db job9))))
      (is (= 0.8 (rebalancer/compute-pending-default-job-dru state "no-pool"(d/entity db job10))))
      (is (= 2.6 (rebalancer/compute-pending-default-job-dru state "no-pool" (d/entity db job11)))))))

(deftest test-pending-gpu-job-dru
  (let [datomic-uri "datomic:mem://test-rebalancer/compute-pending-normal-job-dru"
        conn (restore-fresh-database! datomic-uri)
        job1 (create-dummy-job conn :name "job1" :user "ljin" :memory 10.0 :ncpus 10.0 :gpus 1.0)
        job2 (create-dummy-job conn :name "job2" :user "ljin" :memory 5.0  :ncpus 5.0 :gpus 1.0)
        job3 (create-dummy-job conn :name "job3" :user "ljin" :memory 15.0 :ncpus 25.0 :gpus 1.0)
        job4 (create-dummy-job conn :name "job4"  :user "ljin" :memory 25.0 :ucpus 15.0 :gpus 1.0)
        job5 (create-dummy-job conn :name "job5" :user "wzhao" :memory 8.0 :ncpus 8.0 :gpus 1.0)
        job6 (create-dummy-job conn :name "job6" :user "wzhao" :memory 10.0 :ncpus 10.0 :gpus 1.0)
        job7 (create-dummy-job conn :name "job7" :user "wzhao" :memory 10.0 :ncpus 10.0 :gpus 1.0)
        job8 (create-dummy-job conn :name "job8" :user "wzhao" :memory 10.0 :ncpus 10.0 :gpus 1.0)

        job9 (create-dummy-job conn :name "job9" :user "wzhao" :memory 10.0 :ncpus 10.0 :gpus 1.0)
        job10 (create-dummy-job conn :name "job10" :user "sunil" :memory 20.0 :ncpus 20.0 :gpus 1.0)
        job11 (create-dummy-job conn :name "job11" :user "ljin" :memory 10.0 :ucpus 10.0 :gpus 2.0)

        _ (create-dummy-instance conn job1 :instance-status :instance.status/running)
        _ (create-dummy-instance conn job2 :instance-status :instance.status/running)
        _ (create-dummy-instance conn job3 :instance-status :instance.status/running)
        _ (create-dummy-instance conn job4 :instance-status :instance.status/running)
        _ (create-dummy-instance conn job5 :instance-status :instance.status/running)
        _ (create-dummy-instance conn job6 :instance-status :instance.status/running)
        _ (create-dummy-instance conn job7 :instance-status :instance.status/running)
        _ (create-dummy-instance conn job8 :instance-status :instance.status/running)

        _ (share/set-share! conn "default" nil
                            "limits for new cluster"
                            :mem 25.0 :cpus 25.0 :gpus 1.0)

        db (d/db conn)
        running-tasks (util/get-running-task-ents db)
        pending-jobs (map #(d/entity db %) [job9 job10 job11])
        pool-ent {:pool/name "no-pool"
                  :pool/dru-mode :pool.dru-mode/gpu}
        state (rebalancer/init-state db running-tasks pending-jobs {} pool-ent)]
    (is (= (rebalancer/compute-pending-gpu-job-dru state "no-pool" (d/entity db job2))
           (rebalancer/compute-pending-gpu-job-dru state "no-pool" (d/entity db job6))))
    (is (= 5.0 (rebalancer/compute-pending-gpu-job-dru state "no-pool" (d/entity db job9))))
    (is (= 1.0 (rebalancer/compute-pending-gpu-job-dru state "no-pool" (d/entity db job10))))
    (is (= 6.0 (rebalancer/compute-pending-gpu-job-dru state "no-pool" (d/entity db job11))))))

(defn initialize-rebalancer
  [db pending-job-ids]
  (let [pending-jobs (map #(d/entity db %) pending-job-ids)
        running-tasks (util/get-running-task-ents db)
        pool-ent {:pool/name "no-pool"
                  :pool/dru-mode :pool.dru-mode/default}
        {:keys [task->scored-task user->sorted-running-task-ents user->dru-divisors]}
        (rebalancer/init-state db running-tasks pending-jobs {} pool-ent)]
    [task->scored-task user->sorted-running-task-ents user->dru-divisors]))

(deftest test-compute-preemption-decision
  (testing "test without group constraints"
    (let [datomic-uri "datomic:mem://test-compute-preemption-decision"
          conn (restore-fresh-database! datomic-uri)
          cotask-cache (atom (cache/fifo-cache-factory {} :threshold 100))
          job1 (create-dummy-job conn :user "ljin" :memory 10.0 :ncpus 10.0)
          job2 (create-dummy-job conn :user "ljin" :memory 5.0  :ncpus 5.0)
          job3 (create-dummy-job conn :user "ljin" :memory 15.0 :ncpus 25.0)
          job4 (create-dummy-job conn :user "ljin" :memory 25.0 :ncpus 15.0)
          job7 (create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)
          job8 (create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)

          job9 (create-dummy-job conn :user "wzhao" :memory 15.0 :ncpus 15.0)
          job10 (create-dummy-job conn :user "sunil" :memory 15.0 :ncpus 15.0)
          job11 (create-dummy-job conn :user "ljin" :memory 15.0 :ncpus 15.0)
          job12 (create-dummy-job conn :user "sunil" :memory 40.0 :ncpus 40.0)
          job13 (create-dummy-job conn :user "sunil" :memory 45.0 :ncpus 45.0)
          job14 (create-dummy-job conn :user "sunil" :memory 80.0 :ncpus 80.0)

          _ (create-dummy-instance conn job1 :instance-status :instance.status/running :hostname "hostA")
          _ (create-dummy-instance conn job2 :instance-status :instance.status/running :hostname "hostA")
          task3 (create-dummy-instance conn job3 :instance-status :instance.status/running :hostname "hostB")
          task4 (create-dummy-instance conn job4 :instance-status :instance.status/running :hostname "hostB")
          task7 (create-dummy-instance conn job7 :instance-status :instance.status/running :hostname "hostA")
          task8 (create-dummy-instance conn job8 :instance-status :instance.status/running :hostname "hostB")

          task-ent3 (d/entity (d/db conn) task3)
          task-ent4 (d/entity (d/db conn) task4)
          task-ent7 (d/entity (d/db conn) task7)
          task-ent8 (d/entity (d/db conn) task8)

          _ (share/set-share! conn "default" nil
                              "limits for new cluster"
                              :mem 25.0 :cpus 25.0 :gpus 1.0)

          agent-attributes-cache (init-agent-attributes-cache)
          db (d/db conn)
          pending-job-ids (list job9 job10 job11 job12 job13 job14)
          [task->scored-task user->sorted-running-task-ents user->dru-divisors]
            (initialize-rebalancer db pending-job-ids)]

      (comment all-decisions-without-spare-resources
        {"hostA" [{:dru 1.12 :task [task-ent7] :mem 10.0 :cpus 10.0}]
         "hostB" [{:dru 2.2 :task [task-ent4] :mem 25.0 :cpus 15.0}
                  {:dru 1.6 :task [task-ent4 task-ent3] :mem 40.0 :cpus 40.0}
                  {:dru 1.52 :task [task-ent4 task-ent3 task-ent8] :mem 50.0 :cpus 50.0}]})

      (is (= {:hostname "hostB" :dru 2.2 :task [task-ent4] :mem 25.0 :cpus 15.0 :gpus 0.0}
             (rebalancer/compute-preemption-decision db agent-attributes-cache
                                                     (->State task->scored-task
                                                              user->sorted-running-task-ents
                                                              {}
                                                              user->dru-divisors
                                                              rebalancer/compute-pending-default-job-dru
                                                              [])
                                                     {:min-dru-diff 0.05 :safe-dru-threshold 1.0}
                                                     "no-pool"
                                                     (d/entity db job9)
                                                     cotask-cache)))

      (is (= {:hostname "hostB" :dru Double/MAX_VALUE :task nil :mem 15.0 :cpus 15.0 :gpus 0.0}
             (rebalancer/compute-preemption-decision db agent-attributes-cache
                                                     (->State task->scored-task
                                                              user->sorted-running-task-ents
                                                              {"hostB" {:mem 15.0 :cpus 15.0}}
                                                              user->dru-divisors
                                                              rebalancer/compute-pending-default-job-dru
                                                              [])
                                                     {:min-dru-diff 0.5 :safe-dru-threshold 1.0}
                                                     "no-pool"
                                                     (d/entity db job9)
                                                     cotask-cache)))

      (is (= {:hostname "hostA" :dru Double/MAX_VALUE :task nil :mem 20.0 :cpus 20.0 :gpus 0.0}
             (rebalancer/compute-preemption-decision db agent-attributes-cache
                                                     (->State task->scored-task
                                                              user->sorted-running-task-ents
                                                              {"hostA" {:mem 20.0 :cpus 20.0}
                                                               "hostB" {:mem 10.0 :cpus 10.0}}
                                                              user->dru-divisors
                                                              rebalancer/compute-pending-default-job-dru
                                                              [])
                                                     {:min-dru-diff 0.5 :safe-dru-threshold 1.0}
                                                     "no-pool"
                                                     (d/entity db job9)
                                                     cotask-cache)))

      (is (= {:hostname "hostB" :dru 2.2 :task [task-ent4] :mem 35.0 :cpus 25.0 :gpus 0.0}
             (rebalancer/compute-preemption-decision db agent-attributes-cache
                                                     (->State task->scored-task
                                                              user->sorted-running-task-ents
                                                              {"hostA" {:mem 10.0 :cpus 10.0}
                                                               "hostB" {:mem 10.0 :cpus 10.0}}
                                                              user->dru-divisors
                                                              rebalancer/compute-pending-default-job-dru
                                                              [])
                                                     {:min-dru-diff 0.0 :safe-dru-threshold 1.0}
                                                     "no-pool"
                                                     (d/entity db job9)
                                                     cotask-cache)))

      (is (= {:hostname "hostB" :dru 2.2 :task [task-ent4] :mem 25.0 :cpus 15.0 :gpus 0.0}
             (rebalancer/compute-preemption-decision db agent-attributes-cache
                                                     (->State task->scored-task
                                                              user->sorted-running-task-ents
                                                              {}
                                                              user->dru-divisors
                                                              rebalancer/compute-pending-default-job-dru
                                                              [])
                                                     {:min-dru-diff 0.5 :safe-dru-threshold 1.0}
                                                     "no-pool"
                                                     (d/entity db job10)
                                                     cotask-cache)))

      (is (= nil
             (rebalancer/compute-preemption-decision db agent-attributes-cache
                                                     (->State task->scored-task
                                                              user->sorted-running-task-ents
                                                              {}
                                                              user->dru-divisors
                                                              rebalancer/compute-pending-default-job-dru
                                                              [])
                                                     {:min-dru-diff 0.5 :safe-dru-threshold 1.0}
                                                     "no-pool"
                                                     (d/entity db job11)
                                                     cotask-cache)))

      (is (= nil
             (rebalancer/compute-preemption-decision db agent-attributes-cache
                                                     (->State task->scored-task
                                                              user->sorted-running-task-ents
                                                              {}
                                                              user->dru-divisors
                                                              rebalancer/compute-pending-default-job-dru
                                                              [])
                                                     {:min-dru-diff 0.0 :safe-dru-threshold 1.0}
                                                     "no-pool"
                                                     (d/entity db job12)
                                                     cotask-cache)))

      (is (= {:hostname "hostA" :dru Double/MAX_VALUE :task nil :mem 40.0 :cpus 40.0 :gpus 0.0}
             (rebalancer/compute-preemption-decision db agent-attributes-cache
                                                     (->State task->scored-task
                                                              user->sorted-running-task-ents
                                                              {"hostA" {:mem 40.0 :cpus 40.0}}
                                                              user->dru-divisors
                                                              rebalancer/compute-pending-default-job-dru
                                                              [])
                                                     {:min-dru-diff 0.5 :safe-dru-threshold 1.0}
                                                     "no-pool"
                                                     (d/entity db job12)
                                                     cotask-cache)))

      (is (= nil
             (rebalancer/compute-preemption-decision db agent-attributes-cache
                                                     (->State task->scored-task
                                                              user->sorted-running-task-ents
                                                              {"hostA" {:mem 35.0 :cpus 35.0}}
                                                              user->dru-divisors
                                                              rebalancer/compute-pending-default-job-dru
                                                              [])
                                                     {:min-dru-diff 0.0 :safe-dru-threshold 1.0}
                                                     "no-pool"
                                                     (d/entity db job12)
                                                     cotask-cache)))

      (is (= {:hostname "hostB" :dru 2.2 :task [task-ent4] :mem 55.0 :cpus 45.0 :gpus 0.0}
             (rebalancer/compute-preemption-decision db agent-attributes-cache
                                                     (->State task->scored-task
                                                              user->sorted-running-task-ents
                                                              {"hostA" {:mem 35.0 :cpus 35.0}
                                                               "hostB" {:mem 30.0 :cpus 30.0}}
                                                              user->dru-divisors
                                                              rebalancer/compute-pending-default-job-dru
                                                              [])
                                                     {:min-dru-diff 0.5 :safe-dru-threshold 1.0}
                                                     "no-pool"
                                                     (d/entity db job12)
                                                     cotask-cache)))

      (is (= nil
             (rebalancer/compute-preemption-decision db agent-attributes-cache
                                                     (->State task->scored-task
                                                              user->sorted-running-task-ents
                                                              {}
                                                              user->dru-divisors
                                                              rebalancer/compute-pending-default-job-dru
                                                              [])
                                                     {:min-dru-diff 0.5 :safe-dru-threshold 1.0}
                                                     "no-pool"
                                                     (d/entity db job13)
                                                     cotask-cache)))

      (is (= nil
             (rebalancer/compute-preemption-decision db agent-attributes-cache
                                                     (->State task->scored-task
                                                              user->sorted-running-task-ents
                                                              {}
                                                              user->dru-divisors
                                                              rebalancer/compute-pending-default-job-dru
                                                              [])
                                                     {:min-dru-diff 2.0 :safe-dru-threshold 1.0}
                                                     "no-pool"
                                                     (d/entity db job13)
                                                     cotask-cache)))

      (is (= nil
             (rebalancer/compute-preemption-decision db agent-attributes-cache
                                                     (->State task->scored-task
                                                              user->sorted-running-task-ents
                                                              {}
                                                              user->dru-divisors
                                                              rebalancer/compute-pending-default-job-dru
                                                              [])
                                                     {:min-dru-diff 0.5 :safe-dru-threshold 1.0}
                                                     "no-pool"
                                                     (d/entity db job14)
                                                     cotask-cache)))))
  (testing "test preemption with novel-host job constraint"
    (let [datomic-uri "datomic:mem://test-compute-preemption-decision"
          conn (restore-fresh-database! datomic-uri)
          user->host {"pig1" "straw"
                      "pig2" "sticks"
                      "pig3" "bricks"
                      "pig4" "rebar"}
          hostname->props {"straw" {"HOSTNAME" "straw"}
                       "sticks" {"HOSTNAME" "sticks"}
                       "bricks" {"HOSTNAME" "bricks"}
                       "rebar" {"HOSTNAME" "rebar"}}
          agent-attributes-cache (init-agent-attributes-cache)
          cotask-cache (atom (cache/fifo-cache-factory {} :threshold 100))]
      (share/set-share! conn "default" nil "new cluster settings"
                        :mem 10.0 :cpus 10.0 :gpus 10.0)
      ; Fill up hosts with preemptable tasks
      (doall (for [[user host] user->host]
               (create-and-cache-running-job conn host agent-attributes-cache hostname->props
                   :user user
                   :ncpus 100.0)))
      (testing "try placing a job that has failed everywhere except rebar"
        (let [pending-job (create-dummy-job conn :user "diego" :ncpus 1.0)
              _ (doall (map #(create-dummy-instance conn pending-job
                              :hostname %
                              :instance-status :instance.status/failed
                              :job-state :job.state/waiting) ["straw" "sticks" "bricks"]))
              db (d/db conn)
              [task->scored-task user->sorted-running-task-ents user->dru-divisors]
                (initialize-rebalancer db (list pending-job))]
          (is (= "rebar"
                (:hostname (rebalancer/compute-preemption-decision db agent-attributes-cache
                                                                   (->State task->scored-task
                                                                            user->sorted-running-task-ents
                                                                            {}
                                                                            user->dru-divisors
                                                                            rebalancer/compute-pending-default-job-dru
                                                                            [])
                                                                   {:min-dru-diff 0.05 :safe-dru-threshold 1.0}
                                                                   "no-pool"
                                                                   (d/entity db pending-job)
                                                                   cotask-cache))))))

      (testing "try placing a job that has been preempted everywhere, but not failed on rebar"
        (let [pending-job (create-dummy-job conn :user "diego" :ncpus 1.0)
              _ (doall (map #(create-dummy-instance conn pending-job
                              :hostname %
                              :instance-status :instance.status/failed
                              :job-state :job.state/waiting) ["straw" "sticks" "bricks"]))
              _ (doall (map #(create-dummy-instance conn pending-job
                              :hostname %
                              :instance-status :instance.status/failed
                              :preempted? true
                              :job-state :job.state/waiting) ["straw" "sticks" "bricks" "rebar"]))
              db (d/db conn)
              [task->scored-task user->sorted-running-task-ents user->dru-divisors]
                (initialize-rebalancer db (list pending-job))]
          (is (= "rebar"
                (:hostname (rebalancer/compute-preemption-decision db agent-attributes-cache
                                                                   (->State task->scored-task
                                                                            user->sorted-running-task-ents
                                                                            {}
                                                                            user->dru-divisors
                                                                            rebalancer/compute-pending-default-job-dru
                                                                            [])
                                                                   {:min-dru-diff 0.05 :safe-dru-threshold 1.0}
                                                                   "no-pool"
                                                                   (d/entity db pending-job)
                                                                   cotask-cache))))))
      (testing "try placing an unconstrained job"
        (let [pending-job (create-dummy-job conn :user "diego" :ncpus 1.0)
              db (d/db conn)
              [task->scored-task user->sorted-running-task-ents user->dru-divisors]
                (initialize-rebalancer db (list pending-job))]
          (is (not (nil?
                 (:hostname (rebalancer/compute-preemption-decision db agent-attributes-cache
                                                                 (->State task->scored-task
                                                                          user->sorted-running-task-ents
                                                                          {}
                                                                          user->dru-divisors
                                                                          rebalancer/compute-pending-default-job-dru
                                                                          [])
                                                                 {:min-dru-diff 0.05 :safe-dru-threshold 1.0}
                                                                 "no-pool"
                                                                 (d/entity db pending-job)
                                                                 cotask-cache)))))))))

  (testing "test preemption with unique host-placement group constraint"
    (let [datomic-uri "datomic:mem://test-compute-preemption-decision"
          conn (restore-fresh-database! datomic-uri)
          user->host {"pig1" "straw"
                      "pig2" "sticks"
                      "pig3" "bricks"
                      "pig4" "rebar"}
          hostname->props {"straw" {"HOSTNAME" "straw"}
                       "sticks" {"HOSTNAME" "sticks"}
                       "bricks" {"HOSTNAME" "bricks"}
                       "rebar" {"HOSTNAME" "rebar"}}
          agent-attributes-cache (init-agent-attributes-cache)
          cotask-cache (atom (cache/fifo-cache-factory {} :threshold 100))]
      (share/set-share! conn "default" nil "new cluster settings"
                        :mem 10.0 :cpus 10.0 :gpus 1.0)
      ; Fill up hosts with preemptable tasks
      (doall (for [[user host] user->host]
               (do (create-and-cache-running-job conn host agent-attributes-cache hostname->props :user user :ncpus 100.0))))
      (testing "try placing job with one unconstrained-host available"
        (let [; Make a group with unique host-placement and with jobs in all but one host (rebar)
              group-hosts ["straw" "sticks" "bricks"]
              group-id (create-dummy-group conn :host-placement {:host-placement/type :host-placement.type/unique})
              _  (doall (map #(create-running-job conn % :user "diego" :ncpus 1.0 :group group-id) group-hosts))
              pending-job (create-dummy-job conn :user "diego" :ncpus 1.0 :group group-id)
              db (d/db conn)
              [task->scored-task user->sorted-running-task-ents user->dru-divisors]
                (initialize-rebalancer db (list pending-job))]
          (is (= "rebar"
                (:hostname (rebalancer/compute-preemption-decision db agent-attributes-cache
                                                                   (->State task->scored-task
                                                                            user->sorted-running-task-ents
                                                                            {}
                                                                            user->dru-divisors
                                                                            rebalancer/compute-pending-default-job-dru
                                                                            [])
                                                                   {:min-dru-diff 0.05 :safe-dru-threshold 1.0}
                                                                   "no-pool"
                                                                   (d/entity db pending-job)
                                                                   cotask-cache))))))

      (testing "try placing job with no unconstrained-hosts available"
        (let [; Make a group with all available hosts occupied
              group-hosts ["straw" "sticks" "bricks" "rebar"]
              group-id (create-dummy-group conn :host-placement {:host-placement/type :host-placement.type/unique})
              _ (doall (map #(create-running-job conn % :user "diego" :ncpus 1.0 :group group-id) group-hosts))
              pending-job (create-dummy-job conn :user "diego" :ncpus 1.0 :group group-id)
              db (d/db conn)
              [task->scored-task user->sorted-running-task-ents user->dru-divisors]
                (initialize-rebalancer db (list pending-job))]
          (is (= nil
                (rebalancer/compute-preemption-decision db agent-attributes-cache
                                                        (->State task->scored-task
                                                                 user->sorted-running-task-ents
                                                                 {}
                                                                 user->dru-divisors
                                                                 rebalancer/compute-pending-default-job-dru
                                                                 [])
                                                        {:min-dru-diff 0.05 :safe-dru-threshold 1.0}
                                                        "no-pool"
                                                        (d/entity db pending-job)
                                                        cotask-cache)))))))

  (testing "test preemption with attribute-equals host-placement group constraint"
    (let [datomic-uri "datomic:mem://test-compute-preemption-decision"
          conn (restore-fresh-database! datomic-uri)
          attr-name "az"
          ; Create jobs that could be preempted
          user->host {"pig1" "straw"
                      "pig2" "sticks"
                      "pig3" "bricks"
                      "pig4" "rebar"}
          hostname->props {"straw" {"az" "east" "HOSTNAME" "straw"}
                           "sticks" {"az" "east" "HOSTNAME" "sticks"}
                           "bricks" {"az" "east" "HOSTNAME" "bricks"}
                           "rebar" {"az" "west" "HOSTNAME" "rebar"}}
          agent-attributes-cache (init-agent-attributes-cache)
          cotask-cache (atom (cache/fifo-cache-factory {} :threshold 100))]
      (share/set-share! conn "default" nil "new cluster limits"
                        :mem 10.0 :cpus 10.0 :gpus 1.0)
      ; Fill up hosts with preemptable tasks
      (doall (for [[user host] user->host]
               (do (create-and-cache-running-job conn host agent-attributes-cache hostname->props :user user :ncpus 100.0))))

      (testing "try placing job with one unconstrained-host available"
        (let [group-id (create-dummy-group conn :host-placement
                         {:host-placement/type :host-placement.type/attribute-equals
                          :host-placement/parameters {:host-placement.attribute-equals/attribute attr-name}})
              [running-job running-task] (create-running-job conn "steel" :user "diego" :ncpus 1.0 :group group-id)
              _ (update-agent-attributes-cache! agent-attributes-cache (:instance/slave-id (d/entity (d/db conn) running-task))
                                          {attr-name "west" "HOSTNAME" "steel"})
              ; Pending job will need to run in host with attr-name=west
              pending-job (create-dummy-job conn :user "diego" :ncpus 1.0 :group group-id)
              db (d/db conn)
              [task->scored-task user->sorted-running-task-ents user->dru-divisors]
                (initialize-rebalancer db (list pending-job))]
          (is (= "rebar"
                (:hostname (rebalancer/compute-preemption-decision db agent-attributes-cache
                                                                   (->State task->scored-task
                                                                            user->sorted-running-task-ents
                                                                            {}
                                                                            user->dru-divisors
                                                                            rebalancer/compute-pending-default-job-dru
                                                                            [])
                                                                   {:min-dru-diff 0.05 :safe-dru-threshold 1.0}
                                                                   "no-pool"
                                                                   (d/entity db pending-job)
                                                                   cotask-cache))))))

      (testing "try placing job with no unconstrained-hosts available"
        (let [group-id (create-dummy-group conn :host-placement
                         {:host-placement/type :host-placement.type/attribute-equals
                          :host-placement/parameters {:host-placement.attribute-equals/attribute attr-name}})
              [running-job running-task] (create-running-job conn "steel" :user "diego" :ncpus 1.0 :group group-id)
              _ (update-agent-attributes-cache! agent-attributes-cache (:instance/slave-id (d/entity (d/db conn) running-task))
                                          {attr-name "south" "HOSTNAME" "steel"}) 
              ; Pending job will need to run in hist with attr-name=south, but no such host exists
              pending-job (create-dummy-job conn :user "diego" :ncpus 1.0 :group group-id)
              db (d/db conn)
              [task->scored-task user->sorted-running-task-ents user->dru-divisors]
                (initialize-rebalancer db (list pending-job))]
          (is (= nil
                (rebalancer/compute-preemption-decision db agent-attributes-cache
                                                        (->State task->scored-task
                                                                 user->sorted-running-task-ents
                                                                 {}
                                                                 user->dru-divisors
                                                                 rebalancer/compute-pending-default-job-dru
                                                                 [])
                                                        {:min-dru-diff 0.05 :safe-dru-threshold 1.0}
                                                        "no-pool"
                                                        (d/entity db pending-job)
                                                        cotask-cache)))))))

  (testing "test preemption with balanced host-placement group constraint"
    (let [datomic-uri "datomic:mem://test-compute-preemption-decision"
          attr-name "az"
          ; Create jobs that could be preempted
          user->host {"pig1" "straw"
                      "pig2" "sticks"
                      "pig3" "bricks"
                      "pig4" "rebar"
                      "pig5" "concrete"
                      "pig6" "steel"
                      "pig7" "gold"
                      "pig8" "titanium"}
          hostname->props {"straw" {"az" "east" "HOSTNAME" "straw"}
                           "sticks" {"az" "west" "HOSTNAME" "sticks"}
                           "bricks" {"az" "south" "HOSTNAME" "bricks"}
                           "rebar" {"az" "north" "HOSTNAME" "rebar"}
                           "concrete" {"az" "east" "HOSTNAME" "concrete"}
                           "steel" {"az" "west" "HOSTNAME" "steel"}
                           "gold" {"az" "south" "HOSTNAME" "gold"}
                           "titanium" {"az" "north" "HOSTNAME" "titanium"}}
          cotask-cache (atom (cache/fifo-cache-factory {} :threshold 100))]

      (testing "try placing job with one unconstrained-host available"
        (let [conn (restore-fresh-database! datomic-uri)
              _ (share/set-share! conn "default" nil "new cluster limits"
                                  :mem 20.0 :cpus 20.0 :gpus 1.0)
              agent-attributes-cache (init-agent-attributes-cache)
              ; Fill up hosts with preemptable tasks
              preemptable (doall
                            (for [[user host] user->host]
                              (do (create-and-cache-running-job conn host agent-attributes-cache hostname->props :user user :ncpus 200.0))))
              group-id (create-dummy-group conn :host-placement
                         {:host-placement/type :host-placement.type/balanced
                          :host-placement/parameters {:host-placement.balanced/attribute attr-name
                                                      :host-placement.balanced/minimum 4}})
              group-hosts ["straw" "sticks" "bricks" "rebar" "concrete" "steel" "gold" "titanium" "straw" "sticks" "bricks" "rebar" "concrete" "steel" "gold"]
              ; Each az has 2 jobs running, except north. Pending job can only go in a host with az north, either rebar
              ; or titanium
              _ (doall (map #(create-and-cache-running-job conn % agent-attributes-cache hostname->props
                                                           :user "diego" :ncpus 1.0 :group group-id) group-hosts))
              pending-job (create-dummy-job conn :user "diego" :ncpus 1.0 :group group-id)
              db (d/db conn)
              [task->scored-task user->sorted-running-task-ents user->dru-divisors]
                (initialize-rebalancer db (list pending-job))]
          (let [preempted-host (:hostname (rebalancer/compute-preemption-decision
                                             db
                                             agent-attributes-cache
                                             (->State task->scored-task
                                                      user->sorted-running-task-ents
                                                      {}
                                                      user->dru-divisors
                                                      rebalancer/compute-pending-default-job-dru
                                                      [])
                                             {:min-dru-diff 0.05 :safe-dru-threshold 1.0}
                                             "no-pool"
                                             (d/entity db pending-job)
                                             cotask-cache))]
            (is (true? (or (= "titanium" preempted-host) (= "rebar" preempted-host)))))))

      (testing "try placing job with two unconstrained hosts available, but one has already been preempted this cycle"
        (let [conn (restore-fresh-database! datomic-uri)
              _ (share/set-share! conn "default" nil "new cluster limits"
                                  :mem 20.0 :cpus 20.0 :gpus 1.0)
              agent-attributes-cache (init-agent-attributes-cache)
              ; Fill up hosts with preemptable tasks
              preemptable (doall
                            (for [[user host] user->host]
                              (do (create-and-cache-running-job conn host agent-attributes-cache hostname->props :user user :ncpus 200.0))))
              group-id (create-dummy-group conn :host-placement
                         {:host-placement/type :host-placement.type/balanced
                          :host-placement/parameters {:host-placement.balanced/attribute attr-name
                                                      :host-placement.balanced/minimum 4}})
              ; Each az has 2 jobs running, except north (rebar and titanium) and south (gold and bricks).
              ; north and south only have 1 job running.
              group-hosts ["straw" "sticks" "bricks" "rebar" "concrete" "steel" "gold" "titanium" "straw" "sticks" "bricks" "rebar" "concrete" "steel"]
              _ (doall (map #(create-and-cache-running-job conn % agent-attributes-cache hostname->props
                                                           :user "diego" :ncpus 1.0 :group group-id) group-hosts))
              ; But the task preempted this cycle is titanium (last preemptable), therefore only
              ; gold and bricks are open for further preemption.
              task-preempted-this-cycle (d/entity (d/db conn) (second (last preemptable)))
              pending-job (create-dummy-job conn :user "diego" :ncpus 1.0 :group group-id)
              db (d/db conn)
              [task->scored-task user->sorted-running-task-ents user->dru-divisors]
                (initialize-rebalancer db (list pending-job))]
          (let [preempted-host (:hostname (rebalancer/compute-preemption-decision
                                             db
                                             agent-attributes-cache
                                             (->State task->scored-task
                                                      user->sorted-running-task-ents
                                                      {}
                                                      user->dru-divisors
                                                      rebalancer/compute-pending-default-job-dru
                                                      [task-preempted-this-cycle])
                                             {:min-dru-diff 0.05 :safe-dru-threshold 1.0}
                                             "no-pool"
                                             (d/entity db pending-job)
                                             cotask-cache))]
            (is (true? (or (= "bricks" preempted-host) (= "gold" preempted-host))))))))))

(deftest test-next-state
  (testing "test1"
    (let [datomic-uri "datomic:mem://test-next-state"
          conn (restore-fresh-database! datomic-uri)
          job1 (create-dummy-job conn :user "ljin" :memory 10.0 :ncpus 10.0 :name "job1")
          job2 (create-dummy-job conn :user "ljin" :memory 5.0 :ncpus 5.0 :name "job2")
          job3 (create-dummy-job conn :user "ljin" :memory 15.0 :ncpus 25.0 :name "job3")
          job4 (create-dummy-job conn :user "ljin" :memory 25.0 :ncpus 15.0 :name "job4")
          job5 (create-dummy-job conn :user "wzhao" :memory 8.0 :ncpus 8.0 :name "job5")
          job6 (create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0 :name "job6")
          job7 (create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0 :name "job7")
          job8 (create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0 :name "job8")

          job9 (create-dummy-job conn :user "wzhao" :memory 15.0 :ncpus 15. :name "job9")
          job10 (create-dummy-job conn :user "sunil" :memory 15.0 :ncpus 15.0 :name "job10")
          job11 (create-dummy-job conn :user "ljin" :memory 15.0 :ncpus 15.0 :name "jo11")
          job12 (create-dummy-job conn :user "sunil" :memory 40.0 :ncpus 40.0 :name "job12")
          job13 (create-dummy-job conn :user "sunil" :memory 45.0 :ncpus 45.0 :name "job13")
          job14 (create-dummy-job conn :user "sunil" :memory 80.0 :ncpus 80.0 :name "job14")

          task1 (create-dummy-instance conn
                                       job1
                                       :instance-status :instance.status/running
                                       :slave-id "testA"
                                       :hostname "hostA")
          task2 (create-dummy-instance conn
                                       job2
                                       :instance-status :instance.status/running
                                       :slave-id "testA"
                                       :hostname "hostA")
          task3 (create-dummy-instance conn
                                       job3
                                       :instance-status :instance.status/running
                                       :slave-id "testB"
                                       :hostname "hostB")
          task4 (create-dummy-instance conn
                                       job4
                                       :instance-status :instance.status/running
                                       :slave-id "testB"
                                       :hostname "hostB")
          task5 (create-dummy-instance conn
                                       job5
                                       :instance-status :instance.status/running
                                       :slave-id "testA"
                                       :hostname "hostA")
          task6 (create-dummy-instance conn
                                       job6
                                       :instance-status :instance.status/running
                                       :slave-id "testB"
                                       :hostname "hostB")
          task7 (create-dummy-instance conn
                                       job7
                                       :instance-status :instance.status/running
                                       :slave-id "testA"
                                       :hostname "hostA")
          task8 (create-dummy-instance conn
                                       job8
                                       :instance-status :instance.status/running
                                       :slave-id "testB"
                                       :hostname "hostB")
          _ (share/set-share! conn "default" nil
                              "limits for new cluster"
                              :mem 25.0 :cpus 25.0 :gpus 1.0)
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
          pool-ent {:pool/name "no-pool"
                    :pool/dru-mode :pool.dru-mode/default}
          state (rebalancer/init-state db running-task-ents pending-job-ents host->spare-resources pool-ent)]
      (let [task-ent9 {:job/_instance job-ent9
                       :instance/hostname "hostB"
                       :instance/slave-id "testB"
                       :instance/status :instance.status/running}
            user->sorted-running-task-ents' {"ljin" (into (sorted-set-by (util/same-user-task-comparator)) [task-ent1 task-ent2 task-ent3 task-ent4])
                                               "wzhao" (into (sorted-set-by (util/same-user-task-comparator)) [task-ent5 task-ent7 task-ent9])}
            host->spare-resources' {"hostA" {:mem 50.0 :cpus 50.0} "hostB" {:mem 5.0 :cpus 5.0 :gpus 0.0}}]
        (let [{task->scored-task'' :task->scored-task
               user->sorted-running-task-ents'' :user->sorted-running-task-ents
               host->spare-resources'' :host->spare-resources
               user->dru-divisors'' :user->dru-divisors}
              (rebalancer/next-state state db job-ent9 {:hostname "hostB" :task [task-ent6 task-ent8] :mem 20.0 :cpus 20.0 :gpus 0.0})]
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
                        :instance/slave-id "testA"
                        :instance/hostname "hostA"
                        :instance/status :instance.status/running}
            user->sorted-running-task-ents' {"ljin" (into (sorted-set-by (util/same-user-task-comparator)) [task-ent1 task-ent3 task-ent4])
                                               "wzhao" (into (sorted-set-by (util/same-user-task-comparator)) [task-ent5 task-ent6 task-ent8])
                                               "sunil" (into (sorted-set-by (util/same-user-task-comparator)) [task-ent10])}
            host->spare-resources' {"hostA" {:mem 50.0 :cpus 50.0 :gpus 0.0}}]
        (let [{task->scored-task'' :task->scored-task
               user->sorted-running-task-ents'' :user->sorted-running-task-ents
               host->spare-resources'' :host->spare-resources}
              (rebalancer/next-state state db job-ent10 {:hostname "hostA" :task [task-ent2 task-ent7] :mem 65.0 :cpus 65.0})]
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
            user->sorted-running-task-ents' {"ljin" (into (sorted-set-by (util/same-user-task-comparator)) [task-ent1 task-ent2 task-ent3 task-ent4])
                                               "wzhao" (into (sorted-set-by (util/same-user-task-comparator)) [task-ent5 task-ent6 task-ent7 task-ent8])
                                               "sunil" (into (sorted-set-by (util/same-user-task-comparator)) [task-ent12])}
            host->spare-resources' {"hostA" {:mem 10.0 :cpus 10.0 :gpus 0.0}}]

        (let [{task->scored-task'' :task->scored-task
               user->sorted-running-task-ents'' :user->sorted-running-task-ents
               host->spare-resources'' :host->spare-resources}
              (rebalancer/next-state state db job-ent12 {:hostname "hostA" :task [] :mem 50.0 :cpus 50.0})]
          (is (= user->sorted-running-task-ents' user->sorted-running-task-ents''))
          (is (= host->spare-resources' host->spare-resources''))
          ;; If these tests break, know that the ordering for equal dru tasks is undefined..
          (is (= [task-ent4 task-ent3 task-ent12 task-ent8 task-ent7 task-ent6 task-ent2 task-ent1 task-ent5]
                 (keys task->scored-task'')))
          (is (= [(dru/->ScoredTask task-ent4 2.2 25.0 15.0)
                  (dru/->ScoredTask task-ent3 1.6 15.0 25.0)
                  (dru/->ScoredTask task-ent12 1.6 40.0 40.0)
                  (dru/->ScoredTask task-ent8 1.52 10.0 10.0)
                  (dru/->ScoredTask task-ent7 1.12 10.0 10.0)
                  (dru/->ScoredTask task-ent6 0.72 10.0 10.0)
                  (dru/->ScoredTask task-ent2 0.6 5.0 5.0)
                  (dru/->ScoredTask task-ent1 0.4 10.0 10.0)
                  (dru/->ScoredTask task-ent5 0.32 8.0 8.0)]
                 (vals task->scored-task'')))
          (let [scored (vals task->scored-task'')
                expected [(dru/->ScoredTask task-ent4 2.2 25.0 15.0)
                          (dru/->ScoredTask task-ent3 1.6 15.0 25.0)
                          (dru/->ScoredTask task-ent12 1.6 40.0 40.0)
                          (dru/->ScoredTask task-ent8 1.52 10.0 10.0)
                          (dru/->ScoredTask task-ent7 1.12 10.0 10.0)
                          (dru/->ScoredTask task-ent6 0.72 10.0 10.0)
                          (dru/->ScoredTask task-ent2 0.6 5.0 5.0)
                          (dru/->ScoredTask task-ent1 0.4 10.0 10.0)
                          (dru/->ScoredTask task-ent5 0.32 8.0 8.0)]
                scored->expected (zipmap scored expected)]
            (doall (for [[scored expected] scored->expected]
              (is (= scored expected))))))))))

(defn rebalance
  "Calculates the jobs to make for and the initial state, and then delegates to rebalancer/rebalance"
  [db agent-attributes-cache pending-job-ents host->spare-resources rebalancer-reservation-atom
   {:keys [max-preemption pool-ent] :as params}]
  (let [jobs-to-make-room-for (->> pending-job-ents
                                   (filter (partial util/job-allowed-to-start? db))
                                   (take max-preemption))
        init-state (rebalancer/init-state db (util/get-running-task-ents db) jobs-to-make-room-for
                                          host->spare-resources pool-ent)]
    (rebalancer/rebalance db agent-attributes-cache rebalancer-reservation-atom
                          params init-state jobs-to-make-room-for (:pool/name pool-ent))))

(deftest test-rebalance
  (let [datomic-uri "datomic:mem://test-rebalance"
        conn (restore-fresh-database! datomic-uri)
        job1 (create-dummy-job conn :user "ljin" :memory 10.0 :ncpus 10.0)
        job2 (create-dummy-job conn :user "ljin" :memory 5.0 :ncpus 5.0)
        job3 (create-dummy-job conn :user "ljin" :memory 15.0 :ncpus 25.0)
        job4 (create-dummy-job conn :user "ljin" :memory 25.0 :ncpus 15.0)
        job5 (create-dummy-job conn :user "wzhao" :memory 8.0 :ncpus 8.0)
        job6 (create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)
        job7 (create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)
        job8 (create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)

        _ (create-dummy-instance conn job1 :instance-status :instance.status/running :hostname "hostA")
        _ (create-dummy-instance conn job2 :instance-status :instance.status/running :hostname "hostA")
        task3 (create-dummy-instance conn job3 :instance-status :instance.status/running :hostname "hostB")
        task4 (create-dummy-instance conn job4 :instance-status :instance.status/running :hostname "hostB")
        _ (create-dummy-instance conn job5 :instance-status :instance.status/running :hostname "hostA")
        _ (create-dummy-instance conn job6 :instance-status :instance.status/running :hostname "hostB")
        _ (create-dummy-instance conn job7 :instance-status :instance.status/running :hostname "hostA")
        task8 (create-dummy-instance conn job8 :instance-status :instance.status/running :hostname "hostB")
        _ (share/set-share! conn "default" nil
                            "limits for new cluster"
                            :mem 25.0 :cpus 25.0)

        db (d/db conn)

        task-ent3 (d/entity db task3)
        task-ent4 (d/entity db task4)
        task-ent8 (d/entity db task8)

        job9 (create-dummy-job conn :user "wzhao" :memory 5.0 :ncpus 5.0)
        job10 (create-dummy-job conn :user "wzhao" :memory 5.0 :ncpus 5.0)
        job11 (create-dummy-job conn :user "wzhao" :memory 5.0 :ncpus 5.0)
        job12 (create-dummy-job conn :user "wzhao" :memory 5.0 :ncpus 5.0)
        job13 (create-dummy-job conn :user "wzhao" :memory 5.0 :ncpus 5.0)
        job14 (create-dummy-job conn :user "wzhao" :memory 5.0 :ncpus 5.0)
        job15 (create-dummy-job conn :user "wzhao" :memory 5.0 :ncpus 5.0)
        job16 (create-dummy-job conn :user "wzhao" :memory 5.0 :ncpus 5.0)
        job17 (create-dummy-job conn :user "wzhao" :memory 5.0 :ncpus 5.0)
        job18 (create-dummy-job conn :user "wzhao" :memory 5.0 :ncpus 5.0)

        job19 (create-dummy-job conn :user "sunil" :memory 5.0 :ncpus 5.0)
        job20 (create-dummy-job conn :user "sunil" :memory 5.0 :ncpus 5.0)
        job21 (create-dummy-job conn :user "sunil" :memory 5.0 :ncpus 5.0)
        job22 (create-dummy-job conn :user "sunil" :memory 5.0 :ncpus 5.0)
        job23 (create-dummy-job conn :user "sunil" :memory 5.0 :ncpus 5.0)
        job24 (create-dummy-job conn :user "sunil" :memory 5.0 :ncpus 5.0)
        job25 (create-dummy-job conn :user "sunil" :memory 5.0 :ncpus 5.0)
        job26 (create-dummy-job conn :user "sunil" :memory 5.0 :ncpus 5.0)
        job27 (create-dummy-job conn :user "sunil" :memory 5.0 :ncpus 5.0)
        job28 (create-dummy-job conn :user "sunil" :memory 5.0 :ncpus 5.0)

        pool-ent {:pool/dru-mode :pool.dru-mode/default
                  :pool/name "no-pool"}
        test-cases [{:jobs [job9 job10 job11 job12 job13
                            job14 job15 job16 job17 job18]
                     :params {:max-preemption 128 :safe-dru-threshold 1.0 :min-dru-diff 0.0 :pool-ent pool-ent}
                     :expected-jobs-to-run [job9 job10 job11]
                     :expected-tasks-to-preempt [task-ent4]
                     :available-resources {}
                     :test-name "simple test"}
                    {:jobs [job9 job10 job11 job12 job13
                            job14 job15 job16 job17 job18]
                     :params {:max-preemption 128 :safe-dru-threshold 1.0 :min-dru-diff 0.0 :pool-ent pool-ent}
                     :expected-jobs-to-run [job9 job10 job11 job12 job13]
                     :expected-tasks-to-preempt [task-ent4]
                     :available-resources {"hostB" {:mem 0.0 :cpus 10.0}}
                     :test-name "simple test with available resources"}
                    {:jobs [job19 job20 job21 job22 job23
                            job24 job25 job26 job27 job28]
                     :params {:max-preemption 128 :safe-dru-threshold 1.0 :min-dru-diff 0.0 :pool-ent pool-ent}
                     :expected-jobs-to-run [job19 job20 job21 job22 job23
                                            job24 job25 job26]
                     :expected-tasks-to-preempt [task-ent4 task-ent3]
                     :available-resources {}
                     :test-name "simple test 2"}
                    {:jobs [job19 job20 job21 job22 job23
                            job24 job25 job26 job27 job28]
                     :params {:max-preemption 128 :safe-dru-threshold 1.0 :min-dru-diff 0.0 :pool-ent pool-ent}
                     :expected-jobs-to-run [job19 job20 job21 job22 job23
                                            job24 job25 job26]
                     :expected-tasks-to-preempt [task-ent4]
                     :available-resources {"hostB" {:cpus 25.0 :mem 25.0}}
                     :test-name "simple test 2 with available resources"}
                    {:jobs [job19 job20 job21 job22 job23
                            job24 job25 job26 job27 job28]
                     :params {:max-preemption 128 :safe-dru-threshold 1.0 :min-dru-diff 0.0 :pool-ent pool-ent}
                     :expected-jobs-to-run [job19 job20 job21 job22 job23
                                            job24 job25 job26 job27 job28]
                     :expected-tasks-to-preempt [task-ent4 task-ent3 task-ent8]
                     :available-resources {}
                     :share-updates [{:user "sunil" :mem 50.0 :cpus 50.0}]
                     :test-name "test with share change"}]]

(doseq  [{:keys [jobs params expected-jobs-to-run expected-tasks-to-preempt available-resources share-updates test-name]}
         test-cases]
  (testing test-name
    (doseq [{:keys [user mem cpus]} share-updates]
      (share/set-share! conn user nil
                        "test update"
                        :mem mem :cpus cpus))
    (let [db (d/db conn)
          agent-attributes-cache (init-agent-attributes-cache)
          pending-job-ents (map #(d/entity db %) jobs)
          preemption-decisions (rebalance db agent-attributes-cache pending-job-ents
                                          available-resources (atom {}) params)
          pending-job-ents-to-run (map :to-make-room-for preemption-decisions)
          task-ents-to-preempt (mapcat :task preemption-decisions)]
      (is (= (map #(d/entity db %) expected-jobs-to-run)
             pending-job-ents-to-run))
      (is (= expected-tasks-to-preempt
             task-ents-to-preempt)))))))

(comment
        {task-ent1 0.4
         task-ent2 0.6
         task-ent3 1.6
         task-ent4 2.2
         task-ent5 0.32
         task-ent6 0.72
         task-ent7 1.12
         task-ent8 1.52})

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
          pending-job-gen  (gen/tuple pending-user-gen mem-gen cpus-gen)]
      (let [conn (restore-fresh-database! datomic-uri)
            agent-attributes-cache (init-agent-attributes-cache)
            _ (share/set-share! conn "default" nil
                                "limits for new cluster"
                                :mem 1024.0 :cpus Double/MAX_VALUE)
            running-tasks-sample-size 10240
            pending-jobs-sample-size 1024

            _ (doseq [_ (range running-tasks-sample-size)]
                (let [[[user mem cpus]] (gen/sample running-job-gen 1)
                      [host] (gen/sample host-gen 1)
                      job-eid (create-dummy-job conn :user user :memory mem :cpus cpus)
                      _ (create-dummy-instance conn job-eid :instance-status :instance.status/running :hostname host)]))

            _ (doseq [_ (range pending-jobs-sample-size)]
                (let [[[user mem cpus]] (gen/sample pending-job-gen 1)
                      _ (create-dummy-job conn :user user :memory mem :cpus cpus)]))

            db (d/db conn)

            pending-job-ents (util/get-pending-job-ents db)
            pool-ent {:pool/dru-mode :pool.dru-mode/default}]
        (rebalance db agent-attributes-cache pending-job-ents {} (atom {})
                   {:max-preemption 128, :pool-ent pool-ent})))))


(deftest test-update-datomic-params-via-config!
  (let [datomic-uri "datomic:mem://test-init-state"
        conn (restore-fresh-database! datomic-uri)
        all-params {:safe-dru-threshold 1.0
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

(deftest test-rebalance-host-reservation
  (testing "reserves host for multiple preemptions"
    (let [datomic-uri "datomic:mem://test-rebalance-host-reservation"
          conn (restore-fresh-database! datomic-uri)
          pool-ent {:pool/name "no-pool"
                    :pool/dru-mode :pool.dru-mode/default}
          params {:max-preemption 128 :safe-dru-threshold 1.0 :min-dru-diff 0.0 :pool-ent pool-ent}
          agent-attributes-cache (init-agent-attributes-cache)
          job1 (create-dummy-job conn :user "user1" :memory 6.0 :ncpus 6.0)
          job2 (create-dummy-job conn :user "user1" :memory 6.0 :ncpus 6.0)
          job4 (create-dummy-job conn :user "user2" :memory 10.0 :ncpus 10.0)
          task1 (create-dummy-instance conn
                                       job1
                                       :instance-status :instance.status/running
                                       :hostname "hostA")
          task2 (create-dummy-instance conn
                                       job2
                                       :instance-status :instance.status/running
                                       :hostname "hostA")
          reservations (atom {})]
      (share/set-share! conn "user1" nil
                        "test update"
                        :mem 1.0 :cpus 1.0)
      (share/set-share! conn "user2" nil
                        "big user"
                        :mem 10.0 :cpus 10.0)
      (let [db (d/db conn)
            [{:keys [hostname task to-make-room-for]}]
            (rebalance db agent-attributes-cache (util/get-pending-job-ents db)
                       {"hostA" {:cpus 0.0 :mem 0.0 :gpus 0.0}} reservations params)]
        (is (= "hostA" hostname))
        (is (= job4 (:db/id to-make-room-for)))
        (is (= [task1 task2] (map :db/id task)))
        (is (= {(:job/uuid (d/entity db job4)) "hostA"}
               (:job-uuid->reserved-host @reservations)))
        (is (= #{} (:launched-job-uuids @reservations))))))

  (testing "does not reserve host for single preempted task"
    (let [datomic-uri "datomic:mem://test-rebalance-host-reservation-single"
          conn (restore-fresh-database! datomic-uri)
          pool-ent {:pool/name "no-pool"
                    :pool/dru-mode :pool.dru-mode/default}
          params {:max-preemption 128 :safe-dru-threshold 1.0 :min-dru-diff 0.0 :pool-ent pool-ent}
          agent-attributes-cache (init-agent-attributes-cache)
          job1 (create-dummy-job conn :user "user1" :memory 6.0 :ncpus 6.0)
          job2 (create-dummy-job conn :user "user2" :memory 1.0 :ncpus 1.0)
          task1 (create-dummy-instance conn
                                       job1
                                       :instance-status :instance.status/running
                                       :hostname "hostA")
          reservations (atom {})]
      (share/set-share! conn "user1" nil
                        "test update"
                        :mem 1.0 :cpus 1.0)
      (share/set-share! conn "user2" nil
                        "test update"
                        :mem 1.0 :cpus 1.0)
      (let [db (d/db conn)
            [{:keys [hostname task to-make-room-for]}]
            (rebalance db agent-attributes-cache (util/get-pending-job-ents db)
                       {"hostA" {:cpus 0.0 :mem 0.0 :gpus 0.0}} reservations params)]
        (is (= "hostA" hostname))
        (is (= job2 (:db/id to-make-room-for)))
        (is (= [task1] (map :db/id task)))
        (is (= {} (:job-uuid->reserved-host @reservations)))
        (is (= #{} (:launched-job-uuids @reservations))))))

  (testing "does not reserve host for job already launched"
    (let [datomic-uri "datomic:mem://test-rebalance-host-reservation"
          conn (restore-fresh-database! datomic-uri)
          pool-ent {:pool/name "no-pool"
                    :pool/dru-mode :pool.dru-mode/default}
          params {:max-preemption 128 :safe-dru-threshold 1.0 :min-dru-diff 0.0 :pool-ent pool-ent}
          agent-attributes-cache (init-agent-attributes-cache)
          job1 (create-dummy-job conn :user "user1" :memory 6.0 :ncpus 6.0)
          job2 (create-dummy-job conn :user "user1" :memory 6.0 :ncpus 6.0)
          job4 (create-dummy-job conn :user "user2" :memory 10.0 :ncpus 10.0)
          task1 (create-dummy-instance conn
                                       job1
                                       :instance-status :instance.status/running
                                       :hostname "hostA")
          task2 (create-dummy-instance conn
                                       job2
                                       :instance-status :instance.status/running
                                       :hostname "hostA")
          reservations (atom {:launched-job-uuids [(:job/uuid (d/entity (d/db conn) job4))]})]
      (share/set-share! conn "user1" nil
                        "test update"
                        :mem 1.0 :cpus 1.0)
      (share/set-share! conn "user2" nil
                        "big user"
                        :mem 10.0 :cpus 10.0)
      (let [db (d/db conn)
            [{:keys [hostname task to-make-room-for]}]
            (rebalance db agent-attributes-cache (util/get-pending-job-ents db)
                       {"hostA" {:cpus 0.0 :mem 0.0 :gpus 0.0}} reservations params)]
        (is (= "hostA" hostname))
        (is (= job4 (:db/id to-make-room-for)))
        (is (= [task1 task2] (map :db/id task)))
        (is (= {} (:job-uuid->reserved-host @reservations)))
        (is (= #{} (:launched-job-uuids @reservations)))))))

(deftest test-reserve-hosts
  (testing "only reserves hosts with multiple preemptions"
    (let [decisions [{:task ["a" "b"]
                      :hostname "hostA"
                      :to-make-room-for {:job/uuid "jobA"}}
                     {:task ["c"]
                      :hostname "hostB"
                      :to-make-room-for {:job/uuid "jobB"}}]
          rebalancer-reservation-atom (atom {})]
      (rebalancer/reserve-hosts! rebalancer-reservation-atom decisions)
      (is (= #{} (:launched-job-uuids @rebalancer-reservation-atom)))
      (is (= {"jobA" "hostA"} (:job-uuid->reserved-host @rebalancer-reservation-atom)))))

  (testing "does not reserve hosts for jobs that have already launched"
    (let [decisions [{:task ["a" "b"]
                      :hostname "hostA"
                      :to-make-room-for {:job/uuid "jobA"}}]
          rebalancer-reservation-atom (atom {:launched-job-uuids #{"jobA"}})]
      (rebalancer/reserve-hosts! rebalancer-reservation-atom decisions)
      (is (= #{} (:launched-job-uuids @rebalancer-reservation-atom)))
      (is (= {} (:job-uuid->reserved-host @rebalancer-reservation-atom))))))

(deftest test-reserve-hosts-integration
  (testing "does not reserve another host after launching job"
    (let [datomic-uri "datomic:mem://test-reserve-hosts-integration"
          conn (restore-fresh-database! datomic-uri)
          job-id (create-dummy-job conn :user "user1" :memory 6.0 :ncpus 6.0)
          {:keys [job/uuid] :as job} (d/entity (d/db conn) job-id)
          first-decision [{:task ["a" "b"]
                           :hostname "hostA"
                           :to-make-room-for job}]
          second-decision [{:task ["a" "b"]
                            :hostname "hostB"
                            :to-make-room-for job}]
          rebalancer-reservation-atom (atom {})]
      (rebalancer/reserve-hosts! rebalancer-reservation-atom first-decision)
      (is (= {uuid "hostA"} (:job-uuid->reserved-host @rebalancer-reservation-atom)))
      (is (= #{} (:launched-job-uuids @rebalancer-reservation-atom)))

      (sched/update-host-reservations! rebalancer-reservation-atom #{uuid})
      (is (= {} (:job-uuid->reserved-host @rebalancer-reservation-atom)))
      (is (= #{uuid} (:launched-job-uuids @rebalancer-reservation-atom)))

      (rebalancer/reserve-hosts! rebalancer-reservation-atom second-decision)
      (is (= {}) (:job-uuid->reserved-host @rebalancer-reservation-atom))
      (is (= #{}) (:launched-job-uuids @rebalancer-reservation-atom)))))

(comment (run-tests))
