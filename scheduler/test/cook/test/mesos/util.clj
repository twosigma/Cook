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
(ns cook.test.mesos.util
 (:use clojure.test)
 (:require [cook.mesos.util :as util]
           [clj-time.core :as t]
           [clj-time.coerce :as tc]
           [cook.test.testutil :as testutil :refer (create-dummy-instance create-dummy-job restore-fresh-database!)]
           [datomic.api :as d :refer (q db)]))

(deftest test-get-pending-job-ents
  (let [uri "datomic:mem://test-get-pending-job-ents"
        conn (restore-fresh-database! uri)]
    (create-dummy-job conn :user "u1" :job-state :job.state/waiting)
    (create-dummy-job conn :user "u1" :job-state :job.state/running)
    (create-dummy-job conn :user "u2" :job-state :job.state/waiting)
    (create-dummy-job conn :user "u1" :job-state :job.state/waiting)
    (is (= 3 (count (util/get-pending-job-ents (db conn)))))))

(deftest test-filter-sequential
  ; Same as filter
  (is (util/filter-sequential (fn [state x]
                            [state (even? x)])
                          {}
                          (range 10))
      (filter even? (range 10)))
  ;; Check lazy
  (is (take 100 (util/filter-sequential (fn [state x]
                            [state (even? x)])
                          {}
                          (range)))
      (take 100 (filter even? (range)))
      )
  ;; Check with state
  ;; Take first 100 odd numbers. Take even numbers after first 100
  (is (take 200
            (util/filter-sequential
              (fn [state x]
                (let [state' (merge-with + state {:even (mod (inc x) 2) ;if x even this is 1
                                                  :odd (mod x 2) ;if x odd, this is 1
                                                  })]
                  [state' (or (and (even? x) (> (:even state) 100))
                              (and (not (even? x)) (< (:odd state) 100)))]))
                                    {:even 0
                                     :odd 0}
                                    (range)))
      (take 200
            (concat (take 100 (filter odd? (range)))
                    (drop 100 (filter even? (range)))))))

(deftest test-task-run-time
  (let [uri "datomic:mem://test-task-run-time"
        conn (restore-fresh-database! uri)]
    (testing "task complete"
      (let [expected-run-time (t/hours 3)
            start (t/ago expected-run-time)
            end (t/now)
            job (create-dummy-job conn :user "tsram" :job-state :job.state/completed :retry-count 3)
            task-entid (create-dummy-instance conn 
                                              job 
                                              :instance-status :instance.status/failed
                                              :start-time (tc/to-date start)
                                              :end-time (tc/to-date end))
            task-ent (d/entity (d/db conn) task-entid)]
        (is (= (t/in-seconds expected-run-time) 
               (t/in-seconds (util/task-run-time task-ent))))))
    (testing "task running"
      (let [expected-run-time (t/hours 3)
            start (t/ago expected-run-time)
            job (create-dummy-job conn :user "tsram" :job-state :job.state/completed :retry-count 3)
            task-entid (create-dummy-instance conn 
                                              job 
                                              :instance-status :instance.status/running
                                              :start-time (tc/to-date start))
            task-ent (d/entity (d/db conn) task-entid)]
        (is (= (t/in-seconds expected-run-time) 
               (t/in-seconds (util/task-run-time task-ent))))))))

(deftest test-attempts-consumed
  (let [uri "datomic:mem://test-attempts-consumed"
        conn (restore-fresh-database! uri)] 
    (testing "No mea-culpa reasons"
      (let [job (create-dummy-job conn :user "tsram" :job-state :job.state/completed :retry-count 3)
           _ (create-dummy-instance conn job :instance-status :instance.status/failed
                                    :reason :unknown) ; Counted
           _ (create-dummy-instance conn job :instance-status :instance.status/failed
                                    :reason :unknown) ; Counted
           _ (create-dummy-instance conn job :instance-status :instance.status/running) ; Not counted
           job-ent (d/entity @(d/sync conn) job)]
        (is (= (util/job-ent->attempts-consumed job-ent) 2))
       ))
    (testing "Some mea-culpa reasons"
      (let [job (create-dummy-job conn :user "tsram" :job-state :job.state/completed :retry-count 3)
           _ (create-dummy-instance conn job :instance-status :instance.status/failed
                                    :reason :unknown) ; Counted
           _ (create-dummy-instance conn job :instance-status :instance.status/failed
                                    :reason :preempted-by-rebalancer) ; Not counted
           _ (create-dummy-instance conn job :instance-status :instance.status/failed
                                    :reason :preempted-by-rebalancer) ; Not counted
           _ (create-dummy-instance conn job :instance-status :instance.status/running) ; Not counted
           job-ent (d/entity @(d/sync conn) job)]
        (is (= (util/job-ent->attempts-consumed job-ent) 1))
       ))
    (testing "All mea-culpa reasons"
      (let [job (create-dummy-job conn :user "tsram" :job-state :job.state/completed :retry-count 3)
           _ (create-dummy-instance conn job :instance-status :instance.status/failed
                                    :reason :preempted-by-rebalancer) ; Not counted
           _ (create-dummy-instance conn job :instance-status :instance.status/failed
                                    :reason :preempted-by-rebalancer) ; Not counted
           _ (create-dummy-instance conn job :instance-status :instance.status/running) ; Not counted
           job-ent (d/entity @(d/sync conn) job)]
        (is (= (util/job-ent->attempts-consumed job-ent) 0))
       ))
    (testing "Some nil reasons"
      (let [job (create-dummy-job conn :user "tsram" :job-state :job.state/completed :retry-count 3)
           _ (create-dummy-instance conn job :instance-status :instance.status/failed
                                    :reason :unknown) ; Counted
           _ (create-dummy-instance conn job :instance-status :instance.status/failed) ; Counted
           _ (create-dummy-instance conn job :instance-status :instance.status/failed
                                    :reason :preempted-by-rebalancer) ; Not counted
           _ (create-dummy-instance conn job :instance-status :instance.status/running) ; Not counted
           job-ent (d/entity @(d/sync conn) job)]
        (is (= (util/job-ent->attempts-consumed job-ent) 2))
       ))
    (testing "Finished running job"
      (let [job (create-dummy-job conn :user "tsram" :job-state :job.state/completed :retry-count 3)
           _ (create-dummy-instance conn job :instance-status :instance.status/success) ; Counted
           _ (create-dummy-instance conn job :instance-status :instance.status/failed) ; Counted
           _ (create-dummy-instance conn job :instance-status :instance.status/failed
                                    :reason :preempted-by-rebalancer) ; Not counted
           _ (create-dummy-instance conn job :instance-status :instance.status/running) ; Not counted
           job-ent (d/entity @(d/sync conn) job)]
        (is (= (util/job-ent->attempts-consumed job-ent) 2))
       ))
    (testing "Finished job"
      (let [job (create-dummy-job conn :user "tsram" :job-state :job.state/completed :retry-count 3)
           _ (create-dummy-instance conn job :instance-status :instance.status/success) ; Counted
           _ (create-dummy-instance conn job :instance-status :instance.status/failed) ; Counted
           _ (create-dummy-instance conn job :instance-status :instance.status/failed
                                    :reason :preempted-by-rebalancer) ; Not counted
           job-ent (d/entity @(d/sync conn) job)]
        (is (= (util/job-ent->attempts-consumed job-ent) 2))
       ))
    
    ))

(comment (run-tests))
