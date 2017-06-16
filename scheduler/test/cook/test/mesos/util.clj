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
 (:require [clj-time.coerce :as tc]
           [clj-time.core :as t]
           [clojure.core.async :as async]
           [cook.mesos.util :as util]
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
           db (d/db conn)
           job-ent (d/entity @(d/sync conn) job)]
        (is (= (util/job-ent->attempts-consumed db job-ent) 2))
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
           db (d/db conn)
           job-ent (d/entity @(d/sync conn) job)]
        (is (= (util/job-ent->attempts-consumed db job-ent) 1))
       ))
    (testing "All mea-culpa reasons"
      (let [job (create-dummy-job conn :user "tsram" :job-state :job.state/completed :retry-count 3)
           _ (create-dummy-instance conn job :instance-status :instance.status/failed
                                    :reason :preempted-by-rebalancer) ; Not counted
           _ (create-dummy-instance conn job :instance-status :instance.status/failed
                                    :reason :preempted-by-rebalancer) ; Not counted
           _ (create-dummy-instance conn job :instance-status :instance.status/running) ; Not counted
           db (d/db conn)
           job-ent (d/entity @(d/sync conn) job)]
        (is (= (util/job-ent->attempts-consumed db job-ent) 0))
       ))
    (testing "Some nil reasons"
      (let [job (create-dummy-job conn :user "tsram" :job-state :job.state/completed :retry-count 3)
           _ (create-dummy-instance conn job :instance-status :instance.status/failed
                                    :reason :unknown) ; Counted
           _ (create-dummy-instance conn job :instance-status :instance.status/failed) ; Counted
           _ (create-dummy-instance conn job :instance-status :instance.status/failed
                                    :reason :preempted-by-rebalancer) ; Not counted
           _ (create-dummy-instance conn job :instance-status :instance.status/running) ; Not counted
           db (d/db conn)
           job-ent (d/entity @(d/sync conn) job)]
        (is (= (util/job-ent->attempts-consumed db job-ent) 2))
       ))
    (testing "Finished running job"
      (let [job (create-dummy-job conn :user "tsram" :job-state :job.state/completed :retry-count 3)
           _ (create-dummy-instance conn job :instance-status :instance.status/success) ; Counted
           _ (create-dummy-instance conn job :instance-status :instance.status/failed) ; Counted
           _ (create-dummy-instance conn job :instance-status :instance.status/failed
                                    :reason :preempted-by-rebalancer) ; Not counted
           _ (create-dummy-instance conn job :instance-status :instance.status/running) ; Not counted
           db (d/db conn)
           job-ent (d/entity @(d/sync conn) job)]
        (is (= (util/job-ent->attempts-consumed db job-ent) 2))
       ))
    (testing "Finished job"
      (let [job (create-dummy-job conn :user "tsram" :job-state :job.state/completed :retry-count 3)
           _ (create-dummy-instance conn job :instance-status :instance.status/success) ; Counted
           _ (create-dummy-instance conn job :instance-status :instance.status/failed) ; Counted
           _ (create-dummy-instance conn job :instance-status :instance.status/failed
                                    :reason :preempted-by-rebalancer) ; Not counted
           db (d/db conn)
           job-ent (d/entity @(d/sync conn) job)]
        (is (= (util/job-ent->attempts-consumed db job-ent) 2))
       ))

    ))

(deftest test-namespace-datomic
  (testing "Example tests"
    (is (= (util/namespace-datomic :straggler-handling :type)
           :straggler-handling/type))
    (is (= (util/namespace-datomic :straggler-handling :type :quantile-deviation)
           :straggler-handling.type/quantile-deviation))))

(deftest test-clear-uncommitted-jobs
  (testing "no uncommitted"
    (let [uri "datomic:mem://test-clear-uncommitted"
          conn (restore-fresh-database! uri)
          _ (dotimes [_ 100]
              (create-dummy-job conn))]
      (is (= (count (util/clear-uncommitted-jobs conn (t/yesterday) true)) 0))))
  (testing "uncommitted but not before"
    (let [uri "datomic:mem://test-clear-uncommitted"
          conn (restore-fresh-database! uri)
          _ (dotimes [_ 100]
              (create-dummy-job conn))
          _ (dotimes [_ 100]
              (create-dummy-job conn
                                :committed? false
                                :submit-time (tc/to-date (-> 6 t/hours t/ago))))]
      (is (= (count (util/clear-uncommitted-jobs conn (t/yesterday) true)) 0))))
  (testing "uncommitted dry-run"
    (let [uri "datomic:mem://test-clear-uncommitted"
          conn (restore-fresh-database! uri)
          _ (dotimes [_ 100]
              (create-dummy-job conn))
          _ (dotimes [_ 100]
              (create-dummy-job conn
                                :committed? false
                                :submit-time (tc/to-date (-> 6 t/hours t/ago))))
          uncommitted-count 25
          _ (dotimes [_ uncommitted-count]
              (create-dummy-job conn
                                :committed? false
                                :submit-time (tc/to-date (-> 48 t/hours t/ago))))]
      (is (= (count (util/clear-uncommitted-jobs conn (t/yesterday) true))
             uncommitted-count))))
  (testing "uncommitted with retract"
    (let [uri "datomic:mem://test-clear-uncommitted"
          conn (restore-fresh-database! uri)
          _ (dotimes [_ 100]
              (create-dummy-job conn))
          _ (dotimes [_ 100]
              (create-dummy-job conn
                                :committed? false
                                :submit-time (tc/to-date (-> 6 t/hours t/ago))))
          uncommitted-count 25
          _ (dotimes [_ uncommitted-count]
              (create-dummy-job conn
                                :committed? false
                                :submit-time (tc/to-date (-> 48 t/hours t/ago))))]
      (is (= (count (util/clear-uncommitted-jobs conn (t/yesterday) false))
             uncommitted-count))
      (is (= (count (util/clear-uncommitted-jobs conn (t/yesterday) false)) 0)))))

(deftest test-make-guuid->juuids
  (let [jobs [{:job/uuid "1"
               :group/_job #{{:group/uuid "a"} {:group/uuid "b"} {:group/uuid "c"} {:group/uuid "d"} {:group/uuid "e"}}}
              {:job/uuid "2"
               :group/_job #{{:group/uuid "b"} {:group/uuid "c"} {:group/uuid "d"} {:group/uuid "e"}}}
              {:job/uuid "3"
               :group/_job #{{:group/uuid "c"} {:group/uuid "d"} {:group/uuid "e"}}}
              {:job/uuid "4"
               :group/_job #{{:group/uuid "d"} {:group/uuid "e"}}}
              {:job/uuid "5"
               :group/_job #{{:group/uuid "e"}}}
              {:job/uuid "6"
               :group/_job #{}}]
        guuid->juuids (util/make-guuid->juuids jobs)]
    (is (= (guuid->juuids "a") #{"1"}))
    (is (= (guuid->juuids "b") #{"1" "2"}))
    (is (= (guuid->juuids "c") #{"1" "2" "3"}))
    (is (= (guuid->juuids "d") #{"1" "2" "3" "4"}))
    (is (= (guuid->juuids "e") #{"1" "2" "3" "4" "5"}))))

(deftest test-read-chan
  (is (= (count (util/read-chan (async/chan) 10)) 0))
  (let [ch (async/chan 10)]
    (async/<!! (async/onto-chan ch [1 2 3 4 5] false))
    (is (= (count (util/read-chan ch 10)) 5))
    (is (= (count (util/read-chan ch 10)) 0))
    (async/<!! (async/onto-chan ch (range 10) false))
    (let [oc (async/onto-chan ch (range 10) false)]
      (is (= (count (util/read-chan ch 10)) 10))
      (async/<!! oc)
      (is (= (count (util/read-chan ch 10)) 10)))))

(deftest test-generate-intervals
  (let [start (tc/from-date #inst "2017-06-01")
        end (tc/from-date #inst "2017-06-02")
        interval->7h-step (fn [interval]
                            (int (/ (t/in-hours interval) 7)))
        step-7h-fn (fn [step]
                     (t/hours (* 7 step)))]
    (is (= [[start end]] (util/generate-intervals start end)))
    (is (= [[start (tc/from-date #inst "2017-06-01T07:00:00")]
            [(tc/from-date #inst "2017-06-01T07:00:00")
             (tc/from-date #inst "2017-06-01T14:00:00")]
            [(tc/from-date #inst "2017-06-01T14:00:00")
             (tc/from-date #inst "2017-06-01T21:00:00")]
            [(tc/from-date #inst "2017-06-01T21:00:00")
             end]]
           (util/generate-intervals start end interval->7h-step step-7h-fn)))))

(def test-get-jobs-by-user-and-state
  (let [uri "datomic:mem://test-get-pending-job-ents"
        conn (restore-fresh-database! uri)]
    (doseq [state [:job.state/waiting :job.state/running :job.state/completed]]
      (create-dummy-job conn
                        :user "u1"
                        :job-state state
                        :submit-time #inst "2017-06-02T12:00:00"
                        :custom-executor? false)
      (create-dummy-job conn
                        :user "u1"
                        :job-state state
                        :submit-time #inst "2017-06-02T12:00:00"
                        :custom-executor? false)
      (create-dummy-job conn
                        :user "u2"
                        :job-state state
                        :submit-time #inst "2017-06-03T12:00:00"
                        :custom-executor? false)
      (create-dummy-job conn
                        :user "u1"
                        :job-state state
                        :submit-time #inst "2017-06-03T12:00:00"
                        :custom-executor? false)
      (testing (str "get " state " jobs")
        (is (= 2 (count (util/get-jobs-by-user-and-state (d/db conn) "u1" state
                                                         #inst "2017-06-02" #inst "2017-06-03"))))
        (is (= 3 (count (util/get-jobs-by-user-and-state (d/db conn) "u1" state
                                                         #inst "2017-06-02" #inst "2017-06-04"))))
        (is (= 1 (count (util/get-jobs-by-user-and-state (d/db conn) "u2" state
                                                         #inst "2017-06-02" #inst "2017-06-04"))))
        (is (= 0 (count (util/get-jobs-by-user-and-state (d/db conn) "u3" state
                                                         #inst "2017-06-02" #inst "2017-06-04"))))
        (is (= 0 (count (util/get-jobs-by-user-and-state (d/db conn) "u1" state
                                                         #inst "2017-06-01" #inst "2017-06-02"))))))))

(comment (run-tests))
