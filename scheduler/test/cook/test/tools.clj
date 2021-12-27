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
(ns cook.test.tools
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clojure.test :refer :all]
            [cook.config :as config]
            [cook.test.postgres]
            [cook.queries :as queries]
            [cook.test.testutil :as testutil
             :refer [create-dummy-group create-dummy-instance create-dummy-job create-dummy-job-with-instances create-pool restore-fresh-database!]]
            [cook.tools :as tools]
            [cook.util :as util]
            [datomic.api :as d :refer [db q]])
  (:import (java.util.concurrent ExecutionException)
           (java.util Date)
           (org.joda.time DateTime)))

(use-fixtures :once cook.test.postgres/with-pg-db)

(deftest test-total-resources-of-jobs
  (let [uri "datomic:mem://test-total-resources-of-jobs"
        conn (restore-fresh-database! uri)
        jobs [(create-dummy-job conn :ncpus 4.00 :memory 0.5)
              (create-dummy-job conn :ncpus 0.10 :memory 10.0 :gpus 1.0)
              (create-dummy-job conn :ncpus 1.00 :memory 30.0 :gpus 2.0)
              (create-dummy-job conn :ncpus 10.0 :memory 5.0)]
        job-ents (mapv (partial d/entity (db conn)) jobs)]
    (is (= (tools/total-resources-of-jobs (take 1 job-ents))
           {:jobs 1 :cpus 4.0 :mem 0.5 :gpus 0.0}))
    (is (= (tools/total-resources-of-jobs (take 2 job-ents))
           {:jobs 2 :cpus 4.1 :mem 10.5 :gpus 1.0}))
    (is (= (tools/total-resources-of-jobs job-ents)
           {:jobs 4 :cpus 15.1 :mem 45.5 :gpus 3.0}))
    (is (= (tools/total-resources-of-jobs nil)
           {:jobs 0 :cpus 0.0 :mem 0.0 :gpus 0.0}))))

(deftest test-get-running-job-ents
  (let [uri "datomic:mem://test-get-running-task-ents"
        conn (restore-fresh-database! uri)]
    (create-pool conn "pool-a")
    (create-pool conn "pool-b")
    (create-dummy-job conn
                      :user "u1"
                      :job-state :job.state/completed)
    (create-dummy-job conn
                      :user "u1"
                      :job-state :job.state/running
                      :pool "pool-a")
    (create-dummy-job conn
                      :user "u1"
                      :job-state :job.state/running
                      :pool "pool-a")
    (create-dummy-job conn
                      :user "u1"
                      :job-state :job.state/running
                      :pool "pool-b")
    (create-dummy-job conn
                      :user "u1"
                      :job-state :job.state/running)
    (create-dummy-job conn
                      :user "u1"
                      :job-state :job.state/running)
    (create-dummy-job conn
                      :user "u1"
                      :job-state :job.state/waiting)
    (create-dummy-job conn
                      :user "u1"
                      :job-state :job.state/waiting)
    (create-dummy-job conn
                      :user "u2"
                      :job-state :job.state/running
                      :pool "pool-a")
    (create-dummy-job conn
                      :user "u2"
                      :job-state :job.state/running
                      :pool "pool-b")
    (create-dummy-job conn
                      :user "u2"
                      :job-state :job.state/running)
    (create-dummy-job conn
                      :user "u2"
                      :job-state :job.state/waiting)
    (create-dummy-job conn
                      :user "u3"
                      :job-state :job.state/running
                      :pool "pool-a")

    ; No default pool, specifying a specific pool
    (is (= 2 (count (tools/get-user-running-job-ents-in-pool (db conn) "u1" "pool-a"))))
    (is (= 1 (count (tools/get-user-running-job-ents-in-pool (db conn) "u2" "pool-b"))))
    (is (= 0 (count (tools/get-user-running-job-ents-in-pool (db conn) "u3" "pool-c"))))

    ; No default pool, not specifying a pool
    (is (= 2 (count (tools/get-user-running-job-ents-in-pool (db conn) "u1" nil))))
    (is (= 1 (count (tools/get-user-running-job-ents-in-pool (db conn) "u2" nil))))
    (is (= 0 (count (tools/get-user-running-job-ents-in-pool (db conn) "u3" nil))))

    ; Default pool defined, specifying a specific pool
    (with-redefs [config/default-pool (constantly "pool-a")]
      (is (= 4 (count (tools/get-user-running-job-ents-in-pool (db conn) "u1" "pool-a"))))
      (is (= 1 (count (tools/get-user-running-job-ents-in-pool (db conn) "u2" "pool-b"))))
      (is (= 0 (count (tools/get-user-running-job-ents-in-pool (db conn) "u3" "pool-c")))))

    ; Default pool defined, not specifying a pool
    (with-redefs [config/default-pool (constantly "pool-b")]
      (is (= 3 (count (tools/get-user-running-job-ents-in-pool (db conn) "u1" nil))))
      (is (= 2 (count (tools/get-user-running-job-ents-in-pool (db conn) "u2" nil))))
      (is (= 0 (count (tools/get-user-running-job-ents-in-pool (db conn) "u3" nil)))))))

(deftest test-get-pending-job-ents
  (let [uri "datomic:mem://test-get-pending-job-ents"
        conn (restore-fresh-database! uri)]
    (create-dummy-job conn :user "u1" :job-state :job.state/waiting)
    (create-dummy-job conn :user "u1" :job-state :job.state/running)
    (create-dummy-job conn :user "u2" :job-state :job.state/waiting)
    (create-dummy-job conn :user "u1" :job-state :job.state/waiting)
    (is (= 3 (count (queries/get-pending-job-ents (db conn)))))))

(deftest test-filter-sequential
  ; Same as filter
  (is (tools/filter-sequential (fn [state x]
                                [state (even? x)])
                               {}
                               (range 10))
      (filter even? (range 10)))
  ;; Check lazy
  (is (take 100 (tools/filter-sequential (fn [state x]
                                          [state (even? x)])
                                         {}
                                         (range)))
      (take 100 (filter even? (range)))
      )
  ;; Check with state
  ;; Take first 100 odd numbers. Take even numbers after first 100
  (is (take 200
            (tools/filter-sequential
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
               (t/in-seconds (tools/task-run-time task-ent))))))
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
               (t/in-seconds (tools/task-run-time task-ent))))))))

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
        (is (= (tools/job-ent->attempts-consumed db job-ent) 2))
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
        (is (= (tools/job-ent->attempts-consumed db job-ent) 1))
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
        (is (= (tools/job-ent->attempts-consumed db job-ent) 0))
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
        (is (= (tools/job-ent->attempts-consumed db job-ent) 2))
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
        (is (= (tools/job-ent->attempts-consumed db job-ent) 2))
        ))
    (testing "Finished job"
      (let [job (create-dummy-job conn :user "tsram" :job-state :job.state/completed :retry-count 3)
            _ (create-dummy-instance conn job :instance-status :instance.status/success) ; Counted
            _ (create-dummy-instance conn job :instance-status :instance.status/failed) ; Counted
            _ (create-dummy-instance conn job :instance-status :instance.status/failed
                                     :reason :preempted-by-rebalancer) ; Not counted
            db (d/db conn)
            job-ent (d/entity @(d/sync conn) job)]
        (is (= (tools/job-ent->attempts-consumed db job-ent) 2))
        ))

    ))

(deftest test-entity->map
  (let [uri "datomic:mem://test-entity-map"
        conn (restore-fresh-database! uri)
        job (create-dummy-job conn :user "tsram")
        job-ent (d/entity @(d/sync conn) job)
        job-map (tools/entity->map job-ent)]
    (is (instance? datomic.Entity job-ent))
    (is (instance? datomic.Entity (first (:job/resource job-ent))))
    (is (not (instance? datomic.Entity job-map)))
    (is (not (instance? datomic.Entity (first (:job/resource job-map)))))))

(deftest test-job-ent->map
  (let [uri "datomic:mem://test-job-ent-map"
        conn (restore-fresh-database! uri)
        group-id (create-dummy-group conn)
        job-id (create-dummy-job conn :group group-id)
        job-map (tools/job-ent->map (d/entity @(d/sync conn) job-id))
        group (first (:group/_job job-map))]
    (is (not (instance? datomic.Entity job-map)))
    (is group)
    (is (not (instance? datomic.Entity group)))
    (is (not (nil? (:group/uuid group))))
    (is (not (nil? (:group/host-placement group))))
    (is (not (instance? datomic.Entity (:group/host-placement group))))
    (is (nil? (:group/job group)))))

(deftest test-namespace-datomic
  (testing "Example tests"
    (is (= (tools/namespace-datomic :straggler-handling :type)
           :straggler-handling/type))
    (is (= (tools/namespace-datomic :straggler-handling :type :quantile-deviation)
           :straggler-handling.type/quantile-deviation))))

(deftest test-clear-uncommitted-jobs
  (testing "no uncommitted"
    (let [uri "datomic:mem://test-clear-uncommitted"
          conn (restore-fresh-database! uri)
          _ (dotimes [_ 100]
              (create-dummy-job conn))]
      (is (= (count (tools/clear-uncommitted-jobs conn (t/yesterday) true)) 0))))
  (testing "uncommitted but not before"
    (let [uri "datomic:mem://test-clear-uncommitted"
          conn (restore-fresh-database! uri)
          _ (dotimes [_ 100]
              (create-dummy-job conn))
          _ (dotimes [_ 100]
              (create-dummy-job conn
                                :committed? false
                                :submit-time (tc/to-date (-> 6 t/hours t/ago))))]
      (is (= (count (tools/clear-uncommitted-jobs conn (t/yesterday) true)) 0))))
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
      (is (= (count (tools/clear-uncommitted-jobs conn (t/yesterday) true))
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
      (is (= (count (tools/clear-uncommitted-jobs conn (t/yesterday) false))
             uncommitted-count))
      (is (= (count (tools/clear-uncommitted-jobs conn (t/yesterday) false)) 0)))))

(deftest test-clear-old-uncommitted-jobs
  (testing "Clear old uncommitted jobs works when we're the master."
    (let [uri "datomic:mem://test-clear-old-uncommitted"
          conn (restore-fresh-database! uri)
          _ (dotimes [_ 100]
              (create-dummy-job conn))
          _ (dotimes [_ 100]
              (create-dummy-job conn
                                :committed? false
                                :submit-time (tc/to-date (-> 6 t/hours t/ago))))
          uncommitted-count 26
          _ (dotimes [_ uncommitted-count]
              (create-dummy-job conn
                                :committed? false
                                :submit-time (tc/to-date (-> 9 t/days t/ago))))]
      (is (= (count (tools/clear-old-uncommitted-jobs conn (atom true))) uncommitted-count))
      (is (= (count (tools/clear-old-uncommitted-jobs conn (atom true))) 0))))

  (testing "Clear old uncommitted jobs does nothing when we're not the master."
    (let [uri "datomic:mem://test-clear-old-uncommitted"
          conn (restore-fresh-database! uri)
          _ (dotimes [_ 100]
              (create-dummy-job conn))
          _ (dotimes [_ 100]
              (create-dummy-job conn
                                :committed? false
                                :submit-time (tc/to-date (-> 6 t/hours t/ago))))
          uncommitted-count 27
          _ (dotimes [_ uncommitted-count]
              (create-dummy-job conn
                                :committed? false
                                :submit-time (tc/to-date (-> 9 t/days t/ago))))]
      (is (= (count (tools/clear-old-uncommitted-jobs conn (atom false))) 0))
      (is (= (count (tools/clear-old-uncommitted-jobs conn (atom false))) 0)))))

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
        guuid->juuids (tools/make-guuid->juuids jobs)]
    (is (= (guuid->juuids "a") #{"1"}))
    (is (= (guuid->juuids "b") #{"1" "2"}))
    (is (= (guuid->juuids "c") #{"1" "2" "3"}))
    (is (= (guuid->juuids "d") #{"1" "2" "3" "4"}))
    (is (= (guuid->juuids "e") #{"1" "2" "3" "4" "5"}))))

(deftest test-read-chan
  (is (= (count (tools/read-chan (async/chan) 10)) 0))
  (let [ch (async/chan 10)]
    (async/<!! (async/onto-chan ch [1 2 3 4 5] false))
    (is (= (count (tools/read-chan ch 10)) 5))
    (is (= (count (tools/read-chan ch 10)) 0))
    (async/<!! (async/onto-chan ch (range 10) false))
    (let [oc (async/onto-chan ch (range 10) false)]
      (is (= (count (tools/read-chan ch 10)) 10))
      (async/<!! oc)
      (is (= (count (tools/read-chan ch 10)) 10)))))

(deftest test-get-jobs-by-user-and-states
  (let [uri "datomic:mem://test-get-pending-job-ents"
        conn (restore-fresh-database! uri)]
    (doseq [state [:job.state/waiting :job.state/running :job.state/completed]]
      ;; This function depends on when the transaction occurred so
      ;; submit time must be based on now and we add sleeps to make the
      ;; test less flaky 
      (let [start-time (Date.)
            job1 (create-dummy-job conn
                                   :name "job1"
                                   :user "u1"
                                   :job-state state
                                   :submit-time (Date.)
                                   :custom-executor? false)
            ; this job should never be returned.
            _ (create-dummy-job conn
                                :name "job1.5"
                                :user "u1"
                                :job-state state
                                :submit-time (Date.)
                                :custom-executor? true)
            _ (Thread/sleep 5)
            job2 (create-dummy-job conn
                                   :name "job2"
                                   :user "u1"
                                   :job-state state
                                   :submit-time (Date.)
                                   :custom-executor? false)
            _ (Thread/sleep 5)
            half-way-time (Date.)
            job3 (create-dummy-job conn
                                   :name "job3"
                                   :user "u2"
                                   :job-state state
                                   :submit-time (Date.)
                                   :custom-executor? false)
            _ (Thread/sleep 5)
            job4 (create-dummy-job conn
                                   :name "job4"
                                   :user "u1"
                                   :job-state state
                                   :submit-time (Date.)
                                   :custom-executor? false)
            _ (Thread/sleep 5)
            end-time (Date.)
            states [(name state)]
            match-any-name-fn (constantly true)
            match-no-name-fn (constantly false)]
        (testing (str "get " state " jobs")
          (is (= 2 (count (tools/get-jobs-by-user-and-states (d/db conn) "u1" states start-time half-way-time
                                                             10 match-any-name-fn false nil))))
          (is (= 1 (count (tools/get-jobs-by-user-and-states (d/db conn) "u1" states start-time half-way-time
                                                             1 match-any-name-fn false nil))))
          (is (= (map :db/id (tools/get-jobs-by-user-and-states (d/db conn) "u1" states start-time half-way-time
                                                                1 match-any-name-fn false nil))
                 [job1]))
          (is (= 3 (count (tools/get-jobs-by-user-and-states (d/db conn) "u1" states start-time end-time
                                                             10 match-any-name-fn false nil))))
          (is (= 2 (count (tools/get-jobs-by-user-and-states (d/db conn) "u1" states start-time end-time
                                                             2 match-any-name-fn false nil))))
          (is (= (map :db/id (tools/get-jobs-by-user-and-states (d/db conn) "u1" states start-time end-time
                                                                2 match-any-name-fn false nil))
                 [job1 job2]))

          (is (= 1 (count (tools/get-jobs-by-user-and-states (d/db conn) "u2" states start-time end-time
                                                             10 match-any-name-fn false nil))))
          (is (= 0 (count (tools/get-jobs-by-user-and-states (d/db conn) "u3" states start-time end-time
                                                             10 match-any-name-fn false nil))))
          (is (= 0 (count (tools/get-jobs-by-user-and-states (d/db conn) "u1" states
                                                             #inst "2017-06-01" #inst "2017-06-02"
                                                             10 match-any-name-fn false nil)))))
        ; Nil means the same as always true.
        (testing (str "get " state " jobs ; name = nil")
          (is (= 2 (count (tools/get-jobs-by-user-and-states (d/db conn) "u1" states start-time half-way-time
                                                             10 nil false nil))))
          (is (= 1 (count (tools/get-jobs-by-user-and-states (d/db conn) "u1" states start-time half-way-time
                                                             1 nil false nil))))
          (is (= (map :db/id (tools/get-jobs-by-user-and-states (d/db conn) "u1" states start-time half-way-time
                                                                1 nil false nil))
                 [job1]))
          (is (= 3 (count (tools/get-jobs-by-user-and-states (d/db conn) "u1" states start-time end-time
                                                             10 nil false nil))))
          (is (= 2 (count (tools/get-jobs-by-user-and-states (d/db conn) "u1" states start-time end-time
                                                             2 nil false nil))))
          (is (= (map :db/id (tools/get-jobs-by-user-and-states (d/db conn) "u1" states start-time end-time
                                                                2 nil false nil))
                 [job1 job2]))

          (is (= 1 (count (tools/get-jobs-by-user-and-states (d/db conn) "u2" states start-time end-time
                                                             10 nil false nil))))
          (is (= 0 (count (tools/get-jobs-by-user-and-states (d/db conn) "u3" states start-time end-time
                                                             10 nil false nil))))
          (is (= 0 (count (tools/get-jobs-by-user-and-states (d/db conn) "u1" states
                                                             #inst "2017-06-01" #inst "2017-06-02"
                                                             10 nil false nil)))))
        ; Never true, so should match nothing.
        (testing (str "get " state " jobs ; name = false")
          (is (= 0 (count (tools/get-jobs-by-user-and-states (d/db conn) "u1" states start-time half-way-time
                                                             10 match-no-name-fn false nil))))
          (is (= 0 (count (tools/get-jobs-by-user-and-states (d/db conn) "u1" states start-time half-way-time
                                                             1 match-no-name-fn false nil))))

          (is (= 0 (count (tools/get-jobs-by-user-and-states (d/db conn) "u1" states start-time end-time
                                                             10 match-no-name-fn false nil))))
          (is (= 0 (count (tools/get-jobs-by-user-and-states (d/db conn) "u1" states start-time end-time
                                                             2 match-no-name-fn false nil))))

          (is (= 0 (count (tools/get-jobs-by-user-and-states (d/db conn) "u2" states start-time end-time
                                                             10 match-no-name-fn false nil))))
          (is (= 0 (count (tools/get-jobs-by-user-and-states (d/db conn) "u3" states start-time end-time
                                                             10 match-no-name-fn false nil))))
          (is (= 0 (count (tools/get-jobs-by-user-and-states (d/db conn) "u1" states
                                                             #inst "2017-06-01" #inst "2017-06-02"
                                                             10 match-no-name-fn false nil)))))))))

(deftest test-reducing-pipe
  (testing "basic piping"
    (let [initial-state []
          data-counter (atom 0)
          in-xform (fn [state data]
                     (swap! data-counter inc)
                     (conj state data))
          in-chan (async/chan 10)
          out-chan (async/chan)
          reducing-go-chan (tools/reducing-pipe in-chan in-xform out-chan :initial-state initial-state)]

      (testing "consume initial batch"
        (async/>!! in-chan 1)
        (async/>!! in-chan 2)
        (async/>!! in-chan 3)
        (testutil/poll-until #(= @data-counter 3) 10 1000)
        (is (= [1 2 3] (async/<!! out-chan))))

      (testing "keep consuming empty data when no new input"
        (is (= [] (async/<!! out-chan)))
        (is (= [] (async/<!! out-chan))))

      (testing "consume subsequent batch"
        (async/>!! in-chan 4)
        (async/>!! in-chan 5)
        (testutil/poll-until #(= @data-counter 5) 10 1000)
        (is (= [4 5] (async/<!! out-chan))))

      (testing "keep consuming empty data when no new input"
        (is (= [] (async/<!! out-chan)))
        (is (= [] (async/<!! out-chan))))

      (async/close! in-chan)

      (async/<!! reducing-go-chan) ;; wait for go-block to terminate
      (testing "out-chan should be closed"
        (is (nil? (async/<!! out-chan)))))))

(deftest test-cache-lookup-and-update
  (let [cache-store (-> {}
                        (cache/lru-cache-factory :threshold 2)
                        atom)]
    (is (= 10 (tools/cache-lookup! cache-store "A" 10)))
    (is (= {"A" 10} (.cache @cache-store)))

    (is (= 10 (tools/cache-lookup! cache-store "A" 11)))
    (is (= {"A" 10} (.cache @cache-store)))

    (is (= 20 (tools/cache-lookup! cache-store "B" 20)))
    (is (= {"A" 10, "B" 20} (.cache @cache-store)))

    (is (= 30 (tools/cache-lookup! cache-store "C" 30)))
    (is (= {"B" 20, "C" 30} (.cache @cache-store)))

    (tools/cache-update! cache-store "C" 31)
    (is (= {"B" 20, "C" 31} (.cache @cache-store)))
    (is (= 31 (tools/cache-lookup! cache-store "C" 32)))
    (is (= {"B" 20, "C" 31} (.cache @cache-store)))

    (is (= 12 (tools/cache-lookup! cache-store "A" 12)))
    (is (= {"A" 12, "C" 31} (.cache @cache-store)))))


(deftest test-retry-job
  (let [uri "datomic:mem://test-retry-job"
        conn (restore-fresh-database! uri)]
    (testing "increment retries on running job"
      (let [[job _] (create-dummy-job-with-instances conn
                                                     :job-state :job.state/running
                                                     :retry-count 5
                                                     :instances [{:instance-status :instance.status/running}])
            uuid (:job/uuid (d/entity (d/db conn) job))]
        (tools/retry-job! conn uuid 10)
        (let [job-ent (d/entity (d/db conn) job)]
          (is (= 10 (:job/max-retries job-ent)))
          (is (= :job.state/running (:job/state job-ent))))))

    (testing "same retries on completed jobs with attempts remaining"
      (let [[job _] (create-dummy-job-with-instances conn
                                                     :job-state :job.state/completed
                                                     :retry-count 2
                                                     :instances [{:instance-status :instance.status/failed}])
            job-uuid (:job/uuid (d/entity (d/db conn) job))]
        (tools/retry-job! conn job-uuid 2)
        (let [job-ent (d/entity (d/db conn) job)]
          (is (= 2 (:job/max-retries job-ent)))
          (is (= :job.state/waiting (:job/state job-ent))))))

    (testing "same retries on completed jobs with mea-culpa attempts remaining"
      (let [[job _] (create-dummy-job-with-instances conn
                                                    :job-state :job.state/completed
                                                    :retry-count 1
                                                    :instances [{:instance-status :instance.status/failed
                                                                 :reason :preempted-by-rebalancer}])
            job-uuid (:job/uuid (d/entity (d/db conn) job))]
        (tools/retry-job! conn job-uuid 1)
        (let [job-ent (d/entity (d/db conn) job)]
          (is (= 1 (:job/max-retries job-ent)))
          (is (= :job.state/waiting (:job/state job-ent))))))

    (testing "same retries on job with no attempts remaining"
      (let [[job _] (create-dummy-job-with-instances conn
                                                     :job-state :job.state/completed
                                                     :retry-count 1
                                                     :instances [{:instance-status :instance.status/failed}])
            job-uuid (:job/uuid (d/entity (d/db conn) job))]
        (tools/retry-job! conn job-uuid 1)
        (let [job-ent (d/entity (d/db conn) job)]
          (is (= 1 (:job/max-retries job-ent)))
          (is (= :job.state/completed (:job/state job-ent))))))

    (testing "increment retries on waiting job"
      (let [job (create-dummy-job conn
                                  :job-state :job.state/waiting
                                  :retry-count 5)
            job-uuid (:job/uuid (d/entity (d/db conn) job))]
        (tools/retry-job! conn job-uuid 20)
        (let [job-ent (d/entity (d/db conn) job)]
          (is (= 20 (:job/max-retries job-ent)))
          (is (= :job.state/waiting (:job/state job-ent))))))

    (testing "decrease retries to less than attempts consumed"
      (let [[job _] (create-dummy-job-with-instances conn
                                                     :retry-count 5
                                                     :job-state :job.state/completed
                                                     :instances [{:instance-status :instance.status/failed}
                                                                 {:instance-status :instance.status/failed}
                                                                 {:instance-status :instance.status/failed}])
            job-uuid (:job/uuid (d/entity (d/db conn) job))]
        (is (thrown-with-msg? ExecutionException #"Attempted to change retries from 5 to 2"
                              (tools/retry-job! conn job-uuid 2)))))

    (testing "decrease retries with attempts remaining"
      (let [[job _] (create-dummy-job-with-instances conn
                                                     :job-state :job.state/completed
                                                     :retry-count 5
                                                     :instances [{:instance-status :instance.status/failed
                                                                  :reason :preempted-by-rebalancer}])
            job-uuid (:job/uuid (d/entity (d/db conn) job))]
        (tools/retry-job! conn job-uuid 1)
        (let [job-ent (d/entity (d/db conn) job)]
          (is (= 1 (:job/max-retries job-ent)))
          (is (= :job.state/waiting (:job/state job-ent))))))))

(deftest test-time-seq
  (testing "Generation of a sequence of times"

    (testing "should work for small numbers of iterations"
      (let [start (DateTime. 1000)
            every-milli (util/time-seq start (t/millis 1))
            every-ten-secs (util/time-seq start (t/seconds 10))]
        (is (= (DateTime. 1000) (first every-milli)))
        (is (= (DateTime. 1001) (second every-milli)))
        (is (= (DateTime. 1002) (nth every-milli 2)))
        (is (= (map #(DateTime. %) [1000 1001 1002 1003 1004 1005 1006 1007 1008 1009])
               (take 10 every-milli)))
        (is (= (map #(DateTime. %) [1000 11000 21000 31000 41000 51000 61000 71000 81000 91000])
               (take 10 every-ten-secs)))))

    (testing "should work for 52 weeks worth of ten-second intervals"
      (let [now (t/now)
            every-ten-secs (util/time-seq now (t/millis 10000))]
        (is (true? (t/equal? (t/plus now (t/weeks 52)) (nth every-ten-secs 3144960))))))))

(deftest test-below-quota?
  (testing "not using quota"
    (is (tools/below-quota? {:count 5, :cpus 15, :mem 9999}
                            {:count 0, :cpus 0, :mem 0})))
  (testing "not using quota with extra keys"
    (is (tools/below-quota? {:count 5, :cpus 15, :mem 9999, :foo 2, :bar 3}
                            {:count 0, :cpus 0, :mem 0})))
  (testing "inside quota"
    (is (tools/below-quota? {:count 5, :cpus 15, :mem 9999}
                            {:count 4, :cpus 10, :mem 1234})))
  (testing "at quota limit"
    (is (tools/below-quota? {:count 5, :cpus 15, :mem 9999}
                            {:count 5, :cpus 15, :mem 9999})))
  (testing "exceed quota limit - count"
    (is (not (tools/below-quota? {:count 5, :cpus 15, :mem 9999}
                                 {:count 6, :cpus 10, :mem 1234}))))
  (testing "exceed quota limit - cpus"
    (is (not (tools/below-quota? {:count 5, :cpus 15, :mem 9999}
                                 {:count 4, :cpus 20, :mem 1234}))))
  (testing "exceed quota limit - mem"
    (is (not (tools/below-quota? {:count 5, :cpus 15, :mem 1234}
                                 {:count 4, :cpus 10, :mem 4321}))))
  (testing "exceed quota limit - gpus"
    (is (not (tools/below-quota? {:count 5, :cpus 15, :mem 9999, :gpus 4}
                                 {:count 4, :cpus 10, :mem 1234, :gpus 5}))))
  (testing "at quota limit with extra keys"
    (is (tools/below-quota? {:count 5, :cpus 15, :mem 9999, :gpus 4}
                            {:count 5, :cpus 15, :mem 9999}))))

(deftest test-job->usage
  (testing "cpus and mem usage"
    (is (= {:count 1, :cpus 2, :mem 2048}
           (tools/job->usage {:job/resource [{:resource/type :cpus, :resource/amount 2}
                                            {:resource/type :mem, :resource/amount 2048}]}))))
  (testing "cpus and mem usage - ignore extra resources"
    (is (= {:count 1, :cpus 2, :mem 2048}
           (tools/job->usage {:job/resource [{:resource/type :cpus, :resource/amount 2}
                                            {:resource/type :mem, :resource/amount 2048}
                                            {:resource/type :uri, :resource.uri/value "www.test.com"}]}))))
  (testing "cpus mem and gpus usage"
    (is (= {:count 1, :cpus 2, :gpus 10, :mem 2048}
           (tools/job->usage {:job/resource [{:resource/type :cpus, :resource/amount 2}
                                            {:resource/type :mem, :resource/amount 2048}
                                            {:resource/type :gpus, :resource/amount 10}
                                            {:resource/type :uri, :resource.uri/value "www.test.com"}]}))))
  (testing "cpus mem and gpus usage - ignore extra resources"
    (is (= {:count 1, :cpus 2, :gpus 10, :mem 2048}
           (tools/job->usage {:job/resource [{:resource/type :cpus, :resource/amount 2}
                                            {:resource/type :mem, :resource/amount 2048}
                                            {:resource/type :gpus, :resource/amount 10}]}))))
  (testing "ensures cpu value - gpus absent"
    (is (= {:count 1, :cpus nil, :mem 2048}
           (tools/job->usage {:job/resource [{:resource/type :mem, :resource/amount 2048}]}))))
  (testing "ensures cpu value - gpus present"
    (is (= {:count 1, :cpus nil, :gpus 10, :mem 2048}
           (tools/job->usage {:job/resource [{:resource/type :mem, :resource/amount 2048}
                                            {:resource/type :gpus, :resource/amount 10}]}))))
  (testing "ensures mem value - gpus absent"
    (is (= {:count 1, :cpus 2, :mem nil}
           (tools/job->usage {:job/resource [{:resource/type :cpus, :resource/amount 2}]}))))
  (testing "ensures mem value - gpus present"
    (is (= {:count 1, :cpus 2, :gpus 10, :mem nil}
           (tools/job->usage {:job/resource [{:resource/type :cpus, :resource/amount 2}
                                            {:resource/type :gpus, :resource/amount 10}]})))))

(deftest test-filter-based-on-pool-quota
  (let [usage {:count 1, :cpus 2, :mem 1024}
        make-job (fn [id cpus mem]
                   {:db/id id
                    :job/resource [{:resource/type :cpus, :resource/amount cpus}
                                   {:resource/type :mem, :resource/amount mem}]})
        queue [(make-job 1 2 2048) (make-job 2 1 1024) (make-job 3 3 4096) (make-job 4 1 1024)]]
    (testing "no jobs included"
      (is (= []
             (tools/filter-based-on-pool-quota nil {:count 1, :cpus 2, :mem 1024} usage queue))))
    (testing "all jobs included"
      (is (= [(make-job 1 2 2048) (make-job 2 1 1024) (make-job 3 3 4096) (make-job 4 1 1024)]
             (tools/filter-based-on-pool-quota nil {:count 10, :cpus 20, :mem 32768} usage queue))))
    (testing "room for later jobs not included"
      (is (= [(make-job 1 2 2048) (make-job 2 1 1024)]
             (tools/filter-based-on-pool-quota nil {:count 4, :cpus 20, :mem 6144} usage queue))))))

(deftest test-filter-based-on-user-quota
  (let [test-user "john"
        user->usage {test-user {:count 1, :cpus 2, :mem 1024}}
        make-job (fn [id cpus mem]
                   {:db/id id
                    :job/user test-user
                    :job/resource [{:resource/type :cpus, :resource/amount cpus}
                                   {:resource/type :mem, :resource/amount mem}]})
        queue [(make-job 1 2 2048) (make-job 2 1 1024) (make-job 3 3 4096) (make-job 4 1 1024)]]
    (testing "no jobs included"
      (is (= []
             (tools/filter-based-on-user-quota nil {test-user {:count 1, :cpus 2, :mem 1024}} user->usage queue))))
    (testing "all jobs included"
      (is (= [(make-job 1 2 2048) (make-job 2 1 1024) (make-job 3 3 4096) (make-job 4 1 1024)]
             (tools/filter-based-on-user-quota nil {test-user {:count 10, :cpus 20, :mem 32768}} user->usage queue))))
    (testing "room for later jobs not included"
      (is (= [(make-job 1 2 2048) (make-job 2 1 1024)]
             (tools/filter-based-on-user-quota nil {test-user {:count 4, :cpus 20, :mem 6144}} user->usage queue))))))

(deftest test-filter-pending-jobs-for-quota
  (let [test-user-1 "john"
        test-user-2 "bob"
        user->usage {test-user-1 {:count 1, :cpus 1, :mem 1} test-user-2 {:count 1, :cpus 1, :mem 1}}
        pool-quota {:count 4, :cpus 100, :mem 100}
        make-job (fn [user]
                   {:job/user user
                    :job/resource [{:resource/type :cpus, :resource/amount 1}
                                   {:resource/type :mem, :resource/amount 1}]})
        queue [(make-job test-user-1) (make-job test-user-1) (make-job test-user-1) (make-job test-user-2)]
        user->quota {test-user-1 {:count 2, :cpus 100, :mem 100} test-user-2 {:count 2, :cpus 100, :mem 100}}
        [job-1 job-2 job-3 job-4] queue]
    ; If we filtered pool quota first, only the first two jobs in the
    ; queue would be seen by the user quota and we'd only launch job-1.
    (testing "User quota filters first."
      (is (= [job-1 job-4]
             (tools/filter-pending-jobs-for-quota nil (atom {}) (atom {})
                                                  user->quota user->usage pool-quota queue))))))

(deftest test-pool->user->usage
  (let [uri "datomic:mem://test-pool-user-usage"
        conn (restore-fresh-database! uri)]
    (create-pool conn "A")
    (create-pool conn "B")
    (create-dummy-job-with-instances conn
                                     :job-state :job.state/running
                                     :user "tom"
                                     :memory 10.0
                                     :cpus 1.0
                                     :pool "A"
                                     :instances [{:instance-status :instance.status/running}])
    (create-dummy-job-with-instances conn
                                     :job-state :job.state/running
                                     :user "tom"
                                     :memory 100.0
                                     :cpus 1.0
                                     :pool "A"
                                     :instances [{:instance-status :instance.status/running}])
    (create-dummy-job-with-instances conn
                                     :job-state :job.state/running
                                     :user "tom"
                                     :memory 10.0
                                     :cpus 1.0
                                     :pool "B"
                                     :instances [{:instance-status :instance.status/running}])
    (create-dummy-job-with-instances conn
                                     :job-state :job.state/running
                                     :user "mary"
                                     :memory 10.0
                                     :cpus 1.0
                                     :pool "A"
                                     :instances [{:instance-status :instance.status/running}])

    (create-dummy-job conn
                      :job-state :job.state/waiting
                      :user "tom"
                      :pool "A"
                      :memory 100.0
                      :cpus 1.0)

    (is (= {"A" {"tom" {:cpus 2.0
                        :mem 110.0
                        :count 2}
                 "mary" {:cpus 1.0
                         :mem 10.0
                         :count 1}}
            "B" {"tom" {:cpus 1.0
                        :mem 10.0
                        :count 1}}}
           (tools/pool->user->usage (d/db conn))))))

(deftest test-atom-updater
  (let [map-atom (atom {})
        testfn (tools/make-atom-updater map-atom)]
    (testfn :a nil 1)
    (is (= {:a 1} @map-atom))
    (testfn :b nil 2)
    (is (= {:a 1 :b 2} @map-atom))
    (testfn :b 2 3)
    (is (= {:a 1 :b 3} @map-atom))
    (testfn :a 1 nil)
    (is (= {:b 3} @map-atom))
    (testfn :b 3 nil)
    (is (= {} @map-atom))))

(deftest test-dissoc-in
  (is (= {:a {:c 3} :d {:e 5 :f 6}} (tools/dissoc-in {:a {:b 2 :c 3} :d {:e 5 :f 6}} [:a :b])))
  (is (= {:a {:b 2} :d {:e 5 :f 6}} (tools/dissoc-in {:a {:b 2 :c 3} :d {:e 5 :f 6}} [:a :c])))
  (is (= {:a {:b 2 :c 3} :d {:e 5}} (tools/dissoc-in {:a {:b 2 :c 3} :d {:e 5 :f 6}} [:d :f])))
  ; These keys aren't in it.
  (is (= {:a {:b 2 :c 3} :d {:e 5 :f 6}} (tools/dissoc-in {:a {:b 2 :c 3} :d {:e 5 :f 6}} [:a :d])))
  (is (= {:a {:b 2 :c 3} :d {:e 5 :f 6}} (tools/dissoc-in {:a {:b 2 :c 3} :d {:e 5 :f 6}} [:b :c])))

  ;; Now try deleting from smaller dictionaries and make sure it deletes empty leafs.
  (is (= {:d {:e 5 :f 6}} (tools/dissoc-in {:a {:b 2} :d {:e 5 :f 6}} [:a :b])))
  (is (= {:a {:b 2}} (tools/dissoc-in {:a {:b 2} :d {:e 5}} [:d :e])))
  (is (= {} (tools/dissoc-in {:a {:b 2}} [:a :b])))

  ;; Check for nil safety.
  (is (= {:a {:b 2 :c 3} nil {nil 6}} (tools/dissoc-in {:a {:b 2 :c 3} nil {:e 5 nil 6}} [nil :e])))
  (is (= {:a {:b 2 :c 3} nil {:e 5}} (tools/dissoc-in {:a {:b 2 :c 3} nil {:e 5 nil 6}} [nil nil])))
  (is (= {:a {:b 2} nil {:e 5 nil 6}} (tools/dissoc-in {:a {:b 2 nil 3} nil {:e 5 nil 6}} [:a nil])))
  (is (= {:a {nil 3} nil {:e 5 nil 6}} (tools/dissoc-in {:a {:b 2 nil 3} nil {:e 5 nil 6}} [:a :b])))

  (is (= {:a {:b 2} :d {:e 5 :f 6}} (tools/dissoc-in {:a {:b 2} :d {:e 5 :f 6}} [nil :b])))
  (is (= {:a {:b 2} :d {:e 5 :f 6}} (tools/dissoc-in {:a {:b 2} :d {:e 5 :f 6}} [:nil nil])))
  (is (= {:d {:e 5 :f 6}} (tools/dissoc-in {nil {:b 2} :d {:e 5 :f 6}} [nil :b])))
  (is (= {:a {:b 2}} (tools/dissoc-in {:a {:b 2} :d {nil 5}} [:d nil]))))

(deftest test-make-nested-atom-updater
  (let [map-atom (atom {})
        testfn (tools/make-nested-atom-updater map-atom :k1 :k2)]
    (testing "Inserting"
      (let [d1 {:k1 :a :k2 :b}
            _ (testfn nil nil d1)
            _ (is (= {:a {:b d1}} @map-atom))
            d2 {:k1 :a :k2 :c}
            _ (testfn nil nil d2)
            _ (is (= {:a {:b d1 :c d2}} @map-atom))
            d3 {:k1 :e :k2 :f}
            _ (testfn nil nil d3)
            _ (is (= {:a {:b d1 :c d2} :e {:f d3}} @map-atom))]))
    (testing "Deleting"
      (let [d1 {:k1 :a :k2 :b}
            d2 {:k1 :a :k2 :c}
            d3 {:k1 :e :k2 :f}
            _ (testfn nil d1 nil)
            _ (is (= {:a {:c d2} :e {:f d3}} @map-atom))
            _ (testfn nil d2 nil)
            _ (is (= {:e {:f d3}} @map-atom))
            _ (testfn nil d3 nil)
            _ (is (= {} @map-atom))]))

    (testing "Inserting nil safe"
      (let [d1 {:k1 :a :k2 nil}
            _ (testfn nil nil d1)
            _ (is (= {:a {nil d1}} @map-atom))
            d2 {:k1 :a :k2 :c}
            _ (testfn nil nil d2)
            _ (is (= {:a {nil d1 :c d2}} @map-atom))
            d3 {:k1 nil :k2 :f}
            _ (testfn nil nil d3)
            _ (is (= {:a {nil d1 :c d2} nil {:f d3}} @map-atom))]))

    (testing "Deleting nil safe"
      (let [d2 {:k1 :a :k2 :c}
            d1 {:k1 :a :k2 nil}
            d3 {:k1 nil :k2 :f}
            _ (testfn nil d1 nil)
            _ (is (= {:a {:c d2} nil {:f d3}} @map-atom))
            _ (testfn nil d2 nil)
            _ (is (= {nil {:f d3}} @map-atom))
            _ (testfn nil d3 nil)
            _ (is (= {} @map-atom))]))

    (testing "Updating"
      (let [d1-1 {:k1 :a :k2 :b :foo 1}
            _ (testfn nil nil d1-1)
            _ (is (= {:a {:b d1-1}} @map-atom))
            ; Change the value, but neither key.
            d1-2 {:k1 :a :k2 :b :foo 2}
            _ (testfn nil d1-1 d1-2)
            _ (is (= {:a {:b d1-2}} @map-atom))
            ; Change the key2
            d2 {:k1 :a :k2 :c}
            _ (testfn nil d1-1 d2)
            _ (is (= {:a {:c d2}} @map-atom))
            d3 {:k1 :e :k2 :f}
            _ (testfn nil d2 d3)
            _ (is (= {:e {:f d3}} @map-atom))]))))

(deftest test-offers->resource-maps
  (testing "adds up resources by type"
    (is (= [{"cpus" 1.2
             "mem" 34
             "gpus/nvidia-tesla-k80" 2
             "gpus/nvidia-tesla-p100" 4}
            {"cpus" 2.3
             "mem" 45
             "gpus/nvidia-tesla-k80" 4
             "gpus/nvidia-tesla-p100" 8}]
           (tools/offers->resource-maps
             [{:resources [{:name "cpus" :type :value-scalar :scalar 1.2}
                           {:name "mem" :type :value-scalar :scalar 34}
                           {:name "gpus"
                            :type :value-text->scalar
                            :text->scalar {"nvidia-tesla-k80" 2 "nvidia-tesla-p100" 4}}]}
              {:resources [{:name "cpus" :type :value-scalar :scalar 2.3}
                           {:name "mem" :type :value-scalar :scalar 45}
                           {:name "gpus"
                            :type :value-text->scalar
                            :text->scalar {"nvidia-tesla-k80" 4 "nvidia-tesla-p100" 8}}]}]))))

  (testing "gracefully handles unexpected resource type"
    (is (= [{"cpus" 1.2
             "mem" 34
             "gpus/nvidia-tesla-k80" 2
             "gpus/nvidia-tesla-p100" 4}
            {}
            {"cpus" 2.3
             "mem" 45
             "gpus/nvidia-tesla-k80" 4
             "gpus/nvidia-tesla-p100" 8}]
           (tools/offers->resource-maps
             [{:resources [{:name "cpus" :type :value-scalar :scalar 1.2}
                           {:name "mem" :type :value-scalar :scalar 34}
                           {:name "gpus"
                            :type :value-text->scalar
                            :text->scalar {"nvidia-tesla-k80" 2 "nvidia-tesla-p100" 4}}]}
              {:resources [{:name "bogus-name" :type :bogus-type}]}
              {:resources [{:name "cpus" :type :value-scalar :scalar 2.3}
                           {:name "mem" :type :value-scalar :scalar 45}
                           {:name "gpus"
                            :type :value-text->scalar
                            :text->scalar {"nvidia-tesla-k80" 4 "nvidia-tesla-p100" 8}}]}])))))