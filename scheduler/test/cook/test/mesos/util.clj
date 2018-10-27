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
            [clojure.core.cache :as cache]
            [cook.config :as config]
            [cook.mesos.util :as util]
            [cook.test.testutil :as testutil
             :refer [create-dummy-group
                     create-dummy-instance
                     create-dummy-job
                     create-dummy-job-with-instances
                     create-pool
                     restore-fresh-database!]]
            [datomic.api :as d :refer (q db)])
  (:import [java.util Date]
           [java.util.concurrent ExecutionException]
           [org.joda.time DateTime]))

(deftest test-total-resources-of-jobs
  (let [uri "datomic:mem://test-total-resources-of-jobs"
        conn (restore-fresh-database! uri)
        jobs [(create-dummy-job conn :ncpus 4.00 :memory 0.5)
              (create-dummy-job conn :ncpus 0.10 :memory 10.0 :gpus 1.0)
              (create-dummy-job conn :ncpus 1.00 :memory 30.0 :gpus 2.0)
              (create-dummy-job conn :ncpus 10.0 :memory 5.0)]
        job-ents (mapv (partial d/entity (db conn)) jobs)]
    (is (= (util/total-resources-of-jobs (take 1 job-ents))
           {:jobs 1 :cpus 4.0 :mem 0.5 :gpus 0.0}))
    (is (= (util/total-resources-of-jobs (take 2 job-ents))
           {:jobs 2 :cpus 4.1 :mem 10.5 :gpus 1.0}))
    (is (= (util/total-resources-of-jobs job-ents)
           {:jobs 4 :cpus 15.1 :mem 45.5 :gpus 3.0}))
    (is (= (util/total-resources-of-jobs nil)
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
    (is (= 2 (count (util/get-user-running-job-ents-in-pool (db conn) "u1" "pool-a"))))
    (is (= 1 (count (util/get-user-running-job-ents-in-pool (db conn) "u2" "pool-b"))))
    (is (= 0 (count (util/get-user-running-job-ents-in-pool (db conn) "u3" "pool-c"))))

    ; No default pool, not specifying a pool
    (is (= 2 (count (util/get-user-running-job-ents-in-pool (db conn) "u1" nil))))
    (is (= 1 (count (util/get-user-running-job-ents-in-pool (db conn) "u2" nil))))
    (is (= 0 (count (util/get-user-running-job-ents-in-pool (db conn) "u3" nil))))

    ; Default pool defined, specifying a specific pool
    (with-redefs [config/default-pool (constantly "pool-a")]
      (is (= 4 (count (util/get-user-running-job-ents-in-pool (db conn) "u1" "pool-a"))))
      (is (= 1 (count (util/get-user-running-job-ents-in-pool (db conn) "u2" "pool-b"))))
      (is (= 0 (count (util/get-user-running-job-ents-in-pool (db conn) "u3" "pool-c")))))

    ; Default pool defined, not specifying a pool
    (with-redefs [config/default-pool (constantly "pool-b")]
      (is (= 3 (count (util/get-user-running-job-ents-in-pool (db conn) "u1" nil))))
      (is (= 2 (count (util/get-user-running-job-ents-in-pool (db conn) "u2" nil))))
      (is (= 0 (count (util/get-user-running-job-ents-in-pool (db conn) "u3" nil)))))))

(deftest test-cache
  (let [cache (util/new-cache)
        extract-fn #(if (odd? %) nil %)
        miss-fn #(if (> % 100) nil %)]
    ;; u1 has 2 jobs running
    (is (= 1 (util/lookup-cache! cache extract-fn miss-fn 1))) ; Should not be cached. Nil from extractor.
    (is (= 2 (util/lookup-cache! cache extract-fn miss-fn 2))) ; Should be cached.
    (is (= nil (.getIfPresent cache 1)))
    (is (= 2 (.getIfPresent cache 2)))
    (is (= nil (util/lookup-cache! cache extract-fn miss-fn 101))) ; Should not be cached. Nil from miss function
    (is (= nil (util/lookup-cache! cache extract-fn miss-fn 102))) ; Should not be cached. Nil from miss function
    (is (= nil (.getIfPresent cache 101)))
    (is (= nil (.getIfPresent cache 102)))
    (is (= 4 (util/lookup-cache! cache extract-fn miss-fn 4))) ; Should be cached.
    (is (= 2 (.getIfPresent cache 2)))
    (is (= 4 (.getIfPresent cache 4)))))


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

(deftest test-entity->map
  (let [uri "datomic:mem://test-entity-map"
        conn (restore-fresh-database! uri)
        job (create-dummy-job conn :user "tsram")
        job-ent (d/entity @(d/sync conn) job)
        job-map (util/entity->map job-ent)]
    (is (instance? datomic.Entity job-ent))
    (is (instance? datomic.Entity (first (:job/resource job-ent))))
    (is (not (instance? datomic.Entity job-map)))
    (is (not (instance? datomic.Entity (first (:job/resource job-map)))))))

(deftest test-job-ent->map
  (let [uri "datomic:mem://test-job-ent-map"
        conn (restore-fresh-database! uri)
        group-id (create-dummy-group conn)
        job-id (create-dummy-job conn :group group-id)
        job-map (util/job-ent->map (d/entity @(d/sync conn) job-id))
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
          (is (= 2 (count (util/get-jobs-by-user-and-states (d/db conn) "u1" states start-time half-way-time
                                                            10 match-any-name-fn false nil))))
          (is (= 1 (count (util/get-jobs-by-user-and-states (d/db conn) "u1" states start-time half-way-time
                                                            1 match-any-name-fn false nil))))
          (is (= (map :db/id (util/get-jobs-by-user-and-states (d/db conn) "u1" states start-time half-way-time
                                                               1 match-any-name-fn false nil))
                 [job1]))
          (is (= 3 (count (util/get-jobs-by-user-and-states (d/db conn) "u1" states start-time end-time
                                                            10 match-any-name-fn false nil))))
          (is (= 2 (count (util/get-jobs-by-user-and-states (d/db conn) "u1" states start-time end-time
                                                            2 match-any-name-fn false nil))))
          (is (= (map :db/id (util/get-jobs-by-user-and-states (d/db conn) "u1" states start-time end-time
                                                               2 match-any-name-fn false nil))
                 [job1 job2]))

          (is (= 1 (count (util/get-jobs-by-user-and-states (d/db conn) "u2" states start-time end-time
                                                            10 match-any-name-fn false nil))))
          (is (= 0 (count (util/get-jobs-by-user-and-states (d/db conn) "u3" states start-time end-time
                                                            10 match-any-name-fn false nil))))
          (is (= 0 (count (util/get-jobs-by-user-and-states (d/db conn) "u1" states
                                                            #inst "2017-06-01" #inst "2017-06-02"
                                                            10 match-any-name-fn false nil)))))
        ; Nil means the same as always true.
        (testing (str "get " state " jobs ; name = nil")
          (is (= 2 (count (util/get-jobs-by-user-and-states (d/db conn) "u1" states start-time half-way-time
                                                            10 nil false nil))))
          (is (= 1 (count (util/get-jobs-by-user-and-states (d/db conn) "u1" states start-time half-way-time
                                                            1 nil false nil))))
          (is (= (map :db/id (util/get-jobs-by-user-and-states (d/db conn) "u1" states start-time half-way-time
                                                               1 nil false nil))
                 [job1]))
          (is (= 3 (count (util/get-jobs-by-user-and-states (d/db conn) "u1" states start-time end-time
                                                            10 nil false nil))))
          (is (= 2 (count (util/get-jobs-by-user-and-states (d/db conn) "u1" states start-time end-time
                                                            2 nil false nil))))
          (is (= (map :db/id (util/get-jobs-by-user-and-states (d/db conn) "u1" states start-time end-time
                                                               2 nil false nil))
                 [job1 job2]))

          (is (= 1 (count (util/get-jobs-by-user-and-states (d/db conn) "u2" states start-time end-time
                                                            10 nil false nil))))
          (is (= 0 (count (util/get-jobs-by-user-and-states (d/db conn) "u3" states start-time end-time
                                                            10 nil false nil))))
          (is (= 0 (count (util/get-jobs-by-user-and-states (d/db conn) "u1" states
                                                            #inst "2017-06-01" #inst "2017-06-02"
                                                            10 nil false nil)))))
        ; Never true, so should match nothing.
        (testing (str "get " state " jobs ; name = false")
          (is (= 0 (count (util/get-jobs-by-user-and-states (d/db conn) "u1" states start-time half-way-time
                                                            10 match-no-name-fn false nil))))
          (is (= 0 (count (util/get-jobs-by-user-and-states (d/db conn) "u1" states start-time half-way-time
                                                            1 match-no-name-fn false nil))))

          (is (= 0 (count (util/get-jobs-by-user-and-states (d/db conn) "u1" states start-time end-time
                                                            10 match-no-name-fn false nil))))
          (is (= 0 (count (util/get-jobs-by-user-and-states (d/db conn) "u1" states start-time end-time
                                                            2 match-no-name-fn false nil))))

          (is (= 0 (count (util/get-jobs-by-user-and-states (d/db conn) "u2" states start-time end-time
                                                            10 match-no-name-fn false nil))))
          (is (= 0 (count (util/get-jobs-by-user-and-states (d/db conn) "u3" states start-time end-time
                                                            10 match-no-name-fn false nil))))
          (is (= 0 (count (util/get-jobs-by-user-and-states (d/db conn) "u1" states
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
          reducing-go-chan (util/reducing-pipe in-chan in-xform out-chan :initial-state initial-state)]

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
    (is (= 10 (util/cache-lookup! cache-store "A" 10)))
    (is (= {"A" 10} (.cache @cache-store)))

    (is (= 10 (util/cache-lookup! cache-store "A" 11)))
    (is (= {"A" 10} (.cache @cache-store)))

    (is (= 20 (util/cache-lookup! cache-store "B" 20)))
    (is (= {"A" 10, "B" 20} (.cache @cache-store)))

    (is (= 30 (util/cache-lookup! cache-store "C" 30)))
    (is (= {"B" 20, "C" 30} (.cache @cache-store)))

    (util/cache-update! cache-store "C" 31)
    (is (= {"B" 20, "C" 31} (.cache @cache-store)))
    (is (= 31 (util/cache-lookup! cache-store "C" 32)))
    (is (= {"B" 20, "C" 31} (.cache @cache-store)))

    (is (= 12 (util/cache-lookup! cache-store "A" 12)))
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
        (util/retry-job! conn uuid 10)
        (let [job-ent (d/entity (d/db conn) job)]
          (is (= 10 (:job/max-retries job-ent)))
          (is (= :job.state/running (:job/state job-ent))))))

    (testing "same retries on completed jobs with attempts remaining"
      (let [[job _] (create-dummy-job-with-instances conn
                                                     :job-state :job.state/completed
                                                     :retry-count 2
                                                     :instances [{:instance-status :instance.status/failed}])
            job-uuid (:job/uuid (d/entity (d/db conn) job))]
        (util/retry-job! conn job-uuid 2)
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
        (util/retry-job! conn job-uuid 1)
        (let [job-ent (d/entity (d/db conn) job)]
          (is (= 1 (:job/max-retries job-ent)))
          (is (= :job.state/waiting (:job/state job-ent))))))

    (testing "same retries on job with no attempts remaining"
      (let [[job _] (create-dummy-job-with-instances conn
                                                     :job-state :job.state/completed
                                                     :retry-count 1
                                                     :instances [{:instance-status :instance.status/failed}])
            job-uuid (:job/uuid (d/entity (d/db conn) job))]
        (util/retry-job! conn job-uuid 1)
        (let [job-ent (d/entity (d/db conn) job)]
          (is (= 1 (:job/max-retries job-ent)))
          (is (= :job.state/completed (:job/state job-ent))))))

    (testing "increment retries on waiting job"
      (let [job (create-dummy-job conn
                                  :job-state :job.state/waiting
                                  :retry-count 5)
            job-uuid (:job/uuid (d/entity (d/db conn) job))]
        (util/retry-job! conn job-uuid 20)
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
                              (util/retry-job! conn job-uuid 2)))))

    (testing "decrease retries with attempts remaining"
      (let [[job _] (create-dummy-job-with-instances conn
                                                     :job-state :job.state/completed
                                                     :retry-count 5
                                                     :instances [{:instance-status :instance.status/failed
                                                                  :reason :preempted-by-rebalancer}])
            job-uuid (:job/uuid (d/entity (d/db conn) job))]
        (util/retry-job! conn job-uuid 1)
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

(deftest test-job-group-resources-usage-ranking-score
  (let [uri "datomic:mem://test-group-uuid-non-queued-resources-percentage"
        conn (restore-fresh-database! uri)]
    (testing "group with no jobs"
      (let [group-ent-id (create-dummy-group conn)
            db (d/db conn)]
        (is (= 100 (util/group-id->non-queued-resources-percentage db group-ent-id)))))

    (testing "all jobs queued"
      (let [group-ent-id (create-dummy-group conn)
            j1 (create-dummy-job conn :expected-runtime 10000 :group group-ent-id)
            j2 (create-dummy-job conn :group group-ent-id)
            j3 (create-dummy-job conn :group group-ent-id)
            db (d/db conn)]
        (is (zero? (util/group-id->non-queued-resources-percentage db group-ent-id)))
        (is (= [0 -10000] (util/job->group-processing-score db (d/entity db j1))))
        (is (= [0 nil] (util/job->group-processing-score db (d/entity db j2))))
        (is (= [0 nil] (util/job->group-processing-score db (d/entity db j3))))))

    (testing "all jobs queued with min-group-ranking-score 40"
      (with-redefs [util/min-group-ranking-score 40]
        (let [group-ent-id (create-dummy-group conn)
              j1 (create-dummy-job conn :group group-ent-id)
              j2 (create-dummy-job conn :expected-runtime 10000 :group group-ent-id)
              j3 (create-dummy-job conn :group group-ent-id)
              db (d/db conn)]
          (is (zero? (util/group-id->non-queued-resources-percentage db group-ent-id)))
          (is (= [-40 nil] (util/job->group-processing-score db (d/entity db j1))))
          (is (= [-40 -10000] (util/job->group-processing-score db (d/entity db j2))))
          (is (= [-40 nil] (util/job->group-processing-score db (d/entity db j3)))))))

    (testing "one job running"
      (let [group-ent-id (create-dummy-group conn)
            j1 (create-dummy-job conn :group group-ent-id)
            j2 (create-dummy-job conn :group group-ent-id :job-state :job.state/running)
            j3 (create-dummy-job conn :group group-ent-id)
            db (d/db conn)]
        (is (= 33 (util/group-id->non-queued-resources-percentage db group-ent-id)))
        (is (= [-33 nil] (util/job->group-processing-score db (d/entity db j1))))
        (is (= [-33 nil] (util/job->group-processing-score db (d/entity db j2))))
        (is (= [-33 nil] (util/job->group-processing-score db (d/entity db j3))))))

    (testing "one job queued"
      (let [group-ent-id (create-dummy-group conn)
            j1 (create-dummy-job conn :group group-ent-id)
            j2 (create-dummy-job conn :group group-ent-id :job-state :job.state/completed)
            j3 (create-dummy-job conn :expected-runtime 10000 :group group-ent-id :job-state :job.state/running)
            j4 (create-dummy-job conn :expected-runtime 20000 :group group-ent-id :job-state :job.state/running)
            j5 (create-dummy-job conn :group group-ent-id :job-state :job.state/completed)
            db (d/db conn)]
        (is (= 80 (util/group-id->non-queued-resources-percentage db group-ent-id)))
        (is (= [-80 nil] (util/job->group-processing-score db (d/entity db j1))))
        (is (= [-80 nil] (util/job->group-processing-score db (d/entity db j2))))
        (is (= [-80 -10000] (util/job->group-processing-score db (d/entity db j3))))
        (is (= [-80 -20000] (util/job->group-processing-score db (d/entity db j4))))
        (is (= [-80 nil] (util/job->group-processing-score db (d/entity db j5))))))

    (testing "no job queued"
      (let [group-ent-id (create-dummy-group conn)
            j1 (create-dummy-job conn :group group-ent-id :job-state :job.state/completed)
            j2 (create-dummy-job conn :group group-ent-id :job-state :job.state/completed)
            j3 (create-dummy-job conn :group group-ent-id :job-state :job.state/running)
            j4 (create-dummy-job conn :group group-ent-id :job-state :job.state/running)
            j5 (create-dummy-job conn :group group-ent-id :job-state :job.state/completed)
            db (d/db conn)]
        (is (= 100 (util/group-id->non-queued-resources-percentage db group-ent-id)))
        (is (= [-100 nil] (util/job->group-processing-score db (d/entity db j1))))
        (is (= [-100 nil] (util/job->group-processing-score db (d/entity db j2))))
        (is (= [-100 nil] (util/job->group-processing-score db (d/entity db j3))))
        (is (= [-100 nil] (util/job->group-processing-score db (d/entity db j4))))
        (is (= [-100 nil] (util/job->group-processing-score db (d/entity db j5))))))

    (testing "one job running with memory dominating"
      (let [group-ent-id (create-dummy-group conn)
            j1 (create-dummy-job conn :group group-ent-id :memory 100 :ncpus 20)
            j2(create-dummy-job conn :group group-ent-id :job-state :job.state/running :memory 300 :ncpus 70)
            j3 (create-dummy-job conn :group group-ent-id :memory 200 :ncpus 10)
            db (d/db conn)]
        (is (= 50 (util/group-id->non-queued-resources-percentage db group-ent-id)))
        (is (= [-50 nil] (util/job->group-processing-score db (d/entity db j1))))
        (is (= [-50 nil] (util/job->group-processing-score db (d/entity db j2))))
        (is (= [-50 nil] (util/job->group-processing-score db (d/entity db j3))))))

    (testing "one job running with cpu dominating"
      (let [group-ent-id (create-dummy-group conn)
            j1 (create-dummy-job conn :group group-ent-id :memory 100 :ncpus 20)
            j2 (create-dummy-job conn :group group-ent-id :job-state :job.state/running :memory 300 :ncpus 10)
            j3 (create-dummy-job conn :group group-ent-id :memory 200 :ncpus 10)
            db (d/db conn)]
        (is (= 25 (util/group-id->non-queued-resources-percentage db group-ent-id)))
        (is (= [-25 nil] (util/job->group-processing-score db (d/entity db j1))))
        (is (= [-25 nil] (util/job->group-processing-score db (d/entity db j2))))
        (is (= [-25 nil] (util/job->group-processing-score db (d/entity db j3))))))

    (testing "one job running and one completed"
      (let [group-ent-id (create-dummy-group conn)
            j1 (create-dummy-job conn :group group-ent-id)
            j2 (create-dummy-job conn :group group-ent-id :job-state :job.state/running)
            j3 (create-dummy-job conn :group group-ent-id :job-state :job.state/completed)
            db (d/db conn)]
        (is (= 66 (util/group-id->non-queued-resources-percentage db group-ent-id)))
        (is (= [-66 nil] (util/job->group-processing-score db (d/entity db j1))))
        (is (= [-66 nil] (util/job->group-processing-score db (d/entity db j2))))
        (is (= [-66 nil] (util/job->group-processing-score db (d/entity db j3))))))))

(comment (run-tests))
