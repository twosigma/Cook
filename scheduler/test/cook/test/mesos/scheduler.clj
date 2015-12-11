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

(ns cook.test.mesos.scheduler
  (:use clojure.test)
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [cook.mesos :as mesos]
            [cook.mesos.scheduler :as sched]
            [cook.mesos.share :as share]
            [cook.mesos.util :as util]
            [cook.mesos.schema :as schem]
            [cook.test.mesos.schema :refer (restore-fresh-database! create-dummy-job create-dummy-instance)]
            [datomic.api :as d :refer (q db)]))

(def datomic-uri "datomic:mem://test-mesos-jobs")

(deftest test-tuplify-offer
  (is (= ["1234" 40.0 100.0] (sched/tuplify-offer {:slave-id "1234"
                                                   :resources {:cpus 40.0
                                                               :mem 100.0}}))))
(deftest test-sort-jobs-by-dru
  (let [uri "datomic:mem://test-sort-jobs-by-dru"
        conn (restore-fresh-database! uri)
        j1 (create-dummy-job conn :user "ljin" :ncpus 1.0 :memory 3.0)
        j2 (create-dummy-job conn :user "ljin" :ncpus 1.0 :memory 5.0)
        j3 (create-dummy-job conn :user "ljin" :ncpus 1.0 :memory 2.0)
        j4 (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0)
        j5 (create-dummy-job conn :user "wzhao" :ncpus 6.0 :memory 6.0)
        j6 (create-dummy-job conn :user "wzhao" :ncpus 5.0 :memory 5.0)
        j7 (create-dummy-job conn :user "sunil" :ncpus 5.0 :memory 10.0)
        j8 (create-dummy-job conn :user "sunil" :ncpus 5.0 :memory 10.0)
        _ (create-dummy-instance conn j1)
        _ (create-dummy-instance conn j5)
        _ (create-dummy-instance conn j7)]
    (testing "test1"
      (let [_ (share/set-share! conn "default" :mem 10.0 :cpus 10.0)
            db (d/db conn)]
        (is (= [j2 j3 j6 j4 j8] (map :db/id (sched/sort-jobs-by-dru db))))))
    (testing "test2"
        (let [_ (share/set-share! conn "default" :mem 10.0 :cpus 10.0)
              _ (share/set-share! conn "sunil" :mem 100.0 :cpus 100.0)
              db (d/db conn)]
          (is (= [j8 j2 j3 j6 j4] (map :db/id (sched/sort-jobs-by-dru db)))))))

  (let [uri "datomic:mem://test-sort-jobs-by-dru"
        conn (restore-fresh-database! uri)
        job-id-1 (create-dummy-job conn :user "ljin"
                                   :job-state :job.state/waiting
                                   :memory 1000
                                   :ncpus 1.0)
        job-id-2 (create-dummy-job conn :user "ljin"
                                   :job-state :job.state/waiting
                                   :memory 1000
                                   :ncpus 1.0
                                   :priority 90)
        job-id-3 (create-dummy-job conn :user "wzhao"
                                   :job-state :job.state/waiting
                                   :memory 1500
                                   :ncpus 1.0)
        job-id-4 (create-dummy-job conn :user "wzhao"
                                   :job-state :job.state/waiting
                                   :memory 1500
                                   :ncpus 1.0
                                   :priority 30)

        test-db (d/db conn)]
    (testing
      (is (= [job-id-2 job-id-3 job-id-1 job-id-4] (map :db/id (sched/sort-jobs-by-dru test-db)))))))

(d/delete-database "datomic:mem://preemption-testdb")
(d/create-database "datomic:mem://preemption-testdb")
;;NB you shouldn't transact to this DB--it's read only once it's been initialized
(def c (d/connect "datomic:mem://preemption-testdb"))

(doseq [[t i] (mapv vector cook.mesos.schema/work-item-schema (range))]
  (deref (d/transact c (conj t
                             [:db/add (d/tempid :db.part/tx) :db/txInstant (java.util.Date. i)]))))

(let [j1 (d/tempid :db.part/user)
      j2 (d/tempid :db.part/user)
      j3 (d/tempid :db.part/user)
      j4 (d/tempid :db.part/user)
      t1-1 (d/tempid :db.part/user)
      t1-2 (d/tempid :db.part/user)
      t2-1 (d/tempid :db.part/user)
      {:keys [tempids db-after]}
      (deref (d/transact c [{:db/id j1
                             :job/command "job 1 command"
                             :job/user "dgrnbrg"
                             :job/uuid #uuid "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
                             :job/max-retries 3
                             :job/state :job.state/waiting
                             :job/submit-time #inst "2014-09-23T00:00"
                             :job/resource [{:db/id (d/tempid :db.part/user)
                                             :resource/type :resource.type/mem
                                             :resource/amount 1000.0}
                                            {:db/id (d/tempid :db.part/user)
                                             :resource/type :resource.type/cpus
                                             :resource/amount 1.0}]
                             :job/instance [{:db/id t1-1
                                             :instance/status :instance.status/failed
                                             :instance/task-id "job 1: task 1"
                                             :instance/start-time #inst "2014-09-23T00:01"
                                             :instance/end-time #inst "2014-09-23T00:10"}
                                            {:db/id t1-2
                                             :instance/status :instance.status/failed
                                             :instance/task-id "job 1: task 2"
                                             :instance/start-time #inst "2014-09-23T00:11"
                                             :instance/end-time #inst "2014-09-23T00:20"}]}
                            {:db/id j2
                             :job/command "job 2 command"
                             :job/user "dgrnbrg"
                             :job/uuid #uuid "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
                             :job/max-retries 3
                             :job/state :job.state/running
                             :job/submit-time #inst "2014-09-23T00:05"
                             :job/resource [{:db/id (d/tempid :db.part/user)
                                             :resource/type :resource.type/mem
                                             :resource/amount 1000.0}
                                            {:db/id (d/tempid :db.part/user)
                                             :resource/type :resource.type/cpus
                                             :resource/amount 1.0}]
                             :job/instance [{:db/id t2-1
                                             :instance/status :instance.status/running
                                             :instance/start-time #inst "2014-09-23T00:06"
                                             :instance/task-id "job 2: task 1"}]}
                            {:db/id j3
                            :job/command "job 3 command"
                             :job/user "wzhao"
                             :job/uuid #uuid "cccccccc-cccc-cccc-cccc-cccccccccccc"
                             :job/max-retries 3
                             :job/submit-time #inst "2014-09-23T00:04"
                             :job/state :job.state/waiting
                             :job/resource [{:db/id (d/tempid :db.part/user)
                                             :resource/type :resource.type/mem
                                             :resource/amount 1000.0}
                                            {:db/id (d/tempid :db.part/user)
                                             :resource/type :resource.type/cpus
                                             :resource/amount 1.0}]}
                            {:db/id j4
                             :job/command "job 4 command"
                             :job/user "palaitis"
                             :job/uuid #uuid "dddddddd-dddd-dddd-dddd-dddddddddddd"
                             :job/max-retries 3
                             :job/submit-time #inst "2014-09-23T00:02"
                             :job/state :job.state/waiting
                             :job/resource [{:db/id (d/tempid :db.part/user)
                                             :resource/type :resource.type/mem
                                             :resource/amount 1000.0}
                                            {:db/id (d/tempid :db.part/user)
                                             :resource/type :resource.type/cpus
                                             :resource/amount 1.0}]}]))]
  (def j1 (d/resolve-tempid db-after tempids j1))
  (def j2 (d/resolve-tempid db-after tempids j2))
  (def j3 (d/resolve-tempid db-after tempids j3))
  (def j4 (d/resolve-tempid db-after tempids j4))
  (def t1-1 (d/resolve-tempid db-after tempids t1-1))
  (def t1-2 (d/resolve-tempid db-after tempids t1-2))
  (def t2-1 (d/resolve-tempid db-after tempids t2-1)))

(deftest test-extract-job-resources
  (let [resources (util/job-ent->resources (d/entity (db c) j1))]
    (is (= 1.0 (:cpus resources)))
    (is (= 1000.0 (:mem resources)))))

(deftest test-match-offer-to-schedule
  (let [schedule (map #(d/entity (db c) %) [j1 j2 j3 j4]) ; all 1gb 1 cpu
        offer-maker (fn [cpus mem]
                      [{:resources {:cpus (double cpus) :mem (double mem)}
                        :id (str "id-" (java.util.UUID/randomUUID))
                        :slave-id (str "slave-" (java.util.UUID/randomUUID))
                        :hostname (str "host-" (java.util.UUID/randomUUID))}])
        fid (str "framework-id-" (java.util.UUID/randomUUID))
        fenzo-maker #(sched/make-fenzo-scheduler nil 100000)] ; The params are for offer declining, which should never happen
    (testing "Consume no schedule cases"
      (are [schedule offers] (= [] (sched/match-offer-to-schedule (fenzo-maker) schedule offers (db c) fid))
           [] (offer-maker 0 0)
           [] (offer-maker 2 2000)
           schedule (offer-maker 0 0)
           schedule (offer-maker 0.5 100)
           schedule (offer-maker 0.5 1000)
           schedule (offer-maker 1 500)))
    (testing "Consume Partial schedule cases"
      ;; We're looking for one task to get assigned
      (are [offers] (= 1 (count (mapcat :tasks (sched/match-offer-to-schedule
                                                 (fenzo-maker) schedule offers (db c) fid))))
           (offer-maker 1 1000)
           (offer-maker 1.5 1500)))
    (testing "Consume full schedule cases"
      ;; We're looking for the entire schedule to get assigned
      (are [offers] (= (count schedule)
                       (count (mapcat :tasks (sched/match-offer-to-schedule
                                               (fenzo-maker) schedule offers (db c) fid))))
           (offer-maker 4 4000)
           (offer-maker 5 5000)))))

(deftest test-ids-of-backfilled-instances
  (let [uri "datomic:mem://test-ids-of-backfilled-instances"
        conn (restore-fresh-database! uri)
        jid (d/tempid :db.part/user)
        tid1 (d/tempid :db.part/user)
        tid2 (d/tempid :db.part/user)
        {:keys [tempids db-after]} @(d/transact conn [[:db/add jid :job/instance tid1]
                                                      [:db/add jid :job/instance tid2]
                                                      [:db/add tid1 :instance/status :instance.status/running]
                                                      [:db/add tid2 :instance/status :instance.status/running]
                                                      [:db/add tid1 :instance/backfilled? false]
                                                      [:db/add tid2 :instance/backfilled? true]])
        jid' (d/resolve-tempid db-after tempids jid)
        tid2' (d/resolve-tempid db-after tempids tid2)
        backfilled-ids (sched/ids-of-backfilled-instances (d/entity db-after jid'))]
    (is (= [tid2'] backfilled-ids))))

(deftest test-process-matches-for-backfill
  (letfn [(mock-job [& instances] ;;instances is a seq of booleans which denote the running instances that could be true or false
            {:job/uuid (java.util.UUID/randomUUID)
             :job/instance (for [backfill? instances]
                             {:instance/status :instance.status/running
                              :instance/backfilled? backfill?
                              :db/id (java.util.UUID/randomUUID)})})]
    (let [j1 (mock-job)
          j2 (mock-job true)
          j3 (mock-job false true true)
          j4 (mock-job)
          j5 (mock-job true)
          j6 (mock-job false false)
          j7 (mock-job false)]
      (testing "Match nothing"
        (let [result (sched/process-matches-for-backfill [j1 j4] j1 [])]
          (is (zero? (count (:fully-processed result))))
          (is (zero? (count (:upgrade-backfill result))))
          (is (zero? (count (:backfill-jobs result))))
          (is (not (:matched-head? result)))))
      (testing "Match everything basic"
        (let [result (sched/process-matches-for-backfill [j1 j4] j1 [j1 j4])]
          (is (= 2 (count (:fully-processed result))))
          (is (zero? (count (:upgrade-backfill result))))
          (is (zero? (count (:backfill-jobs result))))
          (is (:matched-head? result))))
      (testing "Don't match head"
        (let [result (sched/process-matches-for-backfill [j1 j4] j1 [j4])]
          (is (zero? (count (:fully-processed result))))
          (is (zero? (count (:upgrade-backfill result))))
          (is (= 1 (count (:backfill-jobs result))))
          (is (not (:matched-head? result)))))
      (testing "Match the tail, but the head isn't considerable" ;;TODO is this even correct?
        (let [result (sched/process-matches-for-backfill [j1 j4] j4 [j4])]
          (is (zero? (count (:fully-processed result))))
          (is (zero? (count (:upgrade-backfill result))))
          (is (= 1 (count (:backfill-jobs result))))
          (is (not (:matched-head? result)))))
      (testing "Match the tail, and the head was backfilled"
        (let [result (sched/process-matches-for-backfill [j2 j1 j4] j1 [j1 j4])]
          (is (= 2 (count (:fully-processed result))))
          (is (= 1 (count (:upgrade-backfill result))))
          (is (zero? (count (:backfill-jobs result))))
          (is (:matched-head? result))))
      (testing "Match the tail, and the head was backfilled (multiple backfilled mixed in)"
        (let [result (sched/process-matches-for-backfill [j2 j1 j3 j4] j1 [j1 j4])]
          (is (= 2 (count (:fully-processed result))))
          (is (= 3 (count (:upgrade-backfill result))))
          (is (zero? (count (:backfill-jobs result))))
          (is (:matched-head? result))))
      (testing "Fail to match the head, but backfill several jobs"
        (let [result (sched/process-matches-for-backfill [j1 j2 j3 j4 j5 j6 j7] j1 [j4 j6 j7])]
          (is (zero? (count (:fully-processed result))))
          (is (zero? (count (:upgrade-backfill result))))
          (is (= 3 (count (:backfill-jobs result))))
          (is (not (:matched-head? result)))))
      (testing "Fail to match the head, but backfill several jobs"
        (let [result (sched/process-matches-for-backfill [j1 j2 j3 j4 j5 j6 j7] j1 [j4 j6 j7])]
          (is (zero? (count (:fully-processed result))))
          (is (zero? (count (:upgrade-backfill result))))
          (is (= 3 (count (:backfill-jobs result))))
          (is (not (:matched-head? result))))))))

(deftest test-get-user->used-resources
  (let [uri "datomic:mem://test-get-used-resources"
        conn (restore-fresh-database! uri)
        j1 (create-dummy-job conn :user "u1" :job-state :job.state/running)
        j2 (create-dummy-job conn :user "u1" :job-state :job.state/running)
        j3 (create-dummy-job conn :user "u2" :job-state :job.state/running)
        db (db conn)]
    ;; Query u1
    (is (= {"u1" {:mem 20.0 :cpus 2.0}} (sched/get-user->used-resources db "u1")))
    ;; Query u2
    (is (= {"u2" {:mem 10.0 :cpus 1.0}} (sched/get-user->used-resources db "u2")))
    ;; Query unknow user
    (is (= {"whoami" {:mem  0.0 :cpus 0.0}} (sched/get-user->used-resources db "whoami")))
    ; Query all users
    (is (= {"u1" {:mem 20.0 :cpus 2.0}
            "u2" {:mem 10.0 :cpus 1.0}}
           (sched/get-user->used-resources db)))))

(defn joda-datetime->java-date
    [datetime]
    (java.util.Date. (tc/to-long datetime)))

(deftest test-get-lingering-tasks
  (let [uri "datomic:mem://test-get-lingering-tasks"
        conn (restore-fresh-database! uri)
        ;; a job has been timeout
        job-id-1 (create-dummy-job conn :user "tsram" :job-state :job.state/running)
        ;; a job has been timeout
        job-id-2 (create-dummy-job conn :user "tsram" :job-state :job.state/running)
        ;; a job is not timeout
        job-id-3 (create-dummy-job conn :user "tsram" :job-state :job.state/running)
        task-id-1 "task-1"
        task-id-2 "task-2"
        task-id-3 "task-3"
        timeout-hours 4
        start-time-1 (joda-datetime->java-date
                       (t/minus (t/now) (t/hours (+ timeout-hours 1))))
        start-time-2 (joda-datetime->java-date
                       (t/minus (t/now) (t/hours (+ timeout-hours 1))))
        start-time-3 (joda-datetime->java-date
                       (t/minus (t/now) (t/hours (- timeout-hours 1))))
        instance-id-1 (create-dummy-instance conn job-id-1
                                             :instance-status :instance.status/unknown
                                             :task-id task-id-1
                                             :start-time start-time-1)
        instance-id-2 (create-dummy-instance conn job-id-2
                                             :instance-status :instance.status/running
                                             :task-id task-id-2
                                             :start-time start-time-2)
        instance-id-3 (create-dummy-instance conn job-id-2
                                             :instance-status :instance.status/running
                                             :task-id task-id-3
                                             :start-time start-time-3)
        test-db (d/db conn)]
    (is (= #{task-id-1 task-id-2} (set (sched/get-lingering-tasks test-db (t/now) timeout-hours))))))

(deftest test-filter-offensive-jobs
  (let [uri "datomic:mem://test-filter-offensive-jobs"
        conn (restore-fresh-database! uri)
        constraints {:memory-gb 10.0
                     :cpus 5.0}
        ;; a job which breaks the memory constraint
        job-id-1 (create-dummy-job conn :user "tsram"
                                   :job-state :job.state/waiting
                                   :memory (* 1024 (+ (:memory-gb constraints) 2.0))
                                   :ncpus (- (:cpus constraints) 1.0))
        ;; a job which breaks the cpu constraint
        job-id-2 (create-dummy-job conn :user "tsram"
                                   :job-state :job.state/waiting
                                   :memory (* 1024 (- (:memory-gb constraints) 2.0))
                                   :ncpus (+ (:cpus constraints) 1.0))
        ;; a job which follows all constraints
        job-id-3 (create-dummy-job conn :user "tsram"
                                   :job-state :job.state/waiting
                                   :memory (* 1024 (- (:memory-gb constraints) 2.0))
                                   :ncpus (- (:cpus constraints) 1.0))
        test-db (d/db conn)
        job-entity-1 (d/entity test-db job-id-1)
        job-entity-2 (d/entity test-db job-id-2)
        job-entity-3 (d/entity test-db job-id-3)
        jobs [job-entity-1 job-entity-2 job-entity-3]
        offensive-jobs-ch (async/chan (count jobs))
        offensive-jobs #{job-entity-1 job-entity-2}]
    (is (= #{job-entity-3} (set (sched/filter-offensive-jobs constraints offensive-jobs-ch jobs))))
    (println "check1")
    (let [received-offensive-jobs (async/<!! offensive-jobs-ch)]
      (is (= (set received-offensive-jobs) offensive-jobs))
      (println "check2")
      (async/close! offensive-jobs-ch))))

(deftest test-rank-jobs
  (let [uri "datomic:mem://test-rank-jobs"
        conn (restore-fresh-database! uri)
        constraints {:memory-gb 10.0
                     :cpus 5.0}
        ;; a job which breaks the memory constraint
        job-id-1 (create-dummy-job conn :user "tsram"
                                   :job-state :job.state/waiting
                                   :memory (* 1024 (+ (:memory-gb constraints) 2.0))
                                   :ncpus (- (:cpus constraints) 1.0))
        ;; a job which follows all constraints
        job-id-2 (create-dummy-job conn :user "tsram"
                                   :job-state :job.state/waiting
                                   :memory (* 1024 (- (:memory-gb constraints) 2.0))
                                   :ncpus (- (:cpus constraints) 1.0))
        test-db (d/db conn)
        job-entity-1 (d/entity test-db job-id-1)
        job-entity-2 (d/entity test-db job-id-2)
        jobs [job-entity-1 job-entity-2]
        offensive-jobs-ch (async/chan (count jobs))
        offensive-jobs #{job-entity-1 job-entity-2}
        offensive-jobs-ch (sched/make-offensive-job-stifler conn)
        offensive-job-filter (partial sched/filter-offensive-jobs constraints offensive-jobs-ch)]
    (is (= #{job-entity-2} (set (sched/rank-jobs test-db offensive-job-filter))))))

(deftest test-get-lingering-tasks
  (let [uri "datomic:mem://test-lingering-tasks"
        conn (restore-fresh-database! uri)
        now (tc/from-date #inst "2015-01-05")
        job-id-1 (create-dummy-job conn
                                   :user "tsram"
                                   :job-state :job.state/running
                                   :max-runtime Long/MAX_VALUE)
        instance-id-1 (create-dummy-instance conn job-id-1
                                             :start-time #inst "2015-01-01")
        job-id-2 (create-dummy-job conn
                                   :user "tsram"
                                   :job-state :job.state/running
                                   :max-runtime 60000)
        instance-id-2 (create-dummy-instance conn job-id-2
                                             :start-time #inst "2015-01-04")
        test-db (d/db conn)
        task-id-2 (-> (d/entity test-db instance-id-2) :instance/task-id)
        ]
    (is (= [task-id-2] (sched/get-lingering-tasks test-db now 120))))
  )

(deftest test-upgrade-backfill-flow
  ;; We need to create a DB and connect the tx monitor queue to it
  ;; Then we'll put in some jobs
  ;; Then pretend to launch them as backfilled
  ;; Check here that they have the right properties (pending, not ready to run)
  ;; Then we'll upgrade the jobs
  ;; Check here that they have the right properties (running, not ready to run)
  ;; Then shut it down

  (let [uri "datomic:mem://test-backfill-upgrade"
        conn (restore-fresh-database! uri)
        check-count-of-pending-and-runnable-jobs
        (fn check-count-of-pending-and-runnable-jobs [expected-pending expected-runnable msg]
          (let [db (db conn)
                pending-jobs (sched/rank-jobs db identity)
                runnable-jobs (filter (fn [job] (sched/job-allowed-to-start? db job)) pending-jobs)]
            (is (= expected-pending (count pending-jobs)) (str "Didn't match pending job count in " msg))
            (is (= expected-runnable (count runnable-jobs)) (str "Didn't match runnable job count in " msg))))
        job (create-dummy-job conn
                              :user "tsram"
                              :job-state :job.state/waiting
                              :max-runtime Long/MAX_VALUE)
        _ (check-count-of-pending-and-runnable-jobs 1 1 "job created")
        instance (create-dummy-instance conn job
                                        :job-state :job.state/waiting
                                        :instance-status :instance.status/running
                                        :backfilled? true)]
    (check-count-of-pending-and-runnable-jobs 1 0 "job backfilled without update")
    @(d/transact conn [[:job/update-state job]])
    (check-count-of-pending-and-runnable-jobs 1 0 "job backfilled")
    @(d/transact conn [[:db/add instance :instance/backfilled? false]])
    @(d/transact conn [[:job/update-state job]])
    (check-count-of-pending-and-runnable-jobs 0 0 "job promoted")))

(comment
  (run-tests))
