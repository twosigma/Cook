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
            [clojure.edn :as edn]
            [cook.mesos :as mesos]
            [cook.mesos.scheduler :as sched]
            [cook.mesos.schema :as schem]
            [cook.mesos.share :as share]
            [cook.mesos.util :as util]
            [cook.test.testutil :refer (restore-fresh-database! create-dummy-group create-dummy-job create-dummy-instance)]
            [datomic.api :as d :refer (q db)]
            [mesomatic.scheduler :as msched]
            [mesomatic.types :as mtypes]
            [plumbing.core :as pc])
  (:import (com.netflix.fenzo TaskAssignmentResult TaskScheduler)
           (java.util UUID)
           (org.mockito Mockito)))

(def datomic-uri "datomic:mem://test-mesos-jobs")

(deftest test-tuplify-offer
  (is (= ["1234" 40.0 100.0] (sched/tuplify-offer {:slave-id {:value "1234"}
                                                   :resources [{:name "cpus"
                                                                :scalar 40.0}
                                                               {:name "mem"
                                                                :scalar 100.0}
                                                               ]}))))
(deftest test-sort-jobs-by-dru
  (let [uri "datomic:mem://test-sort-jobs-by-dru"
        conn (restore-fresh-database! uri)
        j1 (create-dummy-job conn :user "ljin" :ncpus 1.0 :memory 3.0 :job-state :job.state/running)
        j2 (create-dummy-job conn :user "ljin" :ncpus 1.0 :memory 5.0)
        j3 (create-dummy-job conn :user "ljin" :ncpus 1.0 :memory 2.0)
        j4 (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0)
        j5 (create-dummy-job conn :user "wzhao" :ncpus 6.0 :memory 6.0 :job-state :job.state/running)
        j6 (create-dummy-job conn :user "wzhao" :ncpus 5.0 :memory 5.0)
        j7 (create-dummy-job conn :user "sunil" :ncpus 5.0 :memory 10.0 :job-state :job.state/running)
        j8 (create-dummy-job conn :user "sunil" :ncpus 5.0 :memory 10.0)
        _ (create-dummy-instance conn j1)
        _ (create-dummy-instance conn j5)
        _ (create-dummy-instance conn j7)]
    (testing "test1"
      (let [_ (share/set-share! conn "default" :mem 10.0 :cpus 10.0)
            db (d/db conn)]
        (is (= [j2 j3 j6 j4 j8] (map :db/id (:normal (sched/sort-jobs-by-dru db db)))))))
    (testing "test2"
      (let [_ (share/set-share! conn "default" :mem 10.0 :cpus 10.0)
            _ (share/set-share! conn "sunil" :mem 100.0 :cpus 100.0)
            db (d/db conn)]
        (is (= [j8 j2 j3 j6 j4] (map :db/id (:normal (sched/sort-jobs-by-dru db db))))))))

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
      (is (= [job-id-2 job-id-3 job-id-1 job-id-4] (map :db/id (:normal (sched/sort-jobs-by-dru test-db test-db))))))))

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
                      [{:resources [{:name "cpus" :scalar cpus}
                                    {:name "mem" :scalar mem}]
                        :id {:value (str "id-" (UUID/randomUUID))}
                        :slave-id {:value (str "slave-" (UUID/randomUUID))}
                        :hostname (str "host-" (UUID/randomUUID))}])
        fid (str "framework-id-" (UUID/randomUUID))
        fenzo-maker #(sched/make-fenzo-scheduler nil 100000 1)] ; The params are for offer declining, which should never happen
    (testing "Consume no schedule cases"
      (are [schedule offers] (= [] (sched/match-offer-to-schedule (fenzo-maker) schedule offers))
                             [] (offer-maker 0 0)
                             [] (offer-maker 2 2000)
                             schedule (offer-maker 0 0)
                             schedule (offer-maker 0.5 100)
                             schedule (offer-maker 0.5 1000)
                             schedule (offer-maker 1 500)))
    (testing "Consume Partial schedule cases"
      ;; We're looking for one task to get assigned
      (are [offers] (= 1 (count (mapcat :tasks (sched/match-offer-to-schedule
                                                 (fenzo-maker) schedule offers))))
                    (offer-maker 1 1000)
                    (offer-maker 1.5 1500)))
    (testing "Consume full schedule cases"
      ;; We're looking for the entire schedule to get assigned
      (are [offers] (= (count schedule)
                       (count (mapcat :tasks (sched/match-offer-to-schedule
                                               (fenzo-maker) schedule offers))))
                    (offer-maker 4 4000)
                    (offer-maker 5 5000)))))

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
    (is (= {"whoami" {:mem 0.0 :cpus 0.0}} (sched/get-user->used-resources db "whoami")))
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
        instance-id-3 (create-dummy-instance conn job-id-3
                                             :instance-status :instance.status/running
                                             :task-id task-id-3
                                             :start-time start-time-3)
        test-db (d/db conn)]
    (is (= #{task-id-1 task-id-2} (set (sched/get-lingering-tasks test-db (t/now) timeout-hours timeout-hours))))))

(deftest test-kill-lingering-tasks
  ;; Ensure that lingering tasks are killed properly
  (let [uri "datomic:mem://test-kill-lingering-tasks"
        conn (restore-fresh-database! uri)
        ;; a job has been timeout
        job-id-1 (create-dummy-job conn :user "tsram" :job-state :job.state/running)
        ;; a job has been timeout
        job-id-2 (create-dummy-job conn :user "tsram" :job-state :job.state/running)
        ;; a job is not timeout
        job-id-3 (create-dummy-job conn :user "tsram" :job-state :job.state/running)
        timeout-hours 4
        start-time-1 (joda-datetime->java-date
                       (t/minus (t/now) (t/hours (+ timeout-hours 1))))
        start-time-2 (joda-datetime->java-date
                       (t/minus (t/now) (t/hours (+ timeout-hours 1))))
        start-time-3 (joda-datetime->java-date
                       (t/minus (t/now) (t/hours (- timeout-hours 1))))
        instance-id-1 (create-dummy-instance conn job-id-1
                                             :instance-status :instance.status/unknown
                                             :task-id "task-1"
                                             :start-time start-time-1)
        instance-id-2 (create-dummy-instance conn job-id-2
                                             :instance-status :instance.status/running
                                             :task-id "task-2"
                                             :start-time start-time-2)
        instance-id-3 (create-dummy-instance conn job-id-3
                                             :instance-status :instance.status/running
                                             :task-id "task-3"
                                             :start-time start-time-3)
        test-db (d/db conn)
        config {:timeout-hours timeout-hours}
        dummy-driver (reify msched/SchedulerDriver (kill-task! [_ _] nil))]

    (sched/kill-lingering-tasks (t/now) conn dummy-driver config)

    (is (= :instance.status/failed
           (ffirst (q '[:find ?status
                        :in $ ?i
                        :where
                        [?i :instance/status ?s]
                        [?s :db/ident ?status]]
                      (db conn) instance-id-1))))
    (is (= :max-runtime-exceeded
           (ffirst (q '[:find ?reason-name
                        :in $ ?i
                        :where
                        [?i :instance/reason ?r]
                        [?r :reason/name ?reason-name]]
                      (db conn) instance-id-1))))

    (is (= :instance.status/failed
           (ffirst (q '[:find ?status
                        :in $ ?i
                        :where
                        [?i :instance/status ?s]
                        [?s :db/ident ?status]]
                      (db conn) instance-id-2))))
    (is (= :max-runtime-exceeded
           (ffirst (q '[:find ?reason-name
                        :in $ ?i
                        :where
                        [?i :instance/reason ?r]
                        [?r :reason/name ?reason-name]]
                      (db conn) instance-id-2))))

    (is (= :instance.status/running
           (ffirst (q '[:find ?status
                        :in $ ?i
                        :where
                        [?i :instance/status ?s]
                        [?s :db/ident ?status]]
                      (db conn) instance-id-3))))))

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
    (let [received-offensive-jobs (async/<!! offensive-jobs-ch)]
      (is (= (set received-offensive-jobs) offensive-jobs))
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
    (is (= {:normal (list job-entity-2)
            :gpu ()}
           (sched/rank-jobs test-db test-db offensive-job-filter)))))

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
    (is (= [task-id-2] (sched/get-lingering-tasks test-db now 120 120))))
  )

(deftest test-virtual-machine-lease-adapter
  ;; ensure that the VirtualMachineLeaseAdapter can successfully handle an offer from Mesomatic.
  (let [;; observed offer from Mesomatic API:
        when (System/currentTimeMillis)
        offer #mesomatic.types.Offer{:id #mesomatic.types.OfferID{:value "my-offer-id"}
                                     :framework-id #mesomatic.types.FrameworkID{:value "my-framework-id"}
                                     :slave-id #mesomatic.types.SlaveID{:value "my-slave-id"},
                                     :hostname "slave3",
                                     :resources [#mesomatic.types.Resource{:name "cpus", :type :value-scalar, :scalar 40.0, :ranges [], :set #{}, :role "*"}
                                                 #mesomatic.types.Resource{:name "mem", :type :value-scalar, :scalar 5000.0, :ranges [], :set #{}, :role "*"}
                                                 #mesomatic.types.Resource{:name "disk", :type :value-scalar, :scalar 6000.0, :ranges [], :set #{}, :role "*"}
                                                 #mesomatic.types.Resource{:name "ports", :type :value-ranges, :scalar 0.0, :ranges [#mesomatic.types.ValueRange{:begin 31000, :end 32000}], :set #{}, :role "*"}
                                                 #mesomatic.types.Resource{:name "gpus", :type :value-scalar :scalar 2.0 :role "*"}],
                                     :attributes [],
                                     :executor-ids []}
        adapter (sched/->VirtualMachineLeaseAdapter offer when)]

    (is (= (.getId adapter) "my-offer-id"))
    (is (= (.cpuCores adapter) 40.0))
    (is (= (.diskMB adapter) 6000.0))
    (is (= (.getOfferedTime adapter) when))
    (is (= (.getVMID adapter) "my-slave-id"))
    (is (= (.hostname adapter) "slave3"))
    (is (= (.memoryMB adapter) 5000.0))
    (is (= (.getScalarValues adapter) {"gpus" 2.0 "cpus" 40.0 "disk" 6000.0 "mem" 5000.0 "ports" 0.0}))
    (is (= (-> adapter .portRanges first .getBeg) 31000))
    (is (= (-> adapter .portRanges first .getEnd) 32000))))

(deftest test-interpret-mesos-status
  (let [mesos-status (mesomatic.types/map->TaskStatus {:task-id #mesomatic.types.TaskID{:value "a07708d8-7ab6-404d-b136-a3e2cb2567e3"},
                                                       :state :task-lost,
                                                       :message "Task launched with invalid offers: Offer mesomatic.types.OfferID@1d7408e3 is no longer valid",
                                                       :source :source-master,
                                                       :reason :reason-invalid-offers,
                                                       :slave-id #mesomatic.types.SlaveID{:value "9c4f0a3f-d5e9-4430-809e-ec2e736f4cc3-S1"},
                                                       :executor-id #mesomatic.types.ExecutorID{:value ""},
                                                       :timestamp 1.470654131281046E9,
                                                       :healthy false
                                                       :data (com.google.protobuf.ByteString/copyFrom (.getBytes (pr-str {:percent 85.0}) "UTF-8"))
                                                       :uuid (com.google.protobuf.ByteString/copyFrom (.getBytes "my-uuid" "UTF-8"))})
        interpreted-status (sched/interpret-task-status mesos-status)]
    (is (= (:progress interpreted-status) 85.0))
    (is (= (:task-id interpreted-status) "a07708d8-7ab6-404d-b136-a3e2cb2567e3"))
    (is (= (:task-state interpreted-status) :task-lost))
    (is (= (:reason interpreted-status) :reason-invalid-offers))))

(deftest test-gpu-constraint
  (let [fid #mesomatic.types.FrameworkID{:value "my-framework-id"}
        gpu-offer #mesomatic.types.Offer{:id #mesomatic.types.OfferID{:value "my-offer-id"}
                                         :framework-id fid
                                         :slave-id #mesomatic.types.SlaveID{:value "my-slave-id"},
                                         :hostname "slave3",
                                         :resources [#mesomatic.types.Resource{:name "cpus", :type :value-scalar, :scalar 40.0, :ranges [], :set #{}, :role "*"}
                                                     #mesomatic.types.Resource{:name "mem", :type :value-scalar, :scalar 5000.0, :ranges [], :set #{}, :role "*"}
                                                     #mesomatic.types.Resource{:name "disk", :type :value-scalar, :scalar 6000.0, :ranges [], :set #{}, :role "*"}
                                                     #mesomatic.types.Resource{:name "ports", :type :value-ranges, :scalar 0.0, :ranges [#mesomatic.types.ValueRange{:begin 31000, :end 32000}], :set #{}, :role "*"}
                                                     #mesomatic.types.Resource{:name "gpus", :type :value-scalar :scalar 2.0 :role "*"}],
                                         :attributes [],
                                         :executor-ids []}
        non-gpu-offer #mesomatic.types.Offer{:id #mesomatic.types.OfferID{:value "my-offer-id"}
                                             :framework-id fid
                                             :slave-id #mesomatic.types.SlaveID{:value "my-slave-id"},
                                             :hostname "slave3",
                                             :resources [#mesomatic.types.Resource{:name "cpus", :type :value-scalar, :scalar 40.0, :ranges [], :set #{}, :role "*"}
                                                         #mesomatic.types.Resource{:name "mem", :type :value-scalar, :scalar 5000.0, :ranges [], :set #{}, :role "*"}
                                                         #mesomatic.types.Resource{:name "disk", :type :value-scalar, :scalar 6000.0, :ranges [], :set #{}, :role "*"}
                                                         #mesomatic.types.Resource{:name "ports", :type :value-ranges, :scalar 0.0, :ranges [#mesomatic.types.ValueRange{:begin 31000, :end 32000}], :set #{}, :role "*"}],
                                             :attributes [],
                                             :executor-ids []}
        uri "datomic:mem://test-gpu-constraint"
        conn (restore-fresh-database! uri)
        gpu-job-id (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 1.0)
        other-gpu-job-id (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 1.0)
        non-gpu-job-id (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 0.0)
        db (db conn)
        gpu-job (d/entity db gpu-job-id)
        other-gpu-job (d/entity db other-gpu-job-id)
        non-gpu-job (d/entity db non-gpu-job-id)
        mock-gpu-assignment #(-> (Mockito/when (.getRequest (Mockito/mock TaskAssignmentResult)))
                                 (.thenReturn (sched/->TaskRequestAdapter other-gpu-job
                                                                          (util/job-ent->resources other-gpu-job)
                                                                          (str (UUID/randomUUID))
                                                                          (atom nil)))
                                 (.getMock))]
    (doseq [[type gpu-lease] [["gpu avail"
                               (reify com.netflix.fenzo.VirtualMachineCurrentState
                                 (getHostname [_] "test-host")
                                 (getRunningTasks [_] [])
                                 (getTasksCurrentlyAssigned [_] [])
                                 (getCurrAvailableResources [_] (sched/->VirtualMachineLeaseAdapter gpu-offer 0)))]
                              ["running gpu"
                               (reify com.netflix.fenzo.VirtualMachineCurrentState
                                 (getHostname [_] "test-host")
                                 (getRunningTasks [_] [(sched/->TaskRequestAdapter other-gpu-job
                                                                                   (util/job-ent->resources other-gpu-job)
                                                                                   (str (UUID/randomUUID))
                                                                                   (atom nil))])
                                 (getTasksCurrentlyAssigned [_] [])
                                 (getCurrAvailableResources [_] (sched/->VirtualMachineLeaseAdapter non-gpu-offer 0)))]
                              ["gpu assigned"
                               (reify com.netflix.fenzo.VirtualMachineCurrentState
                                 (getHostname [_] "test-host")
                                 (getRunningTasks [_] [])
                                 (getTasksCurrentlyAssigned [_] [(mock-gpu-assignment)])
                                 (getCurrAvailableResources [_] (sched/->VirtualMachineLeaseAdapter non-gpu-offer 0)))]]]
      (is (.isSuccessful
            (.evaluate (sched/gpu-host-constraint gpu-job)
                       (sched/->TaskRequestAdapter gpu-job
                                                   (util/job-ent->resources gpu-job)
                                                   (str (UUID/randomUUID))
                                                   (atom nil))
                       gpu-lease
                       nil))
          (str "GPU task on GPU host with " type " should succeed"))
      (is (not (.isSuccessful
                 (.evaluate (sched/gpu-host-constraint non-gpu-job)
                            (sched/->TaskRequestAdapter non-gpu-job
                                                        (util/job-ent->resources non-gpu-job)
                                                        (str (UUID/randomUUID))
                                                        (atom nil))
                            gpu-lease
                            nil)))
          (str "GPU task on GPU host with " type " should fail"))
      (is (not (.isSuccessful
                 (.evaluate (sched/gpu-host-constraint gpu-job)
                            (sched/->TaskRequestAdapter gpu-job
                                                        (util/job-ent->resources gpu-job)
                                                        (str (UUID/randomUUID))
                                                        (atom nil))
                            (reify com.netflix.fenzo.VirtualMachineCurrentState
                              (getHostname [_] "test-host")
                              (getRunningTasks [_] [])
                              (getTasksCurrentlyAssigned [_] [])
                              (getCurrAvailableResources [_] (sched/->VirtualMachineLeaseAdapter non-gpu-offer 0)))
                            nil)))
          "GPU task on non GPU host should fail")
      (is (.isSuccessful
            (.evaluate (sched/gpu-host-constraint non-gpu-job)
                       (sched/->TaskRequestAdapter non-gpu-job
                                                   (util/job-ent->resources non-gpu-job)
                                                   (str (UUID/randomUUID))
                                                   (atom nil))
                       (reify com.netflix.fenzo.VirtualMachineCurrentState
                         (getHostname [_] "test-host")
                         (getRunningTasks [_] [])
                         (getTasksCurrentlyAssigned [_] [])
                         (getCurrAvailableResources [_] (sched/->VirtualMachineLeaseAdapter non-gpu-offer 0)))
                       nil))
          "non GPU task on non GPU host should succeed"))))

(deftest test-gpu-share-prioritization
  (let [uri "datomic:mem://test-gpu-shares"
        conn (restore-fresh-database! uri)
        ljin-1 (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 1.0)
        ljin-2 (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 1.0)
        ljin-3 (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 1.0)
        ljin-4 (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 1.0)
        wzhao-1 (create-dummy-job conn :user "wzhao" :ncpus 5.0 :memory 5.0 :gpus 1.0)
        wzhao-2 (create-dummy-job conn :user "wzhao" :ncpus 5.0 :memory 5.0 :gpus 1.0)
        ; Update ljin-1 to running
        inst (create-dummy-instance conn ljin-1 :instance-status :instance.status/unknown)
        _ @(d/transact conn [[:instance/update-state inst :instance.status/running [:reason/name :unknown]]])
        _ (share/set-share! conn "default" :cpus 1.0 :mem 2.0 :gpus 1.0)]
    (testing "test1"
      (let [_ (share/set-share! conn "ljin" :gpus 2.0)
            db (d/db conn)]
        (is (= [ljin-2 wzhao-1 ljin-3 ljin-4 wzhao-2] (map :db/id (:gpu (sched/sort-jobs-by-dru db db)))))))
    (testing "test2"
      (let [_ (share/set-share! conn "ljin" :gpus 1.0)
            db (d/db conn)]
        (is (= [wzhao-1 wzhao-2 ljin-2 ljin-3 ljin-4] (map :db/id (:gpu (sched/sort-jobs-by-dru db db)))))))))

(deftest test-cancelled-task-killer
  (let [uri "datomic:mem://test-gpu-shares"
        conn (restore-fresh-database! uri)
        job1 (create-dummy-job conn :user "mforsyth")
        inst-cancelled (create-dummy-instance conn job1
                                              :instance-status :instance.status/running
                                              :cancelled true)
        inst-not-cancelled (create-dummy-instance conn job1
                                                  :instance-status :instance.status/running)
        inst-not-running (create-dummy-instance conn job1
                                                :instance-status :instance.status/success
                                                :cancelled true)]

    (testing "killable-cancelled-tasks"
      (let [db (d/db conn)
            killable (sched/killable-cancelled-tasks db)]
        (is (= 1 (count killable)))
        (is (= (-> killable first :db/id) inst-cancelled))))))


(deftest test-handle-status-update
  (let [uri "datomic:mem://test-handle-status-update"
        conn (restore-fresh-database! uri)
        tasks-killed (atom #{})
        driver (reify msched/SchedulerDriver
                 (kill-task! [_ task] (swap! tasks-killed conj (:value task)))) ; Conjoin the task-id
        driver-atom (atom nil)
        fenzo (sched/make-fenzo-scheduler driver-atom 1500 0.8)
        make-dummy-status-update (fn [task-id reason state & {:keys [progress] :or {progress nil}}]
                                   (let [task {:task-id {:value task-id}
                                               :reason reason
                                               :state state}]
                                     task))]
    (testing "Mesos task death"
      (let [job-id (create-dummy-job conn :user "tsram" :job-state :job.state/running)
            task-id "task1"
            instance-id (create-dummy-instance conn job-id
                                               :instance-status :instance.status/running
                                               :task-id task-id)]
        ; Wait for async database transaction inside handle-status-update
        (async/<!! (sched/handle-status-update conn driver fenzo
                                               (make-dummy-status-update task-id :reason-gc-error :task-killed)))
        (is (= :instance.status/failed
               (ffirst (q '[:find ?status
                            :in $ ?i
                            :where
                            [?i :instance/status ?s]
                            [?s :db/ident ?status]]
                          (db conn) instance-id))))
        (is (= :mesos-gc-error
               (ffirst (q '[:find ?reason-name
                            :in $ ?i
                            :where
                            [?i :instance/reason ?r]
                            [?r :reason/name ?reason-name]]
                          (db conn) instance-id))))))
    (testing "Pre-existing reason is not mea-culpa. New reason is. Job still out of retries because non-mea-culpa takes preference"
      (let [job-id (create-dummy-job conn
                                     :user "tsram"
                                     :job-state :job.state/completed
                                     :retry-count 3)
            task-id "task2"
            _ (create-dummy-instance conn job-id
                                     :instance-status :instance.status/failed
                                     :reason :unknown)
            _ (create-dummy-instance conn job-id
                                     :instance-status :instance.status/failed
                                     :reason :unknown)
            _ (create-dummy-instance conn job-id
                                     :instance-status :instance.status/failed
                                     :reason :mesos-master-disconnected) ; Mea-culpa
            instance-id (create-dummy-instance conn job-id
                                               :instance-status :instance.status/failed
                                               :task-id task-id
                                               :reason :max-runtime-exceeded)] ; Previous reason is not mea-culpa
        ; Status update says slave got restarted (mea-culpa)
        (async/<!! (sched/handle-status-update conn driver fenzo
                                               (make-dummy-status-update task-id :mesos-slave-restarted :task-killed)))
        ; Assert old reason persists
        (is (= :max-runtime-exceeded
               (ffirst (q '[:find ?reason-name
                            :in $ ?i
                            :where
                            [?i :instance/reason ?r]
                            [?r :reason/name ?reason-name]]
                          (db conn) instance-id))))
        ; Assert job still marked as out of retries
        (is (= :job.state/completed
               (ffirst (q '[:find ?state
                            :in $ ?j
                            :where
                            [?j :job/state ?s]
                            [?s :db/ident ?state]]
                          (db conn) job-id))))
        ))
    (testing "Tasks of completed jobs are killed"
      (let [job-id (create-dummy-job conn
                                     :user "tsram"
                                     :job-state :job.state/completed
                                     :retry-count 3)
            task-id-a "taska"
            task-id-b "taskb"
            instance-id-a (create-dummy-instance conn job-id
                                                 :instance-status :instance.status/running
                                                 :task-id task-id-a
                                                 :reason :unknown)
            instance-id-b (create-dummy-instance conn job-id
                                                 :instance-status :instance.status/success
                                                 :task-id task-id-b
                                                 :reason :unknown)]
        (async/<!! (sched/handle-status-update conn driver fenzo
                                               (make-dummy-status-update task-id-a :mesos-slave-restarted :task-running)))
        (is (true? (contains? @tasks-killed task-id-a)))
        ))

    (testing "instance persists mesos-start-time when task is first known to be starting or running"
      (let [job-id (create-dummy-job conn
                                     :user "mforsyth"
                                     :job-state :job.state/running
                                     :retry-count 3)
            task-id "task-mesos-start-time"
            instance-id-a (create-dummy-instance conn job-id
                                                 :instance-status :instance.status/unknown
                                                 :task-id task-id
                                                 :reason :unknown)
            mesos-start-time (fn [] (-> conn
                                        d/db
                                        (d/entity [:instance/task-id task-id])
                                        :instance/mesos-start-time))]
        (is (nil? (mesos-start-time)))
        (async/<!! (sched/handle-status-update conn driver fenzo
                                               (make-dummy-status-update task-id :unknown :task-staging)))
        (is (nil? (mesos-start-time)))
        (async/<!! (sched/handle-status-update conn driver fenzo
                                               (make-dummy-status-update task-id :unknown :task-running)))
        (let [first-observed-start-time (.getTime (mesos-start-time))]
          (is (not (nil? first-observed-start-time)))
          (async/<!! (sched/handle-status-update conn driver fenzo
                                                 (make-dummy-status-update task-id :unknown :task-running)))
          (is (= first-observed-start-time (.getTime (mesos-start-time)))))))))

(deftest test-handle-stragglers
  (let [uri "datomic:mem://test-handle-stragglers"
        conn (restore-fresh-database! uri)]
    (testing "one cycle"
      (let [;; no straggler handling
            group-ent-id (create-dummy-group conn)
            job-a (create-dummy-job conn :group group-ent-id)
            _ (create-dummy-instance conn job-a :instance-status :instance.status/success
                                     :start-time (tc/to-date (t/ago (t/hours 3)))
                                     :end-time (tc/to-date (t/ago (t/hours 2))))
            job-b (create-dummy-job conn :group group-ent-id)
            _ (create-dummy-instance conn job-b :instance-status :instance.status/running
                                     :start-time (tc/to-date (t/ago (t/minutes 30))))
            ;; Straggler handling configured
            straggler-handling {:straggler-handling/type :straggler-handling.type/quantile-deviation
                                :straggler-handling/parameters
                                {:straggler-handling.quantile-deviation/quantile 0.5
                                 :straggler-handling.quantile-deviation/multiplier 2.0}}
            group-ent-id (create-dummy-group conn :straggler-handling straggler-handling)
            job-c (create-dummy-job conn :group group-ent-id)
            _ (create-dummy-instance conn job-c :instance-status :instance.status/success
                                     :start-time (tc/to-date (t/ago (t/hours 3)))
                                     :end-time (tc/to-date (t/ago (t/hours 2))))
            job-d (create-dummy-job conn :group group-ent-id)
            straggler (create-dummy-instance conn job-d :instance-status :instance.status/running
                                             :start-time (tc/to-date (t/ago (t/minutes 190))))
            straggler-task (d/entity (d/db conn) straggler)
            kill-fn (fn [task]
                      (is (= task straggler-task)))]
        ;; Check that group with straggler handling configured has instance killed
        (sched/handle-stragglers conn kill-fn)))))


(deftest test-receive-offers
  (let [declined-offer-ids-atom (atom [])
        offers-chan (async/chan (async/buffer 1))
        mock-driver (reify msched/SchedulerDriver
                      (decline-offer [driver id]
                        (swap! declined-offer-ids-atom conj id)))
        offer-1 {:id {:value "foo"}}
        offer-2 {:id {:value "bar"}}
        offer-3 {:id {:value "baz"}}]
    (testing "offer chan overflow"
      (sched/receive-offers offers-chan mock-driver [offer-1])
      @(sched/receive-offers offers-chan mock-driver [offer-2])
      @(sched/receive-offers offers-chan mock-driver [offer-3])
      (is (= @declined-offer-ids-atom [(:id offer-2) (:id offer-3)])))))

(deftest test-below-quota?
  (testing "not using quota"
    (is (sched/below-quota? {:count 5, :cpus 15, :mem 9999}
                            {:count 0, :cpus 0, :mem 0})))
  (testing "not using quota with extra keys"
    (is (sched/below-quota? {:count 5, :cpus 15, :mem 9999, :foo 2, :bar 3}
                            {:count 0, :cpus 0, :mem 0})))
  (testing "inside quota"
    (is (sched/below-quota? {:count 5, :cpus 15, :mem 9999}
                            {:count 4, :cpus 10, :mem 1234})))
  (testing "at quota limit"
    (is (sched/below-quota? {:count 5, :cpus 15, :mem 9999}
                            {:count 5, :cpus 15, :mem 9999})))
  (testing "exceed quota limit - count"
    (is (not (sched/below-quota? {:count 5, :cpus 15, :mem 9999}
                                 {:count 6, :cpus 10, :mem 1234}))))
  (testing "exceed quota limit - cpus"
    (is (not (sched/below-quota? {:count 5, :cpus 15, :mem 9999}
                                 {:count 4, :cpus 20, :mem 1234}))))
  (testing "exceed quota limit - mem"
    (is (not (sched/below-quota? {:count 5, :cpus 15, :mem 1234}
                                 {:count 4, :cpus 10, :mem 4321}))))
  (testing "exceed quota limit - gpus"
    (is (not (sched/below-quota? {:count 5, :cpus 15, :mem 9999, :gpus 4}
                                 {:count 4, :cpus 10, :mem 1234, :gpus 5}))))
  (testing "at quota limit with extra keys"
    (is (sched/below-quota? {:count 5, :cpus 15, :mem 9999, :gpus 4}
                            {:count 5, :cpus 15, :mem 9999}))))

(deftest test-job->usage
  (testing "cpus and mem usage"
    (is (= {:count 1, :cpus 2, :mem 2048}
           (sched/job->usage {:job/resource [{:resource/type :cpus, :resource/amount 2}
                                             {:resource/type :mem, :resource/amount 2048}]}))))
  (testing "cpus and mem usage - ignore extra resources"
    (is (= {:count 1, :cpus 2, :mem 2048}
           (sched/job->usage {:job/resource [{:resource/type :cpus, :resource/amount 2}
                                             {:resource/type :mem, :resource/amount 2048}
                                             {:resource/type :uri, :resource.uri/value "www.test.com"}]}))))
  (testing "cpus mem and gpus usage"
    (is (= {:count 1, :cpus 2, :gpus 10, :mem 2048}
           (sched/job->usage {:job/resource [{:resource/type :cpus, :resource/amount 2}
                                             {:resource/type :mem, :resource/amount 2048}
                                             {:resource/type :gpus, :resource/amount 10}
                                             {:resource/type :uri, :resource.uri/value "www.test.com"}]}))))
  (testing "cpus mem and gpus usage - ignore extra resources"
    (is (= {:count 1, :cpus 2, :gpus 10, :mem 2048}
           (sched/job->usage {:job/resource [{:resource/type :cpus, :resource/amount 2}
                                             {:resource/type :mem, :resource/amount 2048}
                                             {:resource/type :gpus, :resource/amount 10}]}))))
  (testing "ensures cpu value - gpus absent"
    (is (= {:count 1, :cpus nil, :mem 2048}
           (sched/job->usage {:job/resource [{:resource/type :mem, :resource/amount 2048}]}))))
  (testing "ensures cpu value - gpus present"
    (is (= {:count 1, :cpus nil, :gpus 10, :mem 2048}
           (sched/job->usage {:job/resource [{:resource/type :mem, :resource/amount 2048}
                                             {:resource/type :gpus, :resource/amount 10}]}))))
  (testing "ensures mem value - gpus absent"
    (is (= {:count 1, :cpus 2, :mem nil}
           (sched/job->usage {:job/resource [{:resource/type :cpus, :resource/amount 2}]}))))
  (testing "ensures mem value - gpus present"
    (is (= {:count 1, :cpus 2, :gpus 10, :mem nil}
           (sched/job->usage {:job/resource [{:resource/type :cpus, :resource/amount 2}
                                             {:resource/type :gpus, :resource/amount 10}]})))))

(deftest test-filter-based-on-quota
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
             (sched/filter-based-on-quota {test-user {:count 1, :cpus 2, :mem 1024}} user->usage queue))))
    (testing "all jobs included"
      (is (= [(make-job 1 2 2048) (make-job 2 1 1024) (make-job 3 3 4096) (make-job 4 1 1024)]
             (sched/filter-based-on-quota {test-user {:count 10, :cpus 20, :mem 32768}} user->usage queue))))
    (testing "room for later jobs not included"
      (is (= [(make-job 1 2 2048) (make-job 2 1 1024)]
             (sched/filter-based-on-quota {test-user {:count 4, :cpus 20, :mem 6144}} user->usage queue))))))

(deftest test-category->pending-jobs->category->considerable-jobs
  (let [uri "datomic:mem://test-category-pending-jobs-category-considerable-jobs"
        conn (restore-fresh-database! uri)
        test-db (d/db conn)
        test-user (System/getProperty "user.name")
        group-ent-id (create-dummy-group conn)
        job-1 (d/entity test-db (create-dummy-job conn :group group-ent-id :ncpus 3 :memory 2048))
        job-2 (d/entity test-db (create-dummy-job conn :group group-ent-id :ncpus 13 :memory 1024))
        job-3 (d/entity test-db (create-dummy-job conn :group group-ent-id :ncpus 7 :memory 4096))
        job-4 (d/entity test-db (create-dummy-job conn :group group-ent-id :ncpus 11 :memory 1024))
        job-5 (d/entity test-db (create-dummy-job conn :group group-ent-id :ncpus 5 :memory 2048 :gpus 2))
        job-6 (d/entity test-db (create-dummy-job conn :group group-ent-id :ncpus 19 :memory 1024 :gpus 4))
        category->pending-jobs {:normal [job-1 job-2 job-3 job-4], :gpu [job-5 job-6]}]

    (testing "jobs inside usage quota"
      (let [user->usage {test-user {:count 1, :cpus 2, :mem 1024, :gpus 0}}
            user->quota {test-user {:count 10, :cpus 50, :mem 32768, :gpus 10}}
            num-considerable 5]
        (is (= {:normal [job-1 job-2 job-3 job-4], :gpu [job-5 job-6]}
               (sched/category->pending-jobs->category->considerable-jobs
                 (d/db conn) category->pending-jobs user->quota user->usage num-considerable)))))
    (testing "some jobs inside usage quota"
      (let [user->usage {test-user {:count 1, :cpus 2, :mem 1024, :gpus 0}}
            user->quota {test-user {:count 5, :cpus 10, :mem 4096, :gpus 10}}
            num-considerable 5]
        (is (= {:normal [job-1], :gpu [job-5]}
               (sched/category->pending-jobs->category->considerable-jobs
                 (d/db conn) category->pending-jobs user->quota user->usage num-considerable)))))
    (testing "some jobs inside usage quota - quota gpus not ignored"
      (let [user->usage {test-user {:count 1, :cpus 2, :mem 1024, :gpus 0}}
            user->quota {test-user {:count 5, :cpus 10, :mem 4096, :gpus 0}}
            num-considerable 5]
        (is (= {:normal [job-1], :gpu []}
               (sched/category->pending-jobs->category->considerable-jobs
                 (d/db conn) category->pending-jobs user->quota user->usage num-considerable)))))
    (testing "all jobs exceed quota"
      (let [user->usage {test-user {:count 1, :cpus 2, :mem 1024, :gpus 0}}
            user->quota {test-user {:count 5, :cpus 3, :mem 4096, :gpus 10}}
            num-considerable 5]
        (is (= {:normal [], :gpu []}
               (sched/category->pending-jobs->category->considerable-jobs
                 (d/db conn) category->pending-jobs user->quota user->usage num-considerable)))))))

(deftest test-extract-matched-job-uuids
  (let [create-task-result (fn [job-uuid cpus mem gpus]
                             (-> (Mockito/when (.getRequest (Mockito/mock TaskAssignmentResult)))
                                 (.thenReturn (sched/->TaskRequestAdapter
                                                {:job/uuid job-uuid
                                                 :job/resource (cond-> [{:resource/type :resource.type/mem, :resource/amount 1000.0}
                                                                        {:resource/type :resource.type/cpus, :resource/amount 1.0}]
                                                                       gpus (conj {:resource/type :resource.type/gpus, :resource/amount gpus}))}
                                                {}
                                                (str "task-id-" job-uuid)
                                                []))
                                 (.getMock)))
        matches [{:tasks [(create-task-result "job-1" 1 1024 nil)
                          (create-task-result "job-2" 2 2048 nil)
                          (create-task-result "job-3" 3 1024 1)]}
                 {:tasks [(create-task-result "job-4" 4 1024 nil)
                          (create-task-result "job-5" 5 2048 2)]}
                 {:tasks [(create-task-result "job-6" 6 1024 3)]}
                 {:tasks [(create-task-result "job-7" 7 1024 nil)]}]]
    (is (= {:matched-gpu-job-uuids #{"job-3" "job-5" "job-6"}
            :matched-normal-job-uuids #{"job-1" "job-2" "job-4" "job-7"}}
           (sched/extract-matched-job-uuids matches)))))

(deftest test-remove-matched-jobs-from-pending-jobs
  (let [create-jobs-in-range (fn [start-inc end-exc]
                               (map (fn [id] {:job/uuid id})
                                    (range start-inc end-exc)))]
    (testing "empty matched jobs"
      (let [category->pending-jobs {:normal (create-jobs-in-range 1 10)
                                    :gpu (create-jobs-in-range 10 15)}
            matched-normal-job-uuids #{}
            matched-gpu-job-uuids #{}
            expected-category->pending-jobs category->pending-jobs]
        (is (= expected-category->pending-jobs
               (sched/remove-matched-jobs-from-pending-jobs
                 category->pending-jobs matched-normal-job-uuids matched-gpu-job-uuids)))))

    (testing "unknown matched jobs"
      (let [category->pending-jobs {:normal (create-jobs-in-range 1 10)
                                    :gpu (create-jobs-in-range 10 15)}
            matched-normal-job-uuids (set (range 20 25))
            matched-gpu-job-uuids (set (range 30 35))
            expected-category->pending-jobs category->pending-jobs]
        (is (= expected-category->pending-jobs
               (sched/remove-matched-jobs-from-pending-jobs
                 category->pending-jobs matched-normal-job-uuids matched-gpu-job-uuids)))))

    (testing "non-empty matched normal jobs"
      (let [category->pending-jobs {:normal (create-jobs-in-range 1 10)
                                    :gpu (create-jobs-in-range 10 15)}
            matched-normal-job-uuids (set (range 1 5))
            matched-gpu-job-uuids #{}
            expected-category->pending-jobs {:normal (create-jobs-in-range 5 10)
                                             :gpu (create-jobs-in-range 10 15)}]
        (is (= expected-category->pending-jobs
               (sched/remove-matched-jobs-from-pending-jobs
                 category->pending-jobs matched-normal-job-uuids matched-gpu-job-uuids)))))

    (testing "non-empty matched gpu jobs"
      (let [category->pending-jobs {:normal (create-jobs-in-range 1 10)
                                    :gpu (create-jobs-in-range 10 15)}
            matched-normal-job-uuids #{}
            matched-gpu-job-uuids (set (range 10 12))
            expected-category->pending-jobs {:normal (create-jobs-in-range 1 10)
                                             :gpu (create-jobs-in-range 12 15)}]
        (is (= expected-category->pending-jobs
               (sched/remove-matched-jobs-from-pending-jobs
                 category->pending-jobs matched-normal-job-uuids matched-gpu-job-uuids)))))

    (testing "non-empty matched normal and gpu jobs"
      (let [category->pending-jobs {:normal (create-jobs-in-range 1 10)
                                    :gpu (create-jobs-in-range 10 15)}
            matched-normal-job-uuids (set (range 5 10))
            matched-gpu-job-uuids (set (range 10 12))
            expected-category->pending-jobs {:normal (create-jobs-in-range 1 5)
                                             :gpu (create-jobs-in-range 12 15)}]
        (is (= expected-category->pending-jobs
               (sched/remove-matched-jobs-from-pending-jobs
                 category->pending-jobs matched-normal-job-uuids matched-gpu-job-uuids)))))))

(deftest test-handle-resource-offers
  (let [uri "datomic:mem://test-handle-resource-offers"
        launched-offer-ids-atom (atom [])
        launched-task-ids-atom (atom [])
        driver (reify msched/SchedulerDriver
                 (launch-tasks! [_ offer-id tasks]
                   (swap! launched-offer-ids-atom conj (-> offer-id first :value))
                   (swap! launched-task-ids-atom concat (map (fn [task]
                                                               ;; :mem is not included as the cpus and gpus numbers are used in the assertions
                                                               {:id (get-in task [:task-id :value])
                                                                :cpus (->> task
                                                                           :resources
                                                                           (filter #(= "cpus" (:name %1)))
                                                                           first
                                                                           :scalar
                                                                           int)
                                                                :gpus (-> (->> task
                                                                               :resources
                                                                               (filter #(= "gpus" (:name %1)))
                                                                               first
                                                                               :scalar)
                                                                          (or 0)
                                                                          int)})
                                                             tasks))))
        offer-maker (fn [cpus mem gpus]
                      {:resources [{:name "cpus", :scalar cpus, :type :value-scalar, :role "cook"}
                                   {:name "mem", :scalar mem, :type :value-scalar, :role "cook"}
                                   {:name "gpus", :scalar gpus, :type :value-scalar, :role "cook"}]
                       :id {:value (str "id-" (UUID/randomUUID))}
                       :slave-id {:value (str "slave-" (UUID/randomUUID))}
                       :hostname (str "host-" (UUID/randomUUID))})
        fid #mesomatic.types.FrameworkID{:value "my-framework-id"}
        test-user (System/getProperty "user.name")
        offers-chan (async/chan (async/buffer 10))
        offer-1 (offer-maker 10 2048 0)
        offer-2 (offer-maker 20 16384 0)
        offer-3 (offer-maker 30 8192 0)
        offer-4 (offer-maker 4 2048 0)
        offer-5 (offer-maker 4 1024 0)
        offer-6 (offer-maker 10 4096 10)
        offer-7 (offer-maker 20 4096 5)
        offer-8 (offer-maker 30 16384 1)
        run-handle-resource-offers! (fn [num-considerable offers & {:keys [user-quota user->usage]}]
                                      (reset! launched-offer-ids-atom [])
                                      (reset! launched-task-ids-atom [])
                                      (let [conn (restore-fresh-database! uri)
                                            test-db (d/db conn)
                                            driver-atom (atom nil)
                                            ^TaskScheduler fenzo (sched/make-fenzo-scheduler driver-atom 1500 0.8)
                                            group-ent-id (create-dummy-group conn)
                                            job-1 (d/entity test-db (create-dummy-job conn :group group-ent-id :ncpus 3 :memory 2048))
                                            job-2 (d/entity test-db (create-dummy-job conn :group group-ent-id :ncpus 13 :memory 1024))
                                            job-3 (d/entity test-db (create-dummy-job conn :group group-ent-id :ncpus 7 :memory 4096))
                                            job-4 (d/entity test-db (create-dummy-job conn :group group-ent-id :ncpus 11 :memory 1024))
                                            job-5 (d/entity test-db (create-dummy-job conn :group group-ent-id :ncpus 5 :memory 2048 :gpus 2))
                                            job-6 (d/entity test-db (create-dummy-job conn :group group-ent-id :ncpus 19 :memory 1024 :gpus 4))
                                            category->pending-jobs-atom (atom {:normal [job-1 job-2 job-3 job-4]
                                                                               :gpu [job-5 job-6]})
                                            user->usage (or user->usage {test-user {:count 1, :cpus 2, :mem 1024, :gpus 0}})
                                            user->quota (or user-quota {test-user {:count 10, :cpus 50, :mem 32768, :gpus 10}})
                                            result (sched/handle-resource-offers! conn driver fenzo fid category->pending-jobs-atom user->usage user->quota
                                                                                  num-considerable offers-chan offers)]
                                        (async/>!! offers-chan :end-marker)
                                        result))]

    (testing "enough offers for all normal jobs"
      (let [num-considerable 10
            offers [offer-1 offer-2 offer-3]]
        (is (run-handle-resource-offers! num-considerable offers))
        (is (= :end-marker (async/<!! offers-chan)))
        (is (= 3 (count @launched-offer-ids-atom)))
        (is (= (+ 3 13 7 11) (reduce + (map :cpus @launched-task-ids-atom))))
        (is (zero? (reduce + (map :gpus @launched-task-ids-atom))))))

    (testing "enough offers for all normal jobs, limited by num-considerable of 1"
      (let [num-considerable 1
            offers [offer-1 offer-2 offer-3]]
        (is (run-handle-resource-offers! num-considerable offers))
        (is (= :end-marker (async/<!! offers-chan)))
        (is (= 1 (count @launched-offer-ids-atom)))
        (is (= (+ 3) (reduce + (map :cpus @launched-task-ids-atom))))
        (is (zero? (reduce + (map :gpus @launched-task-ids-atom))))))

    (testing "enough offers for all normal jobs, limited by num-considerable of 2"
      (let [num-considerable 2
            offers [offer-1 offer-2 offer-3]]
        (is (run-handle-resource-offers! num-considerable offers))
        (is (= :end-marker (async/<!! offers-chan)))
        (is (= 2 (count @launched-offer-ids-atom)))
        (is (= (+ 3 13) (reduce + (map :cpus @launched-task-ids-atom))))
        (is (zero? (reduce + (map :gpus @launched-task-ids-atom))))))

    (testing "enough offers for all normal jobs, limited by quota"
      (let [num-considerable 1
            offers [offer-1 offer-2 offer-3]
            user-quota {test-user {:count 5, :cpus 45, :mem 16384, :gpus 0}}]
        (is (run-handle-resource-offers! num-considerable offers :user-quota user-quota))
        (is (= :end-marker (async/<!! offers-chan)))
        (is (= 1 (count @launched-offer-ids-atom)))
        (is (= (+ 3) (reduce + (map :cpus @launched-task-ids-atom))))
        (is (zero? (reduce + (map :gpus @launched-task-ids-atom))))))

    (testing "enough offers for all normal jobs, limited by usage capacity"
      (let [num-considerable 1
            offers [offer-1 offer-2 offer-3]
            user->usage {test-user {:count 5, :cpus 5, :mem 16384, :gpus 0}}]
        (is (run-handle-resource-offers! num-considerable offers :user->usage user->usage))
        (is (= :end-marker (async/<!! offers-chan)))
        (is (= 1 (count @launched-offer-ids-atom)))
        (is (= (+ 3) (reduce + (map :cpus @launched-task-ids-atom))))
        (is (zero? (reduce + (map :gpus @launched-task-ids-atom))))))

    (testing "offer for single normal job"
      (let [num-considerable 10
            offers [offer-4]]
        (is (run-handle-resource-offers! num-considerable offers))
        (is (= :end-marker (async/<!! offers-chan)))
        (is (= 1 (count @launched-offer-ids-atom)))
        (is (= (+ 3) (reduce + (map :cpus @launched-task-ids-atom))))
        (is (zero? (reduce + (map :gpus @launched-task-ids-atom))))))

    (testing "offer for first three normal jobs"
      (let [num-considerable 10
            offers [offer-3]]
        (is (run-handle-resource-offers! num-considerable offers))
        (is (= :end-marker (async/<!! offers-chan)))
        (is (= 1 (count @launched-offer-ids-atom)))
        (is (= (+ 3 13 7) (reduce + (map :cpus @launched-task-ids-atom))))
        (is (zero? (reduce + (map :gpus @launched-task-ids-atom))))))

    (testing "offer not fit for any normal job"
      (let [num-considerable 10
            offers [offer-5]]
        (is (run-handle-resource-offers! num-considerable offers))
        (is (= :end-marker (async/<!! offers-chan)))
        (is (zero? (count @launched-offer-ids-atom)))
        (is (zero? (reduce + (map :cpus @launched-task-ids-atom))))
        (is (zero? (reduce + (map :gpus @launched-task-ids-atom))))))

    (testing "offer fit but user has too little quota"
      (let [num-considerable 10
            offers [offer-1 offer-2 offer-3]
            user-quota {test-user {:count 5, :cpus 4, :mem 4096, :gpus 0}}]
        (is (run-handle-resource-offers! num-considerable offers :user-quota user-quota))
        (is (= :end-marker (async/<!! offers-chan)))
        (is (zero? (count @launched-offer-ids-atom)))
        (is (zero? (reduce + (map :cpus @launched-task-ids-atom))))
        (is (zero? (reduce + (map :gpus @launched-task-ids-atom))))))

    (testing "offer fit but user has capacity usage"
      (let [num-considerable 10
            offers [offer-1 offer-2 offer-3]
            user->usage {test-user {:count 10, :cpus 50, :mem 32768, :gpus 10}}]
        (is (run-handle-resource-offers! num-considerable offers :user->usage user->usage))
        (is (= :end-marker (async/<!! offers-chan)))
        (is (zero? (count @launched-offer-ids-atom)))
        (is (zero? (reduce + (map :cpus @launched-task-ids-atom))))
        (is (zero? (reduce + (map :gpus @launched-task-ids-atom))))))

    (testing "gpu offers for all gpu jobs"
      (let [num-considerable 10
            offers [offer-6 offer-7]]
        (is (not (run-handle-resource-offers! num-considerable offers))) ; normal jobs form the head
        (is (= :end-marker (async/<!! offers-chan)))
        (is (= 2 (count @launched-offer-ids-atom)))
        (is (= (+ 5 19) (reduce + (map :cpus @launched-task-ids-atom))))
        (is (= (+ 2 4) (reduce + (map :gpus @launched-task-ids-atom))))))

    (testing "gpu offer for single gpu job"
      (let [num-considerable 10
            offers [offer-6]]
        (is (not (run-handle-resource-offers! num-considerable offers))) ; normal jobs form the head
        (is (= :end-marker (async/<!! offers-chan)))
        (is (= 1 (count @launched-offer-ids-atom)))
        (is (= (+ 5) (reduce + (map :cpus @launched-task-ids-atom))))
        (is (= (+ 2) (reduce + (map :gpus @launched-task-ids-atom))))))

    (testing "gpu offer matching no gpu job"
      (let [num-considerable 10
            offers [offer-8]]
        (is (run-handle-resource-offers! num-considerable offers))
        (is (= :end-marker (async/<!! offers-chan)))
        (is (zero? (count @launched-offer-ids-atom)))
        (is (zero? (reduce + (map :cpus @launched-task-ids-atom))))
        (is (zero? (reduce + (map :gpus @launched-task-ids-atom))))))

    (testing "offer for single normal and single gpu job"
      (let [num-considerable 10
            offers [offer-4 offer-6]]
        (is (run-handle-resource-offers! num-considerable offers))
        (is (= :end-marker (async/<!! offers-chan)))
        (is (= 2 (count @launched-offer-ids-atom)))
        (is (= (+ 3 5) (reduce + (map :cpus @launched-task-ids-atom))))
        (is (= (+ 0 2) (reduce + (map :gpus @launched-task-ids-atom))))))))

(comment
  (run-tests))
