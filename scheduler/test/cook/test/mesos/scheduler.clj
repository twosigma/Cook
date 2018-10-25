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
  (:use [clojure.test])
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [cook.config :as config]
            [cook.mesos.data-locality :as dl]
            [cook.mesos.heartbeat :as heartbeat]
            [cook.mesos.sandbox :as sandbox]
            [cook.mesos.scheduler :as sched]
            [cook.mesos.share :as share]
            [cook.mesos.util :as util]
            [cook.test.testutil :refer [restore-fresh-database! create-dummy-group create-dummy-job
                                        create-dummy-instance init-agent-attributes-cache poll-until wait-for
                                        create-dummy-job-with-instances create-pool]]
            [criterium.core :as crit]
            [datomic.api :as d :refer (q db)]
            [mesomatic.scheduler :as msched]
            [mesomatic.types :as mtypes]
            [plumbing.core :as pc])
  (:import (clojure.lang ExceptionInfo)
           (com.netflix.fenzo SimpleAssignmentResult TaskAssignmentResult TaskRequest TaskScheduler)
           (com.netflix.fenzo.plugins BinPackingFitnessCalculators)
           (java.util UUID)
           (java.util.concurrent CountDownLatch TimeUnit)
           (org.mockito Mockito)))

(def datomic-uri "datomic:mem://test-mesos-jobs")

(defn make-uuid
  []
  (str (UUID/randomUUID)))

(defn create-running-job
  [conn host & args]
  (let [job (apply create-dummy-job (cons conn args))
        inst (create-dummy-instance conn job :instance-status :instance.status/running :hostname host)]
    [job inst]))

(defn make-dummy-scheduler
  []
  (let [driver (atom nil)]
    (.. (com.netflix.fenzo.TaskScheduler$Builder.)
        (disableShortfallEvaluation) ;; We're not using the autoscaling features
        (withLeaseOfferExpirySecs 1) ;; should be at least 1 second
        (withRejectAllExpiredOffers)
        (withFitnessCalculator BinPackingFitnessCalculators/cpuMemBinPacker)
        (withFitnessGoodEnoughFunction (reify com.netflix.fenzo.functions.Func1
                                         (call [_ fitness]
                                           (> fitness 0.8))))
        (withLeaseRejectAction (reify com.netflix.fenzo.functions.Action1
                                 (call [_ lease] (do))))
        (build))))

(defn make-resource
  [name type val]
  (mtypes/map->Resource (merge
                          {:name name
                           :type type
                           :scalar nil
                           :ranges []
                           :set #{}
                           :role "*"}
                          (case type
                            :value-scalar {:scalar val}
                            :value-ranges {:ranges [(mtypes/map->ValueRange val)]}
                            :value-set {:set #{val}}
                            {}))))

(defn make-offer-resources
  [cpus mem disk ports gpus]
  [(make-resource "cpus" :value-scalar cpus)
   (make-resource "mem" :value-scalar mem)
   (make-resource "disk" :value-scalar disk)
   (make-resource "ports" :value-ranges ports)
   (make-resource "gpus" :value-scalar gpus)])

(defn make-attribute
  [name type val]
  (mtypes/map->Attribute (merge
                           {:name name
                            :type type
                            :scalar nil
                            :ranges []
                            :set #{}
                            :role "*"}
                           (case type
                             :value-scalar {:scalar val}
                             :value-text {:text val}
                             :value-ranges {:ranges [(mtypes/map->ValueRange val)]}
                             :value-set {:set #{val}}
                             nil))))

(defn make-offer-attributes
  [attrs]
  (mapv #(make-attribute (key %) :value-text (val %)) attrs))

(defn make-mesos-offer
  [id framework-id slave-id hostname & {:keys [cpus mem disk ports gpus attrs]
                                        :or {cpus 40.0 mem 5000.0 disk 6000.0 ports {:begin 31000 :end 32000} gpus 0.0 attrs {}}}]
  (mtypes/map->Offer {:id (mtypes/map->OfferID {:value id})
                      :framework-id framework-id
                      :slave-id (mtypes/map->SlaveID {:value slave-id})
                      :hostname hostname
                      :resources (make-offer-resources cpus mem disk ports gpus)
                      :attributes (make-offer-attributes (merge attrs {"HOSTNAME" hostname}))
                      :executor-ids []}))

(defn make-vm-offer
  [framework-id host offer-id & {:keys [attrs cpus mem disk] :or {attrs {} cpus 100.0 mem 100000.0 disk 100000.0}}]
  (sched/->VirtualMachineLeaseAdapter
    (make-mesos-offer offer-id framework-id "test-slave" host
                      :cpus cpus :mem mem :disk disk :attrs attrs) 0))

;(.getOffer (make-vm-offer (make-uuid) "lol" (make-uuid)))

(defn schedule-and-run-jobs
  [conn framework-id scheduler offers job-ids]
  (let [db (d/db conn)
        jobs (->> job-ids
                  (map #(d/entity db %))
                  (map #(util/job-ent->map %)))
        task-ids (take (count jobs) (repeatedly #(str (java.util.UUID/randomUUID))))
        guuid->considerable-cotask-ids (util/make-guuid->considerable-cotask-ids (zipmap jobs task-ids))
        cache (atom (cache/fifo-cache-factory {} :threshold (count job-ids)))
        tasks (map #(sched/make-task-request db %1 :task-id %2 :guuid->considerable-cotask-ids guuid->considerable-cotask-ids
                                             :running-cotask-cache cache)
                   jobs task-ids)
        result (-> scheduler
                   (.scheduleOnce tasks offers))
        tasks-assigned (some->> result
                                .getResultMap
                                vals
                                (mapcat #(.getTasksAssigned %)))
        tid-to-task (zipmap task-ids tasks)
        tid-to-job (zipmap task-ids jobs)
        tid-to-hostname (into {} (map (juxt #(.getTaskId %) #(.getHostname %)) tasks-assigned))
        scheduled (set (keys tid-to-hostname))]
    (if (> (count scheduled) 0)
      (do
        ; Create an instance as if job was running
        (doall (map #(create-dummy-instance conn [:job/uuid (:job/uuid (get tid-to-job (key %)))] :instance-status :instance.status/running
                                            :task-id (key %) :hostname (val %)) tid-to-hostname))
        ; Tell fenzo the job was scheduled
        (doall (map #(.call (.getTaskAssigner scheduler) (get tid-to-task (key %)) (val %)) tid-to-hostname))
        ; Return
        {:scheduled scheduled :result result})
      {:result result})))

(deftest test-sort-jobs-by-dru-pool
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

    (testing "sort-jobs-by-dru default share for everyone"
      (let [_ (share/set-share! conn "default" nil
                                "limits for new cluster"
                                :mem 10.0 :cpus 10.0)
            db (d/db conn)]
        (is (= [j2 j3 j6 j4 j8] (map :db/id (get (sched/sort-jobs-by-dru-pool db) "no-pool"))))))

    (testing "sort-jobs-by-dru one user has non-default share"
      (let [_ (share/set-share! conn "default" nil "limits for new cluster" :mem 10.0 :cpus 10.0)
            _ (share/set-share! conn "sunil" nil "needs more resources" :mem 100.0 :cpus 100.0)
            db (d/db conn)]
        (is (= [j8 j2 j3 j6 j4] (map :db/id (get (sched/sort-jobs-by-dru-pool db) "no-pool")))))))

  (testing "test-sort-jobs-by-dru:normal-jobs"
    (let [uri "datomic:mem://test-sort-jobs-by-dru-normal-jobs"
          conn (restore-fresh-database! uri)
          j1n (create-dummy-job conn :user "u1" :job-state :job.state/waiting :memory 1000 :ncpus 1.0)
          j2n (create-dummy-job conn :user "u1" :job-state :job.state/waiting :memory 1000 :ncpus 1.0 :priority 90)
          j3n (create-dummy-job conn :user "u2" :job-state :job.state/waiting :memory 1500 :ncpus 1.0)
          j4n (create-dummy-job conn :user "u2" :job-state :job.state/waiting :memory 1500 :ncpus 1.0 :priority 30)
          test-db (d/db conn)]
      (is (= [j2n j3n j1n j4n] (map :db/id (get (sched/sort-jobs-by-dru-pool test-db) "no-pool"))))
      (is (empty? (get (sched/sort-jobs-by-dru-pool test-db) "gpu")))))

  (testing "test-sort-jobs-by-dru:gpu-jobs"
    (let [uri "datomic:mem://test-sort-jobs-by-dru-gpu-jobs"
          conn (restore-fresh-database! uri)
          _ (create-pool conn "gpu" :dru-mode :pool.dru-mode/gpu)
          j1g (create-dummy-job conn :user "u1" :job-state :job.state/waiting :memory 1000 :ncpus 1.0 :gpus 10.0 :pool "gpu")
          j2g (create-dummy-job conn :user "u1" :job-state :job.state/waiting :memory 1000 :ncpus 1.0 :gpus 25.0 :pool "gpu" :priority 90)
          j3g (create-dummy-job conn :user "u2" :job-state :job.state/waiting :memory 1500 :ncpus 1.0 :gpus 20.0 :pool "gpu")
          j4g (create-dummy-job conn :user "u2" :job-state :job.state/waiting :memory 1500 :ncpus 1.0 :gpus 10.0 :pool "gpu" :priority 30)
          test-db (d/db conn)]
      (is (empty? (get (sched/sort-jobs-by-dru-pool test-db) "normal")))
      (is (= [j3g j2g j4g j1g] (map :db/id (get (sched/sort-jobs-by-dru-pool test-db) "gpu"))))))

  (testing "test-sort-jobs-by-dru:mixed-jobs"
    (let [uri "datomic:mem://test-sort-jobs-by-dru-mixed-jobs"
          conn (restore-fresh-database! uri)
          _ (create-pool conn "normal" :dru-mode :pool.dru-mode/default)
          _ (create-pool conn "gpu" :dru-mode :pool.dru-mode/gpu)
          j1n (create-dummy-job conn :user "u1" :job-state :job.state/waiting :memory 1000 :ncpus 1.0 :pool "normal")
          j2n (create-dummy-job conn :user "u1" :job-state :job.state/waiting :memory 1000 :ncpus 1.0 :pool "normal" :priority 90)
          j3n (create-dummy-job conn :user "u2" :job-state :job.state/waiting :memory 1500 :ncpus 1.0 :pool "normal")
          j4n (create-dummy-job conn :user "u2" :job-state :job.state/waiting :memory 1500 :ncpus 1.0 :pool "normal" :priority 30)
          j1g (create-dummy-job conn :user "u1" :job-state :job.state/waiting :memory 1000 :ncpus 1.0 :pool "gpu" :gpus 10.0)
          j2g (create-dummy-job conn :user "u1" :job-state :job.state/waiting :memory 1000 :ncpus 1.0 :pool "gpu" :gpus 25.0 :priority 90)
          j3g (create-dummy-job conn :user "u2" :job-state :job.state/waiting :memory 1500 :ncpus 1.0 :pool "gpu" :gpus 20.0)
          j4g (create-dummy-job conn :user "u2" :job-state :job.state/waiting :memory 1500 :ncpus 1.0 :pool "gpu" :gpus 10.0 :priority 30)
          test-db (d/db conn)]
      (is (= [j2n j3n j1n j4n] (map :db/id (get (sched/sort-jobs-by-dru-pool test-db) "normal"))))
      (is (= [j3g j2g j4g j1g] (map :db/id (get (sched/sort-jobs-by-dru-pool test-db) "gpu")))))))

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
  (cook.test.testutil/flush-caches!)
  (let [resources (util/job-ent->resources (d/entity (db c) j1))]
    (is (= 1.0 (:cpus resources)))
    (is (= 1000.0 (:mem resources)))))

(deftest test-match-offer-to-schedule
  (let [schedule (map #(d/entity (db c) %) [j1 j2 j3 j4]) ; all 1gb 1 cpu
        offer-maker (fn [cpus mem]
                      [{:resources [{:name "cpus" :type :value-scalar :scalar cpus}
                                    {:name "mem" :type :value-scalar :scalar mem}]
                        :id {:value (str "id-" (UUID/randomUUID))}
                        :slave-id {:value (str "slave-" (UUID/randomUUID))}
                        :hostname (str "host-" (UUID/randomUUID))}])
        framework-id (str "framework-id-" (UUID/randomUUID))
        fenzo-maker #(sched/make-fenzo-scheduler nil 100000 nil 1)] ; The params are for offer declining, which should never happen
    (testing "Consume no schedule cases"
      (are [schedule offers] (= [] (:matches (sched/match-offer-to-schedule (db c) (fenzo-maker) schedule offers (atom {}))))
                             [] (offer-maker 0 0)
                             [] (offer-maker 2 2000)
                             schedule (offer-maker 0 0)
                             schedule (offer-maker 0.5 100)
                             schedule (offer-maker 0.5 1000)
                             schedule (offer-maker 1 500)))
    (testing "Consume Partial schedule cases"
      ;; We're looking for one task to get assigned
      (are [offers] (= 1 (count (mapcat :tasks
                                        (:matches (sched/match-offer-to-schedule
                                                    (db c) (fenzo-maker) schedule offers (atom {}))))))
                    (offer-maker 1 1000)
                    (offer-maker 1.5 1500)))
    (testing "Consume full schedule cases"
      ;; We're looking for the entire schedule to get assigned
      (are [offers] (= (count schedule)
                       (count (mapcat :tasks
                                      (:matches (sched/match-offer-to-schedule
                                                  (db c) (fenzo-maker) schedule offers (atom {}))))))
                    (offer-maker 4 4000)
                    (offer-maker 5 5000)))))

(deftest test-match-offer-to-schedule-ordering
  (let [datomic-uri "datomic:mem://test-match-offer-to-schedule-ordering"
        conn (restore-fresh-database! datomic-uri)
        conflict-host "test-host"

        offer-maker (fn [cpus hostname]
                      (make-mesos-offer (make-uuid) (make-uuid) (make-uuid) hostname :cpus cpus :mem 200000.0))
        constraint-group (create-dummy-group conn :host-placement
                                             {:host-placement/type :host-placement.type/balanced
                                              :host-placement/parameters {:host-placement.balanced/attribute "HOSTNAME"
                                                                          :host-placement.balanced/minimum 10}})
        conflicting-job-id (create-dummy-job conn :ncpus 1.0 :memory 1.0 :name "conflict"
                                             :group constraint-group)
        low-priority-ids (doall (repeatedly 8
                                            #(create-dummy-job conn :ncpus 1.0
                                                               :memory 1000.0
                                                               :name "low-priority"
                                                               :priority 1
                                                               :job-state :job.state/waiting)))
        high-priority-ids (doall (repeatedly 1
                                             #(create-dummy-job conn :ncpus 1.0
                                                                :memory 1000.0
                                                                :name "high-priority"
                                                                :priority 100
                                                                :job-state :job.state/waiting)))
        framework-id (str "framework-id-" (java.util.UUID/randomUUID))
        fenzo (make-dummy-scheduler)
        ; Schedule conflicting
        _ (schedule-and-run-jobs conn framework-id fenzo [(make-vm-offer (make-uuid)
                                                                         conflict-host
                                                                         (make-uuid))] [conflicting-job-id])
        low-priority (map #(d/entity (d/db conn) %) low-priority-ids)
        high-priority (map #(d/entity (d/db conn) %) high-priority-ids)
        considerable (concat high-priority low-priority)]
    (testing "Scheduling order respected?"
      (let [schedule (sched/match-offer-to-schedule (d/db conn) fenzo considerable [(offer-maker 1.0 "empty_host")] (atom {}))]
        (is (= {"empty_host" ["high-priority"]}
               (->> schedule
                    :matches
                    (map :tasks)
                    (mapcat seq)
                    (map (juxt #(.getHostname %) #(vector (:job/name (:job (.getRequest %))))))
                    (map (partial apply hash-map))
                    (cons concat)
                    (apply merge-with))))))))

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
  (let [uri "datomic:mem://test-lingering-tasks"
        conn (restore-fresh-database! uri)
        now (tc/from-date #inst "2015-01-05T00:00:30")
        next-month (t/plus now (t/months 1))
        next-year (t/plus now (t/years 1))
        long-timeout (-> 64 t/days t/in-millis)
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

        job-id-3 (create-dummy-job conn
                                   :user "tsram"
                                   :job-state :job.state/running
                                   ; this timeout is equal to the "now" value
                                   :max-runtime 30000)
        instance-id-3 (create-dummy-instance conn job-id-3
                                             :start-time #inst "2015-01-05")

        job-id-4 (create-dummy-job conn
                                   :user "tsram"
                                   :job-state :job.state/running
                                   :max-runtime 10000)
        instance-id-4 (create-dummy-instance conn job-id-4
                                             :start-time #inst "2015-01-05")

        job-id-5 (create-dummy-job conn
                                   :user "tsram"
                                   :job-state :job.state/running
                                   ; timeout value exceeds Integer/MAX_VALUE millis
                                   :max-runtime long-timeout)
        instance-id-5 (create-dummy-instance conn job-id-5
                                             :start-time #inst "2015-01-01")

        test-db (d/db conn)
        task-id-2 (-> (d/entity test-db instance-id-2) :instance/task-id)
        task-id-4 (-> (d/entity test-db instance-id-4) :instance/task-id)
        task-id-5 (-> (d/entity test-db instance-id-5) :instance/task-id)]
    (is (= #{task-id-2 task-id-4} (set (sched/get-lingering-tasks test-db now 120 120))))
    (is (not (contains? (set (sched/get-lingering-tasks test-db next-month 1e4 1e4)) task-id-5)))
    (is (contains? (set (sched/get-lingering-tasks test-db next-year 1e5 1e5)) task-id-5))))

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
        ;; a job which follows all constraints
        job-id (create-dummy-job conn :user "tsram"
                                 :job-state :job.state/waiting
                                 :memory (* 1024 (- (:memory-gb constraints) 2.0))
                                 :ncpus (- (:cpus constraints) 1.0))
        test-db (d/db conn)
        job-entity (d/entity test-db job-id)
        offensive-jobs-ch (sched/make-offensive-job-stifler conn)
        offensive-job-filter (partial sched/filter-offensive-jobs constraints offensive-jobs-ch)]
    (is (= {"no-pool" (list (util/job-ent->map job-entity))}
           (sched/rank-jobs test-db offensive-job-filter)))))

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
  (let [mesos-status (mtypes/map->TaskStatus {:task-id #mesomatic.types.TaskID{:value "a07708d8-7ab6-404d-b136-a3e2cb2567e3"},
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

(deftest test-unique-host-placement-constraint
  (let [uri "datomic:mem://test-unique-host-placement-constraint"
        conn (restore-fresh-database! uri)
        framework-id #mesomatic.types.FrameworkID{:value "my-original-framework-id"}]
    (testing "conflicting jobs, different scheduling cycles"
      (let [scheduler (make-dummy-scheduler)
            shared-host "test-host"
            ; Group jobs, setting unique host-placement constraint
            group-id (create-dummy-group conn :host-placement {:host-placement/type :host-placement.type/unique})
            conflicted-job-id (create-dummy-job conn :group group-id)
            conflicting-job-id (create-dummy-job conn :group group-id)
            make-offers #(vector (make-vm-offer framework-id shared-host (make-uuid)))
            group (d/entity (d/db conn) group-id)
            ; Schedule first job
            scheduled-tasks (schedule-and-run-jobs conn framework-id scheduler (make-offers) [conflicting-job-id])
            _ (is (= 1 (count (:scheduled scheduled-tasks))))
            conflicting-task-id (first (:scheduled scheduled-tasks))
            ; Try to schedule conflicted job, but fail
            failures (-> (schedule-and-run-jobs conn framework-id scheduler (make-offers) [conflicted-job-id])
                         :result
                         .getFailures)
            task-results (-> failures
                             vals
                             first)
            fail-reason (-> task-results
                            first
                            .getConstraintFailure
                            .getReason)]
        (is (= 1 (count failures)))
        (is (= 1 (count task-results)))
        (is (= fail-reason (format "The hostname %s is being used by other instances in group %s"
                                   shared-host (:group/uuid group))))))
    (testing "conflicting jobs, same scheduling cycle"
      (let [scheduler (make-dummy-scheduler)
            shared-host "test-host"
            ; Group jobs, setting unique host-placement constraint
            group-id (create-dummy-group conn :host-placement {:host-placement/type :host-placement.type/unique})
            conflicted-job-id (create-dummy-job conn :group group-id)
            conflicting-job-id (create-dummy-job conn :group group-id)
            make-offers #(vector (make-vm-offer framework-id shared-host (make-uuid)))
            group (d/entity (d/db conn) group-id)
            ; Schedule first job
            result (schedule-and-run-jobs conn framework-id scheduler (make-offers) [conflicting-job-id
                                                                                     conflicted-job-id])
            _ (is (= 1 (count (:scheduled result))))
            conflicting-task-id (-> result :scheduled first)
            ; Try to schedule conflicted job, but fail
            failures (-> result :result .getFailures)
            task-results (-> failures
                             vals
                             first)
            fail-reason (-> task-results
                            first
                            .getConstraintFailure
                            .getReason)]
        (is (= 1 (count failures)))
        (is (= 1 (count task-results)))
        (is (= fail-reason (format "The hostname %s is being used by other instances in group %s"
                                   shared-host (:group/uuid group))))))
    (testing "non conflicting jobs"
      (let [scheduler (make-dummy-scheduler)
            shared-host "test-host"
            make-offers #(vector (make-vm-offer framework-id shared-host (make-uuid)))
            isolated-job-id1 (create-dummy-job conn)
            isolated-job-id2 (create-dummy-job conn)]
        (is (= 1 (count (:scheduled (schedule-and-run-jobs conn framework-id scheduler (make-offers) [isolated-job-id1])))))
        (is (= 1 (count (:scheduled (schedule-and-run-jobs conn framework-id scheduler (make-offers) [isolated-job-id2])))))))))


(deftest test-balanced-host-placement-constraint
  (let [uri "datomic:mem://test-balanced-host-placement-constraint"
        conn (restore-fresh-database! uri)
        framework-id #mesomatic.types.FrameworkID{:value "my-original-framework-id"}]
    (testing "schedule 9 jobs with hp-type balanced on 3 hosts, each host should get 3 jobs"
      (let [scheduler (make-dummy-scheduler)
            hostnames ["straw" "sticks" "bricks"]
            make-offers (fn [] (mapv #(make-vm-offer framework-id % (make-uuid)) hostnames))
            ; Group jobs, setting balanced host-placement constraint
            group-id (create-dummy-group conn
                                         :host-placement {:host-placement/type :host-placement.type/balanced
                                                          :host-placement/parameters {:host-placement.balanced/attribute "HOSTNAME"
                                                                                      :host-placement.balanced/minimum 3}})
            job-ids (doall (repeatedly 9 #(create-dummy-job conn :group group-id)))]
        (is (= {"straw" 3 "sticks" 3 "bricks" 3}
               (->> job-ids
                    (schedule-and-run-jobs conn framework-id scheduler (make-offers))
                    :result
                    .getResultMap
                    (pc/map-vals #(count (.getTasksAssigned %))))))))
    (testing "schedule 9 jobs with no placement constraints on 3 hosts, assignment not balanced"
      (let [scheduler (make-dummy-scheduler)
            hostnames ["straw" "sticks" "bricks"]
            make-offers (fn [] (mapv #(make-vm-offer framework-id % (make-uuid)) hostnames))
            job-ids (doall (repeatedly 9 #(create-dummy-job conn)))]
        (is (not (= (list 3) (->> job-ids
                                  (schedule-and-run-jobs conn framework-id scheduler (make-offers))
                                  :result
                                  .getResultMap
                                  vals
                                  (map #(count (.getTasksAssigned %)))
                                  distinct))))))))

(deftest test-attr-equals-host-placement-constraint
  (let [uri "datomic:mem://test-attr-equals-host-placement-constraint"
        conn (restore-fresh-database! uri)
        framework-id #mesomatic.types.FrameworkID{:value "my-original-framework-id"}
        make-hostname #(str (java.util.UUID/randomUUID))
        attr-name "az"
        attr-val "east"
        make-attr-offer (fn [cpus]
                          (make-vm-offer framework-id (make-hostname) (make-uuid)
                                         :cpus cpus :attrs {attr-name attr-val}))
        ; Each non-attr offer can take only one job
        make-non-attr-offers (fn [n]
                               (into [] (repeatedly n #(make-vm-offer framework-id (make-hostname) (make-uuid)
                                                                      :cpus 1.0 :attrs {attr-name "west"}))))]
    (testing "Create group, schedule one job unto VM, then all subsequent jobs must have same attr as the VM."
      (let [scheduler (make-dummy-scheduler)
            group-id (create-dummy-group conn :host-placement
                                         {:host-placement/type :host-placement.type/attribute-equals
                                          :host-placement/parameters {:host-placement.attribute-equals/attribute attr-name}})
            first-job (create-dummy-job conn :group group-id)
            other-jobs (doall (repeatedly 20 #(create-dummy-job conn :ncpus 1.0 :group group-id)))
            ; Group jobs, setting balanced host-placement constraint
            ; Schedule the first job
            _ (is (= 1 (->> (schedule-and-run-jobs conn framework-id scheduler [(make-attr-offer 1.0)] [first-job])
                            :scheduled
                            count)))
            batch-result (->> (schedule-and-run-jobs conn framework-id scheduler
                                                     (conj (make-non-attr-offers 20) (make-attr-offer 5.0)) other-jobs)
                              :result)]
        (testing "Other jobs all pile up on attr-offer."
          (is (= (list attr-val)
                 (->> batch-result
                      .getResultMap
                      vals
                      (filter #(> (count (.getTasksAssigned %)) 0))
                      (mapcat #(.getLeasesUsed %))
                      (map #(.getAttributeMap %))
                      (map #(get % attr-name))
                      distinct))))
        (testing "attr offer only fits five jobs."
          (is (= 5 (->> batch-result
                        .getResultMap
                        vals
                        (reduce #(+ %1 (count (.getTasksAssigned %2))) 0)))))
        (testing "Other 15 jobs are unscheduled."
          (is (= 15 (->> batch-result
                         .getFailures
                         count))))))
    (testing "Jobs use any vm freely when forced and no attr-equals constraint is given"
      (let [scheduler (make-dummy-scheduler)
            first-job (create-dummy-job conn)
            other-jobs (doall (repeatedly 20 #(create-dummy-job conn)))
            _ (is (= 1 (->> (schedule-and-run-jobs conn framework-id scheduler [(make-attr-offer 2.0)] [first-job])
                            :scheduled
                            count)))]
        ; Need to use all offers to fit all 20 other-jobs
        (is (= 20 (->> (schedule-and-run-jobs conn framework-id scheduler
                                              (conj (make-non-attr-offers 15) (make-attr-offer 5.0)) other-jobs)
                       :result
                       .getResultMap
                       vals
                       (reduce #(+ %1 (count (.getTasksAssigned %2))) 0))))))))

(deftest ^:benchmark stress-test-constraint
  (let [framework-id #mesomatic.types.FrameworkID{:value "my-original-framework-id"}
        uri "datomic:mem://stress-test-constraint"
        conn (restore-fresh-database! uri)
        scheduler (make-dummy-scheduler)
        hosts (map (fn [x] (str "test-host-" x)) (range 10))
        make-offers (fn [] (map #(make-vm-offer framework-id % (make-uuid)) hosts))
        group-id (create-dummy-group conn :host-placement {:host-placement/type :host-placement.type/unique})
                                        ;        group-id (create-dummy-group conn)
        jobs (doall (take 200 (repeatedly (fn [] (create-dummy-job conn :group group-id)))))
        group (d/entity (d/db conn) group-id)]
    (println  "============ match offers with group constraints timing ============")
    (crit/bench (schedule-and-run-jobs conn framework-id scheduler (make-offers) jobs))))

(deftest test-gpu-share-prioritization
  (let [uri "datomic:mem://test-gpu-shares"
        conn (restore-fresh-database! uri)
        pool-name "gpu"
        _ (create-pool conn pool-name :dru-mode :pool.dru-mode/gpu)
        ljin-1 (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 1.0 :pool pool-name)
        ljin-2 (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 1.0 :pool pool-name)
        ljin-3 (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 1.0 :pool pool-name)
        ljin-4 (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 1.0 :pool pool-name)
        wzhao-1 (create-dummy-job conn :user "wzhao" :ncpus 5.0 :memory 5.0 :gpus 1.0 :pool pool-name)
        wzhao-2 (create-dummy-job conn :user "wzhao" :ncpus 5.0 :memory 5.0 :gpus 1.0 :pool pool-name)
        ; Update ljin-1 to running
        inst (create-dummy-instance conn ljin-1 :instance-status :instance.status/unknown)
        _ @(d/transact conn [[:instance/update-state inst :instance.status/running [:reason/name :unknown]]])
        _ (share/set-share! conn "default" pool-name
                            "limits for new cluster"
                            :cpus 1.0 :mem 2.0 :gpus 1.0)]
    (testing "one user has double gpu share"
      (let [_ (share/set-share! conn "ljin" pool-name
                                "Needs some GPUs"
                                :gpus 2.0)
            db (d/db conn)]
        (is (= [ljin-2 wzhao-1 ljin-3 ljin-4 wzhao-2] (map :db/id (get (sched/sort-jobs-by-dru-pool db) pool-name))))))
    (testing "one user has single gpu share"
      (let [_ (share/set-share! conn "ljin" pool-name
                                "Doesn't need lots of gpus"
                                :gpus 1.0)
            db (d/db conn)]
        (is (= [wzhao-1 wzhao-2 ljin-2 ljin-3 ljin-4] (map :db/id (get (sched/sort-jobs-by-dru-pool db) pool-name))))))))

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
        fenzo (sched/make-fenzo-scheduler driver-atom 1500 nil 0.8)
        make-dummy-status-update (fn [task-id reason state & {:keys [progress] :or {progress nil}}]
                                   (let [task {:task-id {:value task-id}
                                               :reason reason
                                               :state state}]
                                     task))
        synced-agents-atom (atom [])
        sync-agent-sandboxes-fn (fn sync-agent-sandboxes-fn [hostname _]
                                  (swap! synced-agents-atom conj hostname))]

    (testing "Mesos task death"
      (let [job-id (create-dummy-job conn :user "tsram" :job-state :job.state/running)
            task-id "task1"
            instance-id (create-dummy-instance conn job-id
                                               :instance-status :instance.status/running
                                               :task-id task-id)]
        ; Wait for async database transaction inside handle-status-update
        (->> (make-dummy-status-update task-id :reason-gc-error :task-killed)
             (sched/handle-status-update conn driver (constantly fenzo) sync-agent-sandboxes-fn)
             async/<!!)

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
                          (db conn) instance-id))))
        (let [get-end-time (fn [] (ffirst (q '[:find ?end-time
                                               :in $ ?i
                                               :where
                                               [?i :instance/end-time ?end-time]]
                                             (db conn) instance-id)))
              original-end-time (get-end-time)]
          (Thread/sleep 100)
          (->> (make-dummy-status-update task-id :reason-gc-error :task-killed)
               (sched/handle-status-update conn driver (constantly fenzo) sync-agent-sandboxes-fn)
               async/<!!)
          (is (= original-end-time (get-end-time))))))

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
        (->> (make-dummy-status-update task-id :mesos-slave-restarted :task-killed)
             (sched/handle-status-update conn driver (constantly fenzo) sync-agent-sandboxes-fn)
             async/<!!)
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
                          (db conn) job-id))))))

    (testing "Tasks of completed jobs are killed"
      (let [job-id (create-dummy-job conn
                                     :user "tsram"
                                     :job-state :job.state/completed
                                     :retry-count 3)
            task-id-a "taska"
            task-id-b "taskb"]
        (create-dummy-instance conn job-id
                               :hostname "www.test-host.com"
                               :instance-status :instance.status/running
                               :task-id task-id-a
                               :reason :unknown)
        (create-dummy-instance conn job-id
                               :instance-status :instance.status/success
                               :task-id task-id-b
                               :reason :unknown)
        (reset! synced-agents-atom [])
        (->> (make-dummy-status-update task-id-a :mesos-slave-restarted :task-running)
             (sched/handle-status-update conn driver (constantly fenzo) sync-agent-sandboxes-fn)
             async/<!!)
        (is (true? (contains? @tasks-killed task-id-a)))
        (is (= ["www.test-host.com"] @synced-agents-atom))))

    (testing "instance persists mesos-start-time when task is first known to be starting or running"
      (let [job-id (create-dummy-job conn
                                     :user "mforsyth"
                                     :job-state :job.state/running
                                     :retry-count 3)
            task-id "task-mesos-start-time"
            mesos-start-time (fn [] (-> conn
                                        d/db
                                        (d/entity [:instance/task-id task-id])
                                        :instance/mesos-start-time))]
        (create-dummy-instance conn job-id
                               :hostname "www.test-host.com"
                               :instance-status :instance.status/unknown
                               :reason :unknown
                               :task-id task-id)
        (is (nil? (mesos-start-time)))
        (->> (make-dummy-status-update task-id :unknown :task-staging)
             (sched/handle-status-update conn driver (constantly fenzo) sync-agent-sandboxes-fn)
             async/<!!)
        (is (nil? (mesos-start-time)))
        (reset! synced-agents-atom [])
        (->> (make-dummy-status-update task-id :unknown :task-running)
             (sched/handle-status-update conn driver (constantly fenzo) sync-agent-sandboxes-fn)
             async/<!!)
        (is (= ["www.test-host.com"] @synced-agents-atom))
        (is (not (nil? (mesos-start-time))))
        (let [first-observed-start-time (.getTime (mesos-start-time))]
          (is (not (nil? first-observed-start-time)))
          (->> (make-dummy-status-update task-id :unknown :task-running)
               (sched/handle-status-update conn driver (constantly fenzo) sync-agent-sandboxes-fn)
               async/<!!)
          (is (= first-observed-start-time (.getTime (mesos-start-time)))))))))

(deftest test-handle-framework-message
  (let [uri "datomic:mem://test-handle-framework-message"
        conn (restore-fresh-database! uri)]

    (letfn [(make-message [message]
              (-> message walk/stringify-keys))
            (query-instance-field [instance-id field]
              (ffirst (q '[:find ?value
                           :in $ ?i ?field
                           :where
                           [?i ?field ?value]
                           [?i :instance/task-id ?task-id]]
                         (db conn) instance-id field)))
            (handle-progress-message-factory [progress-aggregator-promise]
              (fn handle-progress-message [progress-message-map]
                (deliver progress-aggregator-promise progress-message-map)))]

      (testing "missing task-id in message"
        (let [task-id (str (UUID/randomUUID))]
          (let [progress-aggregator-promise (promise)
                handle-progress-message (handle-progress-message-factory progress-aggregator-promise)
                handle-exit-code (constantly true)
                handlers {:handle-exit-code handle-exit-code :handle-progress-message handle-progress-message}
                message (make-message {:dummy-data task-id})]
            (is (nil? (sched/handle-framework-message conn handlers message)))
            (is (nil? (deref progress-aggregator-promise 1000 nil))))))

      (testing "no transactions"
        (let [task-id (str (UUID/randomUUID))]
          (let [progress-aggregator-promise (promise)
                handle-progress-message (handle-progress-message-factory progress-aggregator-promise)
                handle-exit-code (constantly true)
                handlers {:handle-exit-code handle-exit-code :handle-progress-message handle-progress-message}
                message (make-message {:task-id task-id})]
            (is (nil? (sched/handle-framework-message conn handlers message)))
            (is (nil? (deref progress-aggregator-promise 1000 nil))))))

      (testing "progress-message update"
        (let [job-id (create-dummy-job conn :user "test-user" :job-state :job.state/running)
              task-id (str (UUID/randomUUID))
              instance-id (create-dummy-instance conn job-id :instance-status :instance.status/running :task-id task-id)]
          (let [progress-aggregator-promise (promise)
                handle-progress-message (handle-progress-message-factory progress-aggregator-promise)
                handle-exit-code (constantly true)
                handlers {:handle-exit-code handle-exit-code :handle-progress-message handle-progress-message}
                progress-message "Almost complete..."
                message (make-message {:task-id task-id :progress-message progress-message})]
            ;; no asynchronous transaction should be created
            (is (nil? (sched/handle-framework-message conn handlers message)))
            (is (nil? (query-instance-field instance-id :instance/progress-message)))
            (is (= {:instance-id instance-id :progress-message progress-message :progress-percent nil :progress-sequence nil}
                   (deref progress-aggregator-promise 1000 nil))))))

      (testing "progress update"
        (let [job-id (create-dummy-job conn :user "test-user" :job-state :job.state/running)
              task-id (str (UUID/randomUUID))
              instance-id (create-dummy-instance conn job-id :instance-status :instance.status/running :task-id task-id)]

          (let [progress-aggregator-promise (promise)
                handle-progress-message (handle-progress-message-factory progress-aggregator-promise)
                handle-exit-code (constantly true)
                handlers {:handle-exit-code handle-exit-code :handle-progress-message handle-progress-message}
                progress-percent 20
                progress-sequence 11
                message (make-message {:task-id task-id :progress-percent progress-percent :progress-sequence progress-sequence})]
            ;; no asynchronous transaction should be created
            (is (nil? (sched/handle-framework-message conn handlers message)))
            (is (= 0 (query-instance-field instance-id :instance/progress)))
            (is (nil? (query-instance-field instance-id :instance/progress-message)))
            (is (= {:instance-id instance-id
                    :progress-message nil
                    :progress-percent progress-percent
                    :progress-sequence progress-sequence}
                   (deref progress-aggregator-promise 1000 nil))))

          (let [progress-aggregator-promise (promise)
                handle-progress-message (handle-progress-message-factory progress-aggregator-promise)
                handle-exit-code (constantly true)
                handlers {:handle-exit-code handle-exit-code :handle-progress-message handle-progress-message}
                progress-percent 50
                progress-sequence 19
                message (make-message {:task-id task-id :progress-percent progress-percent :progress-sequence progress-sequence})]
            ;; no asynchronous transaction should be created
            (is (nil? (sched/handle-framework-message conn handlers message)))
            (is (= 0 (query-instance-field instance-id :instance/progress)))
            (is (nil? (query-instance-field instance-id :instance/progress-message)))
            (is (= {:instance-id instance-id
                    :progress-message nil
                    :progress-percent progress-percent
                    :progress-sequence progress-sequence}
                   (deref progress-aggregator-promise 1000 nil))))))

      (testing "exit-code update"
        (let [job-id (create-dummy-job conn :user "test-user" :job-state :job.state/running)
              task-id (str (UUID/randomUUID))
              instance-id (create-dummy-instance conn job-id :instance-status :instance.status/running :task-id task-id)]
          (let [progress-aggregator-promise (promise)
                handle-progress-message (handle-progress-message-factory progress-aggregator-promise)
                handle-exit-code (fn [task-id exit-code]
                                   @(d/transact conn [[:db/add [:instance/task-id task-id] :instance/exit-code exit-code]]))
                handlers {:handle-exit-code handle-exit-code :handle-progress-message handle-progress-message}
                exit-code 0
                message (make-message {:task-id task-id :exit-code exit-code})]
            (sched/handle-framework-message conn handlers message)
            (is (= exit-code (query-instance-field instance-id :instance/exit-code)))
            (is (nil? (deref progress-aggregator-promise 1000 nil))))))

      (testing "all fields update"
        (let [job-id (create-dummy-job conn :user "test-user" :job-state :job.state/running)
              task-id (str (UUID/randomUUID))
              instance-id (create-dummy-instance conn job-id :instance-status :instance.status/running :task-id task-id)]
          (let [progress-aggregator-promise (promise)
                handle-progress-message (handle-progress-message-factory progress-aggregator-promise)
                handle-exit-code (fn [task-id exit-code]
                                   @(d/transact conn [[:db/add [:instance/task-id task-id] :instance/exit-code exit-code]]))
                handlers {:handle-exit-code handle-exit-code :handle-progress-message handle-progress-message}
                exit-code 0
                progress-percent 90
                progress-message "Almost complete..."
                sandbox-directory "/sandbox/location/for/task"
                message (make-message {:task-id task-id
                                       :exit-code exit-code
                                       :progress-message progress-message
                                       :progress-percent progress-percent
                                       :sandbox-directory sandbox-directory})]
            (sched/handle-framework-message conn handlers message)
            (is (= exit-code (query-instance-field instance-id :instance/exit-code)))
            (is (nil? (query-instance-field instance-id :instance/sandbox-directory)))
            (is (= 0 (query-instance-field instance-id :instance/progress)))
            (is (nil? (query-instance-field instance-id :instance/progress-message)))
            (is (= {:instance-id instance-id
                    :progress-message progress-message
                    :progress-percent progress-percent
                    :progress-sequence nil}
                   (deref progress-aggregator-promise 1000 nil)))))))))

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
        match-trigger-chan (async/chan (async/sliding-buffer 5))
        mock-driver (reify msched/SchedulerDriver
                      (decline-offer [driver id]
                        (swap! declined-offer-ids-atom conj id)))
        offer-1 {:id {:value "foo"}}
        offer-2 {:id {:value "bar"}}
        offer-3 {:id {:value "baz"}}]
    (testing "offer chan overflow"
      (sched/receive-offers offers-chan match-trigger-chan mock-driver "no-pool" [offer-1])
      @(sched/receive-offers offers-chan match-trigger-chan mock-driver "no-pool" [offer-2])
      @(sched/receive-offers offers-chan match-trigger-chan mock-driver "no-pool" [offer-3])
      (is (= @declined-offer-ids-atom [(:id offer-2) (:id offer-3)]))
      (async/close! match-trigger-chan)
      (is (= (count (async/<!! (async/into [] match-trigger-chan))) 1)))))

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

(deftest test-pending-jobs->considerable-jobs
  (let [uri "datomic:mem://test-pending-jobs-considerable-jobs"
        conn (restore-fresh-database! uri)
        test-db (d/db conn)
        test-user (System/getProperty "user.name")
        group-ent-id (create-dummy-group conn)
        entity->map (fn [entity]
                      (util/job-ent->map entity (d/db conn)))
        job-1 (entity->map (d/entity test-db (create-dummy-job conn :group group-ent-id :ncpus 3 :memory 2048)))
        job-2 (entity->map (d/entity test-db (create-dummy-job conn :group group-ent-id :ncpus 13 :memory 1024)))
        job-3 (entity->map (d/entity test-db (create-dummy-job conn :group group-ent-id :ncpus 7 :memory 4096)))
        job-4 (entity->map (d/entity test-db (create-dummy-job conn :group group-ent-id :ncpus 11 :memory 1024)))
        job-5 (entity->map (d/entity test-db (create-dummy-job conn :group group-ent-id :ncpus 5 :memory 2048 :gpus 2)))
        job-6 (entity->map (d/entity test-db (create-dummy-job conn :group group-ent-id :ncpus 19 :memory 1024 :gpus 4)))
        non-gpu-jobs [job-1 job-2 job-3 job-4]
        gpu-jobs [job-5 job-6]]

    (testing "jobs inside usage quota"
      (let [user->usage {test-user {:count 1, :cpus 2, :mem 1024, :gpus 0}}
            user->quota {test-user {:count 10, :cpus 50, :mem 32768, :gpus 10}}
            num-considerable 5]
        (is (= non-gpu-jobs
               (sched/pending-jobs->considerable-jobs
                 (d/db conn) non-gpu-jobs user->quota user->usage num-considerable (atom {}) nil)))
        (is (= gpu-jobs
               (sched/pending-jobs->considerable-jobs
                 (d/db conn) gpu-jobs user->quota user->usage num-considerable (atom {}) nil)))))
    (testing "jobs inside usage quota limited by num-considerable of 3"
      (let [user->usage {test-user {:count 1, :cpus 2, :mem 1024, :gpus 0}}
            user->quota {test-user {:count 10, :cpus 50, :mem 32768, :gpus 10}}
            num-considerable 3]
        (is (= [job-1 job-2 job-3]
               (sched/pending-jobs->considerable-jobs
                 (d/db conn) non-gpu-jobs user->quota user->usage num-considerable (atom {}) nil)))
        (is (= gpu-jobs
               (sched/pending-jobs->considerable-jobs
                 (d/db conn) gpu-jobs user->quota user->usage num-considerable (atom {}) nil)))))
    (testing "jobs inside usage quota limited by num-considerable of 2"
      (let [user->usage {test-user {:count 1, :cpus 2, :mem 1024, :gpus 0}}
            user->quota {test-user {:count 10, :cpus 50, :mem 32768, :gpus 10}}
            num-considerable 2]
        (is (= [job-1 job-2]
               (sched/pending-jobs->considerable-jobs
                 (d/db conn) non-gpu-jobs user->quota user->usage num-considerable (atom {}) nil)))
        (is (= gpu-jobs
               (sched/pending-jobs->considerable-jobs
                 (d/db conn) gpu-jobs user->quota user->usage num-considerable (atom {}) nil)))))
    (testing "jobs inside usage quota limited by num-considerable of 1"
      (let [user->usage {test-user {:count 1, :cpus 2, :mem 1024, :gpus 0}}
            user->quota {test-user {:count 10, :cpus 50, :mem 32768, :gpus 10}}
            num-considerable 1]
        (is (= [job-1]
               (sched/pending-jobs->considerable-jobs
                 (d/db conn) non-gpu-jobs user->quota user->usage num-considerable (atom {}) nil)))
        (is (= [job-5]
               (sched/pending-jobs->considerable-jobs
                 (d/db conn) gpu-jobs user->quota user->usage num-considerable (atom {}) nil)))))
    (testing "jobs inside usage quota influenced by optimizer"
      (let [user->usage {test-user {:count 1, :cpus 2, :mem 1024, :gpus 0}}
            user->quota {test-user {:count 10, :cpus 50, :mem 32768, :gpus 10}}
            num-considerable 5
            pool-name->optimizer-suggested-job-ids-atom (atom {"cpu-pool" [(:job/uuid job-4) (:job/uuid job-2) (:job/uuid job-6)]
                                                              "gpu-pool" [(:job/uuid job-2) (:job/uuid job-6)]})]
        (is (= [job-4 job-2 job-1 job-3]
               (sched/pending-jobs->considerable-jobs
                 (d/db conn) non-gpu-jobs user->quota user->usage num-considerable
                 pool-name->optimizer-suggested-job-ids-atom "cpu-pool")))
        (is (= [job-6 job-5]
               (sched/pending-jobs->considerable-jobs
                 (d/db conn) gpu-jobs user->quota user->usage num-considerable
                 pool-name->optimizer-suggested-job-ids-atom "gpu-pool")))
        (is (= {"cpu-pool" [] "gpu-pool" []}
               @pool-name->optimizer-suggested-job-ids-atom))))
    (testing "jobs inside usage quota influenced by optimizer limited by num-considerable of 1"
      (let [user->usage {test-user {:count 1, :cpus 2, :mem 1024, :gpus 0}}
            user->quota {test-user {:count 10, :cpus 50, :mem 32768, :gpus 10}}
            num-considerable 1
            pool-name->optimizer-suggested-job-ids-atom (atom {"cpu-pool" [(:job/uuid job-4) (:job/uuid job-2) (:job/uuid job-6)]
                                                              "gpu-pool" [(:job/uuid job-2) (:job/uuid job-6)]})]
        (is (= [job-4]
               (sched/pending-jobs->considerable-jobs
                 (d/db conn) non-gpu-jobs user->quota user->usage num-considerable
                 pool-name->optimizer-suggested-job-ids-atom "cpu-pool")))
        (is (= [job-6]
               (sched/pending-jobs->considerable-jobs
                 (d/db conn) gpu-jobs user->quota user->usage num-considerable
                 pool-name->optimizer-suggested-job-ids-atom "gpu-pool")))
        (is (= {"cpu-pool" [] "gpu-pool" []}
               @pool-name->optimizer-suggested-job-ids-atom))))
    (testing "some jobs inside usage quota"
      (let [user->usage {test-user {:count 1, :cpus 2, :mem 1024, :gpus 0}}
            user->quota {test-user {:count 5, :cpus 10, :mem 4096, :gpus 10}}
            num-considerable 5]
        (is (= [job-1]
               (sched/pending-jobs->considerable-jobs
                 (d/db conn) non-gpu-jobs user->quota user->usage num-considerable (atom {}) nil)))
        (is (= [job-5]
               (sched/pending-jobs->considerable-jobs
                 (d/db conn) gpu-jobs user->quota user->usage num-considerable (atom {}) nil)))))
    (testing "some jobs inside usage quota - quota gpus not ignored"
      (let [user->usage {test-user {:count 1, :cpus 2, :mem 1024, :gpus 0}}
            user->quota {test-user {:count 5, :cpus 10, :mem 4096, :gpus 0}}
            num-considerable 5]
        (is (= [job-1]
               (sched/pending-jobs->considerable-jobs
                 (d/db conn) non-gpu-jobs user->quota user->usage num-considerable (atom {}) nil)))
        (is (= []
               (sched/pending-jobs->considerable-jobs
                 (d/db conn) gpu-jobs user->quota user->usage num-considerable (atom {}) nil)))))
    (testing "all jobs exceed quota"
      (let [user->usage {test-user {:count 1, :cpus 2, :mem 1024, :gpus 0}}
            user->quota {test-user {:count 5, :cpus 3, :mem 4096, :gpus 10}}
            num-considerable 5]
        (is (= []
               (sched/pending-jobs->considerable-jobs
                 (d/db conn) non-gpu-jobs user->quota user->usage num-considerable (atom {}) nil)))
        (is (= []
               (sched/pending-jobs->considerable-jobs
                 (d/db conn) gpu-jobs user->quota user->usage num-considerable (atom {}) nil)))))))

(deftest test-matches->job-uuids
  (let [create-task-result (fn [job-uuid _ _ gpus]
                             (-> (Mockito/when (.getRequest (Mockito/mock TaskAssignmentResult)))
                                 (.thenReturn (sched/make-task-request
                                                (Object.)
                                                {:job/uuid job-uuid
                                                 :job/resource (cond-> [{:resource/type :resource.type/mem, :resource/amount 1000.0}
                                                                        {:resource/type :resource.type/cpus, :resource/amount 1.0}]
                                                                       gpus (conj {:resource/type :resource.type/gpus, :resource/amount gpus}))}
                                                :task-id (str "task-id-" job-uuid)))
                                 (.getMock)))
        job-1 (create-task-result "job-1" 1 1024 nil)
        job-2 (create-task-result "job-2" 2 2048 nil)
        job-3 (create-task-result "job-3" 3 1024 1)
        job-4 (create-task-result "job-4" 4 1024 nil)
        job-5 (create-task-result "job-5" 5 2048 2)
        job-6 (create-task-result "job-6" 6 1024 3)
        job-7 (create-task-result "job-7" 7 1024 nil)]
    (is (= #{"job-3" "job-5" "job-6"}
           (sched/matches->job-uuids
             [{:tasks [job-3]}, {:tasks #{job-5}}, {:tasks [job-6]}] nil)))
    (is (= #{"job-1" "job-2" "job-4" "job-7"}
           (sched/matches->job-uuids
             [{:tasks [job-1 job-2]}, {:tasks #{job-4}}, {:tasks [job-7]}] nil)))
    (is (= #{"job-1" "job-2" "job-4" "job-7"}
           (sched/matches->job-uuids
             [{:tasks [job-1 job-2]}, {:tasks #{job-4}}, {:tasks #{}}, {:tasks [job-7]}] nil)))
    (is (= #{"job-3" "job-5" "job-6"}
           (sched/matches->job-uuids
             [{:tasks [job-3]}, {:tasks #{job-5}}, {:tasks #{job-6}}, {:tasks []}] nil)))
    (is (= #{}
           (sched/matches->job-uuids
             [{:tasks []}, {:tasks #{}}, {:tasks #{}}, {:tasks []}] nil)))))

(deftest test-remove-matched-jobs-from-pending-jobs
  (let [create-jobs-in-range (fn [start-inc end-exc]
                               (map (fn [id] {:job/uuid id}) (range start-inc end-exc)))]
    (testing "empty matched jobs"
      (let [pool->pending-jobs {:gpu (create-jobs-in-range 10 15)
                                :normal (create-jobs-in-range 1 10)}
            matched-job-uuids #{}
            expected-pool->pending-jobs pool->pending-jobs]
        (is (= expected-pool->pending-jobs
               (sched/remove-matched-jobs-from-pending-jobs pool->pending-jobs matched-job-uuids :gpu)))
        (is (= expected-pool->pending-jobs
               (sched/remove-matched-jobs-from-pending-jobs pool->pending-jobs matched-job-uuids :normal)))))

    (testing "unknown matched jobs"
      (let [pool->pending-jobs {:gpu (create-jobs-in-range 10 15)
                                :normal (create-jobs-in-range 1 10)}
            pool->matched-job-uuids {:gpu (set (range 30 35))
                                     :normal (set (range 20 25))}
            expected-pool->pending-jobs pool->pending-jobs]
        (is (= expected-pool->pending-jobs
               (sched/remove-matched-jobs-from-pending-jobs pool->pending-jobs (:gpu pool->matched-job-uuids) :gpu)))
        (is (= expected-pool->pending-jobs
               (sched/remove-matched-jobs-from-pending-jobs pool->pending-jobs (:normal pool->matched-job-uuids) :normal)))))

    (testing "non-empty matched normal jobs"
      (let [pool->pending-jobs {:gpu (create-jobs-in-range 10 15)
                                :normal (create-jobs-in-range 1 10)}
            pool->matched-job-uuids {:gpu #{}
                                     :normal (set (range 1 5))}
            expected-pool->pending-jobs {:gpu (create-jobs-in-range 10 15)
                                             :normal (create-jobs-in-range 5 10)}]
        (is (= (:gpu expected-pool->pending-jobs)
               (:gpu (sched/remove-matched-jobs-from-pending-jobs pool->pending-jobs (:gpu pool->matched-job-uuids) :gpu))))
        (is (= (:normal expected-pool->pending-jobs)
               (:normal (sched/remove-matched-jobs-from-pending-jobs pool->pending-jobs (:normal pool->matched-job-uuids) :normal))))))

    (testing "non-empty matched gpu jobs"
      (let [pool->pending-jobs {:gpu (create-jobs-in-range 10 15)
                                :normal (create-jobs-in-range 1 10)}
            pool->matched-job-uuids {:gpu (set (range 10 12))
                                     :normal #{}}
            expected-pool->pending-jobs {:gpu (create-jobs-in-range 12 15)
                                         :normal (create-jobs-in-range 1 10)}]
        (is (= (:gpu expected-pool->pending-jobs)
               (:gpu (sched/remove-matched-jobs-from-pending-jobs pool->pending-jobs (:gpu pool->matched-job-uuids) :gpu))))
        (is (= (:normal expected-pool->pending-jobs)
               (:normal (sched/remove-matched-jobs-from-pending-jobs pool->pending-jobs (:normal pool->matched-job-uuids) :normal))))))

    (testing "non-empty matched normal and gpu jobs"
      (let [pool->pending-jobs {:normal (create-jobs-in-range 1 10)
                                :gpu (create-jobs-in-range 10 15)}
            pool->matched-job-uuids {:gpu (set (range 10 12))
                                     :normal (set (range 5 10))}
            expected-pool->pending-jobs {:gpu (create-jobs-in-range 12 15)
                                         :normal (create-jobs-in-range 1 5)}]
        (is (= (:gpu expected-pool->pending-jobs)
               (:gpu (sched/remove-matched-jobs-from-pending-jobs pool->pending-jobs (:gpu pool->matched-job-uuids) :gpu))))
        (is (= (:normal expected-pool->pending-jobs)
               (:normal (sched/remove-matched-jobs-from-pending-jobs pool->pending-jobs (:normal pool->matched-job-uuids) :normal))))))))

(deftest test-handle-resource-offers
  (let [test-user (System/getProperty "user.name")
        uri "datomic:mem://test-handle-resource-offers"
        executor {:command "cook-executor"
                  :default-progress-regex-string "regex-string"
                  :log-level "INFO"
                  :max-message-length 512
                  :progress-sample-interval-ms 1000
                  :uri {:cache true
                        :executable true
                        :extract false
                        :value "file:///path/to/cook-executor"}}
        launched-offer-ids-atom (atom [])
        launched-job-ids-atom (atom [])
        driver (reify msched/SchedulerDriver
                 (launch-tasks! [_ offer-id tasks]
                   (swap! launched-offer-ids-atom conj (-> offer-id first :value))
                   (swap! launched-job-ids-atom concat (map (fn extract-job-id [task]
                                                              (let [task-name (:name task)
                                                                    suffix-start (str/index-of task-name (str "_" test-user "_"))]
                                                                (subs task-name 0 suffix-start)))
                                                            tasks))))
        offer-maker (fn [cpus mem gpus]
                      {:resources [{:name "cpus", :scalar cpus, :type :value-scalar, :role "cook"}
                                   {:name "mem", :scalar mem, :type :value-scalar, :role "cook"}
                                   {:name "gpus", :scalar gpus, :type :value-scalar, :role "cook"}]
                       :id {:value (str "id-" (UUID/randomUUID))}
                       :slave-id {:value (str "slave-" (UUID/randomUUID))}
                       :hostname (str "host-" (UUID/randomUUID))})
        framework-id #mesomatic.types.FrameworkID{:value "my-framework-id"}
        offers-chan (async/chan (async/buffer 10))
        offer-1 (offer-maker 10 2048 0)
        offer-2 (offer-maker 20 16384 0)
        offer-3 (offer-maker 30 8192 0)
        offer-4 (offer-maker 4 2048 0)
        offer-5 (offer-maker 4 1024 0)
        offer-6 (offer-maker 10 4096 10)
        offer-7 (offer-maker 20 4096 5)
        offer-8 (offer-maker 30 16384 1)
        offer-9 (offer-maker 100 200000 0)
        run-handle-resource-offers! (fn [num-considerable offers pool-name &
                                         {:keys [job-name->uuid
                                                 pool-name->optimizer-suggested-job-ids-atom
                                                 rebalancer-reservation-atom
                                                 user-quota user->usage]
                                          :or {job-name->uuid {}
                                               pool-name->optimizer-suggested-job-ids-atom (atom {})
                                               rebalancer-reservation-atom (atom {})}}]
                                      (reset! launched-offer-ids-atom [])
                                      (reset! launched-job-ids-atom [])
                                      (let [conn (restore-fresh-database! uri)
                                            test-db (d/db conn)
                                            driver-atom (atom nil)
                                            ^TaskScheduler fenzo (sched/make-fenzo-scheduler driver-atom 1500 nil 0.8)
                                            group-ent-id (create-dummy-group conn)
                                            get-uuid (fn [name] (get job-name->uuid name (d/squuid)))
                                            job-1 (d/entity test-db (create-dummy-job conn
                                                                                      :uuid (get-uuid "job-1")
                                                                                      :group group-ent-id
                                                                                      :name "job-1"
                                                                                      :ncpus 3
                                                                                      :memory 2048))
                                            job-2 (d/entity test-db (create-dummy-job conn
                                                                                      :uuid (get-uuid "job-2")
                                                                                      :group group-ent-id
                                                                                      :name "job-2"
                                                                                      :ncpus 13
                                                                                      :memory 1024))
                                            job-3 (d/entity test-db (create-dummy-job conn
                                                                                      :uuid (get-uuid "job-3")
                                                                                      :group group-ent-id
                                                                                      :name "job-3"
                                                                                      :ncpus 7
                                                                                      :memory 4096))
                                            job-4 (d/entity test-db (create-dummy-job conn
                                                                                      :uuid (get-uuid "job-4")
                                                                                      :group group-ent-id
                                                                                      :name "job-4"
                                                                                      :ncpus 11
                                                                                      :memory 1024))
                                            job-5 (d/entity test-db (create-dummy-job conn
                                                                                      :uuid (get-uuid "job-5")
                                                                                      :group group-ent-id
                                                                                      :name "job-5"
                                                                                      :ncpus 5
                                                                                      :memory 2048
                                                                                      :gpus 2))
                                            job-6 (d/entity test-db (create-dummy-job conn
                                                                                      :uuid (get-uuid "job-6")
                                                                                      :group group-ent-id
                                                                                      :name "job-6"
                                                                                      :ncpus 19
                                                                                      :memory 1024
                                                                                      :gpus 4))
                                            entity->map (fn [entity]
                                                          (util/job-ent->map entity (d/db conn)))
                                            pool->pending-jobs (->> {:normal [job-1 job-2 job-3 job-4] :gpu [job-5 job-6]}
                                                                    (pc/map-vals (partial map entity->map)))
                                            pool-name->pending-jobs-atom (atom pool->pending-jobs)
                                            user->usage (or user->usage {test-user {:count 1, :cpus 2, :mem 1024, :gpus 0}})
                                            user->quota (or user-quota {test-user {:count 10, :cpus 50, :mem 32768, :gpus 10}})
                                            mesos-run-as-user nil
                                            result (sched/handle-resource-offers!
                                                     conn driver fenzo framework-id pool-name->pending-jobs-atom mesos-run-as-user
                                                     user->usage user->quota num-considerable offers-chan offers
                                                     rebalancer-reservation-atom pool-name->optimizer-suggested-job-ids-atom
                                                     pool-name)]
                                        (async/>!! offers-chan :end-marker)
                                        result))]
    (with-redefs [cook.config/executor-config (constantly executor)]
      (testing "enough offers for all normal jobs"
        (let [num-considerable 10
              offers [offer-1 offer-2 offer-3]]
          (is (run-handle-resource-offers! num-considerable offers :normal))
          (is (= :end-marker (async/<!! offers-chan)))
          (is (= 3 (count @launched-offer-ids-atom)))
          (is (= 4 (count @launched-job-ids-atom)))
          (is (= #{"job-1" "job-2" "job-3" "job-4"} (set @launched-job-ids-atom)))))

      (testing "enough offers for all normal jobs, limited by num-considerable of 1"
        (let [num-considerable 1
              offers [offer-1 offer-2 offer-3]]
          (is (run-handle-resource-offers! num-considerable offers :normal))
          (is (= :end-marker (async/<!! offers-chan)))
          (is (= 1 (count @launched-offer-ids-atom)))
          (is (= 1 (count @launched-job-ids-atom)))
          (is (= #{"job-1"} (set @launched-job-ids-atom)))))

      (testing "enough offers for all normal jobs, limited by num-considerable of 2"
        (let [num-considerable 2
              offers [offer-1 offer-2 offer-3]]
          (is (run-handle-resource-offers! num-considerable offers :normal))
          (is (= :end-marker (async/<!! offers-chan)))
          (is (= 2 (count @launched-offer-ids-atom)))
          (is (= 2 (count @launched-job-ids-atom)))
          (is (= #{"job-1" "job-2"} (set @launched-job-ids-atom)))))

      (testing "enough offers for all normal jobs, limited by quota"
        (let [num-considerable 1
              offers [offer-1 offer-2 offer-3]
              user-quota {test-user {:count 5, :cpus 45, :mem 16384, :gpus 0}}]
          (is (run-handle-resource-offers! num-considerable offers :normal :user-quota user-quota))
          (is (= :end-marker (async/<!! offers-chan)))
          (is (= 1 (count @launched-offer-ids-atom)))
          (is (= 1 (count @launched-job-ids-atom)))
          (is (= #{"job-1"} (set @launched-job-ids-atom)))))

      (testing "enough offers for all normal jobs, limited by usage capacity"
        (let [num-considerable 1
              offers [offer-1 offer-2 offer-3]
              user->usage {test-user {:count 5, :cpus 5, :mem 16384, :gpus 0}}]
          (is (run-handle-resource-offers! num-considerable offers :normal :user->usage user->usage))
          (is (= :end-marker (async/<!! offers-chan)))
          (is (= 1 (count @launched-offer-ids-atom)))
          (is (= 1 (count @launched-job-ids-atom)))
          (is (= #{"job-1"} (set @launched-job-ids-atom)))))

      (testing "offer for single normal job"
        (let [num-considerable 10
              offers [offer-4]]
          (is (run-handle-resource-offers! num-considerable offers :normal))
          (is (= :end-marker (async/<!! offers-chan)))
          (is (= 1 (count @launched-offer-ids-atom)))
          (is (= 1 (count @launched-job-ids-atom)))
          (is (= #{"job-1"} (set @launched-job-ids-atom)))))

      (testing "offer for first three normal jobs"
        (let [num-considerable 10
              offers [offer-3]]
          (is (run-handle-resource-offers! num-considerable offers :normal))
          (is (= :end-marker (async/<!! offers-chan)))
          (is (= 1 (count @launched-offer-ids-atom)))
          (is (= 3 (count @launched-job-ids-atom)))
          (is (= #{"job-1" "job-2" "job-3"} (set @launched-job-ids-atom)))))

      (testing "offer not fit for any normal job"
        (let [num-considerable 10
              offers [offer-5]]
          (is (run-handle-resource-offers! num-considerable offers :normal))
          (is (= :end-marker (async/<!! offers-chan)))
          (is (zero? (count @launched-offer-ids-atom)))
          (is (empty? @launched-job-ids-atom))))

      (testing "offer fit but user has too little quota"
        (let [num-considerable 10
              offers [offer-1 offer-2 offer-3]
              user-quota {test-user {:count 5, :cpus 4, :mem 4096, :gpus 0}}]
          (is (run-handle-resource-offers! num-considerable offers :normal :user-quota user-quota))
          (is (= :end-marker (async/<!! offers-chan)))
          (is (zero? (count @launched-offer-ids-atom)))
          (is (empty? @launched-job-ids-atom))))

      (testing "offer fit but user has capacity usage"
        (let [num-considerable 10
              offers [offer-1 offer-2 offer-3]
              user->usage {test-user {:count 10, :cpus 50, :mem 32768, :gpus 10}}]
          (is (run-handle-resource-offers! num-considerable offers :normal :user->usage user->usage))
          (is (= :end-marker (async/<!! offers-chan)))
          (is (zero? (count @launched-offer-ids-atom)))
          (is (empty? @launched-job-ids-atom))))

      (testing "gpu offers for all gpu jobs"
        (let [num-considerable 10
              offers [offer-6 offer-7]]
          (is (run-handle-resource-offers! num-considerable offers :gpu))
          (is (= :end-marker (async/<!! offers-chan)))
          (is (= 2 (count @launched-offer-ids-atom)))
          (is (= 2 (count @launched-job-ids-atom)))
          (is (= #{"job-5" "job-6"} (set @launched-job-ids-atom)))))

      (testing "gpu offer for single gpu job"
        (let [num-considerable 10
              offers [offer-6]]
          (is (run-handle-resource-offers! num-considerable offers :gpu))
          (is (= :end-marker (async/<!! offers-chan)))
          (is (= 1 (count @launched-offer-ids-atom)))
          (is (= 1 (count @launched-job-ids-atom)))
          (is (= #{"job-5"} (set @launched-job-ids-atom)))))

      (testing "gpu offer matching no gpu job"
        (let [num-considerable 10
              offers [offer-8]]
          (is (run-handle-resource-offers! num-considerable offers :gpu))
          (is (= :end-marker (async/<!! offers-chan)))
          (is (zero? (count @launched-offer-ids-atom)))
          (is (empty? @launched-job-ids-atom))))

      (testing "will not launch jobs on reserved host"
        (let [num-considerable 10
              offers [offer-1]
              initial-reservation-state {:job-uuid->reserved-host {(UUID/randomUUID) (:hostname offer-1)}}
              rebalancer-reservation-atom (atom initial-reservation-state)]
          (is (run-handle-resource-offers! num-considerable offers :normal :rebalancer-reservation-atom rebalancer-reservation-atom))
          (is (= 0 (count @launched-job-ids-atom)))
          (is (= initial-reservation-state @rebalancer-reservation-atom))))

      (testing "only launches reserved jobs on reserved host"
        (let [num-considerable 10
              offers [offer-9] ; large enough to launch jobs 1, 2, 3, and 4
              job-1-uuid (d/squuid)
              job-2-uuid (d/squuid)
              initial-reservation-state {:job-uuid->reserved-host {job-1-uuid (:hostname offer-9)
                                                                   job-2-uuid (:hostname offer-9)}}
              rebalancer-reservation-atom (atom initial-reservation-state)]
          (is (run-handle-resource-offers! num-considerable offers :normal :rebalancer-reservation-atom rebalancer-reservation-atom
                                           :job-name->uuid {"job-1" job-1-uuid "job-2" job-2-uuid}))
          (is (= :end-marker (async/<!! offers-chan)))
          (is (= 2 (count @launched-job-ids-atom)))
          (is (= #{"job-1" "job-2"} (set @launched-job-ids-atom)))
          (is (= {:job-uuid->reserved-host {}
                  :launched-job-uuids #{job-1-uuid job-2-uuid}}
                 @rebalancer-reservation-atom)))))))


(deftest test-handle-resource-offers-with-data-locality
  (with-redefs [config/data-local-fitness-config (constantly {:data-locality-weight 0.95
                                                              :base-calculator BinPackingFitnessCalculators/cpuMemBinPacker})
                dl/job-uuid->dataset-maps-cache (util/new-cache)]
    (let [test-user (System/getProperty "user.name")
          uri "datomic:mem://test-handle-resource-offers"
          launched-tasks-atom (atom [])
          driver (reify msched/SchedulerDriver
                   (launch-tasks! [_ _ tasks]
                     (swap! launched-tasks-atom concat tasks)))
          offer-maker (fn [cpus mem gpus]
                        {:resources [{:name "cpus", :scalar cpus, :type :value-scalar, :role "cook"}
                                     {:name "mem", :scalar mem, :type :value-scalar, :role "cook"}
                                     {:name "gpus", :scalar gpus, :type :value-scalar, :role "cook"}]
                         :id {:value (str "id-" (UUID/randomUUID))}
                         :slave-id {:value (str "slave-" (UUID/randomUUID))}
                         :hostname (str "host-" (UUID/randomUUID))})
          framework-id #mesomatic.types.FrameworkID{:value "my-framework-id"}
          offers-chan (async/chan (async/buffer 10))
          offer-1 (offer-maker 10 2048 0)
          offer-2 (offer-maker 20 16384 0)
          offer-3 (offer-maker 30 8192 0)
          [d1 d2] [#{{:dataset {"a" "a"}} {:dataset {"b" "b"}}}]
          run-handle-resource-offers! (fn [num-considerable offers &
                                           {:keys [job-name->uuid
                                                   pool-name->optimizer-suggested-job-ids-atom
                                                   rebalancer-reservation-atom
                                                   user-quota user->usage]
                                            :or {job-name->uuid {}
                                                 pool-name->optimizer-suggested-job-ids-atom (atom {})
                                                 rebalancer-reservation-atom (atom {})}}]
                                        (reset! launched-tasks-atom [])
                                        (let [conn (restore-fresh-database! uri)
                                              driver-atom (atom nil)

                                              ^TaskScheduler fenzo (sched/make-fenzo-scheduler driver-atom 1500
                                                                                               "cook.mesos.data-locality/make-data-local-fitness-calculator"
                                                                                               0.8)
                                              group-ent-id (create-dummy-group conn)
                                              get-uuid (fn [name] (get job-name->uuid name (d/squuid)))
                                              job-1 (d/entity (d/db conn) (create-dummy-job conn
                                                                                            :uuid (get-uuid "job-1")
                                                                                            :group group-ent-id
                                                                                            :name "job-1"
                                                                                            :ncpus 3
                                                                                            :memory 2048
                                                                                            :datasets d1))
                                              job-2 (d/entity (d/db conn) (create-dummy-job conn
                                                                                            :uuid (get-uuid "job-2")
                                                                                            :group group-ent-id
                                                                                            :name "job-2"
                                                                                            :ncpus 13
                                                                                            :memory 1024
                                                                                            :datasets d2))
                                              _ (dl/update-data-local-costs {d1 {(:hostname offer-1) 0.0
                                                                                 (:hostname offer-2) 0.0
                                                                                 (:hostname offer-3) 100.0}
                                                                             d2 {(:hostname offer-1) 0.0
                                                                                 (:hostname offer-2) 0.0
                                                                                 (:hostname offer-3) 0.0}}
                                                                            [])
                                              entity->map (fn [entity]
                                                            (util/job-ent->map entity (d/db conn)))
                                              pool->pending-jobs (->> {:normal [job-1 job-2]}
                                                                      (pc/map-vals (partial map entity->map)))
                                              pool-name->pending-jobs-atom (atom pool->pending-jobs)
                                              user->usage (or user->usage {test-user {:count 1, :cpus 2, :mem 1024, :gpus 0}})
                                              user->quota (or user-quota {test-user {:count 10, :cpus 50, :mem 32768, :gpus 10}})
                                              mesos-run-as-user nil
                                              result (sched/handle-resource-offers!
                                                       conn driver fenzo framework-id pool-name->pending-jobs-atom mesos-run-as-user
                                                       user->usage user->quota num-considerable offers-chan offers
                                                       rebalancer-reservation-atom pool-name->optimizer-suggested-job-ids-atom
                                                       :normal)]
                                          result))]
      (testing "enough offers for all normal jobs"
        (let [num-considerable 10
              offers [offer-1 offer-2 offer-3]]
          (is (run-handle-resource-offers! num-considerable offers :job-name->uuid {"job-1" (d/squuid) "job-2" (d/squuid)}))
          (let [launched-tasks @launched-tasks-atom
                task-1 (first (filter #(.startsWith (:name %) "job-1") launched-tasks))
                task-2 (first (filter #(.startsWith (:name %) "job-2") launched-tasks))]
            (is (= 2 (count launched-tasks)))
            (is (= (:slave-id offer-1) (:slave-id task-1)))
            (is (= (:slave-id offer-2) (:slave-id task-2)))))))))

(deftest test-in-order-status-update-processing
  (let [status-store (atom {})
        latch (CountDownLatch. 11)]
    (with-redefs [sched/handle-status-update
                  (fn [_ _ _ _ status]
                    (let [task-id (-> status :task-id :value str)]
                      (swap! status-store update task-id
                             (fn [statuses] (conj (or statuses [])
                                                  (-> status mtypes/pb->data :state)))))
                    (Thread/sleep (rand-int 100))
                    (.countDown latch))]
      (let [s (sched/create-mesos-scheduler nil true nil nil nil nil nil nil nil nil)]

        (.statusUpdate s nil (mtypes/->pb :TaskStatus {:task-id {} :state :task-starting}))
        (.statusUpdate s nil (mtypes/->pb :TaskStatus {:task-id {:value "T1"} :state :task-starting}))
        (.statusUpdate s nil (mtypes/->pb :TaskStatus {:task-id {:value "T2"} :state :task-starting}))
        (.statusUpdate s nil (mtypes/->pb :TaskStatus {:task-id {:value "T1"} :state :task-running}))
        (.statusUpdate s nil (mtypes/->pb :TaskStatus {:task-id {:value "T2"} :state :task-running}))
        (.statusUpdate s nil (mtypes/->pb :TaskStatus {:task-id {:value "T3"} :state :task-starting}))
        (.statusUpdate s nil (mtypes/->pb :TaskStatus {:task-id {:value "T3"} :state :task-running}))
        (.statusUpdate s nil (mtypes/->pb :TaskStatus {:task-id {:value "T1"} :state :task-finished}))
        (.statusUpdate s nil (mtypes/->pb :TaskStatus {:task-id {:value "T3"} :state :task-failed}))
        (.statusUpdate s nil (mtypes/->pb :TaskStatus {:task-id {:value "T4"} :state :task-starting}))
        (.statusUpdate s nil (mtypes/->pb :TaskStatus {:task-id {} :state :task-failed}))

        (.await latch 4 TimeUnit/SECONDS)

        (is (= [:task-starting :task-failed] (->> "" (get @status-store) vec)))
        (is (= [:task-starting :task-running :task-finished] (->> "T1" (get @status-store) vec)))
        (is (= [:task-starting :task-running] (->> "T2" (get @status-store) vec)))
        (is (= [:task-starting :task-running :task-failed] (->> "T3" (get @status-store) vec)))
        (is (= [:task-starting] (->> "T4" (get @status-store) vec)))))))

(deftest test-framework-message-processing-delegation
  (let [framework-message-store (atom [])
        heartbeat-store (atom [])
        sandbox-store (atom [])]
    (with-redefs [heartbeat/notify-heartbeat (fn [_ _ _ framework-message]
                                               (swap! heartbeat-store conj framework-message))
                  sandbox/update-sandbox (fn [_ framework-message]
                                           (swap! sandbox-store conj framework-message))
                  sched/handle-framework-message (fn [_ _ framework-message]
                                                   (swap! framework-message-store conj framework-message))]
      (let [s (sched/create-mesos-scheduler nil true nil nil nil nil nil nil nil nil)
            make-message (fn [message] (-> message json/write-str str (.getBytes "UTF-8")))]

        (testing "message delegation"
          (let [task-id "T1"
                executor-id (-> task-id mtypes/->ExecutorID mtypes/data->pb)
                m1 {"task-id" task-id}
                m2 {"task-id" task-id, "timestamp" 123456, "type" "heartbeat"}
                m3 {"exit-code" 0, "task-id" task-id}
                m4 {"task-id" task-id, "timestamp" 123456, "type" "heartbeat"}
                m5 {"sandbox" "/path/to/a/directory", "task-id" task-id, "type" "directory"}]

            (.frameworkMessage s nil executor-id nil (make-message m1))
            (.frameworkMessage s nil executor-id nil (make-message m2))
            (.frameworkMessage s nil executor-id nil (make-message m3))
            (.frameworkMessage s nil executor-id nil (make-message m4))
            (.frameworkMessage s nil executor-id nil (make-message m5))

            (let [latch (CountDownLatch. 1)]
              (sched/async-in-order-processing task-id #(.countDown latch))
              (.await latch))

            (is (= [m1 m3] @framework-message-store))
            (is (= [m2 m4] @heartbeat-store))
            (is (= [m5] @sandbox-store))))))))

(deftest test-in-order-framework-message-processing
  (let [messages-store (atom {})
        latch (CountDownLatch. 11)]
    (with-redefs [heartbeat/notify-heartbeat (constantly true)
                  sched/handle-framework-message
                  (fn [_ _ framework-message]
                    (let [{:strs [message task-id]} framework-message]
                      (swap! messages-store update (str task-id) (fn [messages] (conj (or messages []) message))))
                    (Thread/sleep (rand-int 100))
                    (.countDown latch))]
      (let [s (sched/create-mesos-scheduler nil true nil nil nil nil nil nil nil nil)
            foo 11
            bar 21
            fee 31
            fie 41
            make-message (fn [index message]
                           (-> {"message" message, "task-id" (str "T" index)}
                               json/write-str
                               str
                               (.getBytes "UTF-8")))]

        (.frameworkMessage s nil (-> "" mtypes/->ExecutorID mtypes/data->pb) nil (make-message 0 foo))
        (.frameworkMessage s nil (-> "T1" mtypes/->ExecutorID mtypes/data->pb) nil (make-message 1 foo))
        (.frameworkMessage s nil (-> "T2" mtypes/->ExecutorID mtypes/data->pb) nil (make-message 2 foo))
        (.frameworkMessage s nil (-> "T1" mtypes/->ExecutorID mtypes/data->pb) nil (make-message 1 bar))
        (.frameworkMessage s nil (-> "T2" mtypes/->ExecutorID mtypes/data->pb) nil (make-message 2 bar))
        (.frameworkMessage s nil (-> "T3" mtypes/->ExecutorID mtypes/data->pb) nil (make-message 3 foo))
        (.frameworkMessage s nil (-> "T3" mtypes/->ExecutorID mtypes/data->pb) nil (make-message 3 bar))
        (.frameworkMessage s nil (-> "T1" mtypes/->ExecutorID mtypes/data->pb) nil (make-message 1 fee))
        (.frameworkMessage s nil (-> "T3" mtypes/->ExecutorID mtypes/data->pb) nil (make-message 3 fie))
        (.frameworkMessage s nil (-> "T4" mtypes/->ExecutorID mtypes/data->pb) nil (make-message 4 foo))
        (.frameworkMessage s nil (-> "" mtypes/->ExecutorID mtypes/data->pb) nil (make-message 0 fie))

        (.await latch 4 TimeUnit/SECONDS)

        (is (= [foo fie] (->> "T0" (get @messages-store) vec)))
        (is (= [foo bar fee] (->> "T1" (get @messages-store) vec)))
        (is (= [foo bar] (->> "T2" (get @messages-store) vec)))
        (is (= [foo bar fie] (->> "T3" (get @messages-store) vec)))
        (is (= [foo] (->> "T4" (get @messages-store) vec)))))))

(defn- task-id->instance-entity
  [db-conn task-id]
  (let [datomic-db (d/db db-conn)]
    (->> task-id
         (d/q '[:find ?i
                :in $ ?task-id
                :where [?i :instance/task-id ?task-id]]
              datomic-db)
         ffirst
         (d/entity (d/db db-conn))
         d/touch)))

(deftest test-sandbox-directory-population-for-mesos-executor-tasks
  (let [db-conn (restore-fresh-database! "datomic:mem://test-sandbox-directory-population")
        executing-tasks-atom (atom #{})
        num-jobs 25
        cache-timeout-ms 65
        get-task-id #(str "task-test-sandbox-directory-population-" %)]
    (dotimes [n num-jobs]
      (let [task-id (get-task-id n)
            job (create-dummy-job db-conn :task-id task-id)]
        (create-dummy-instance db-conn job :executor :executor/mesos :executor-id task-id :task-id task-id)))

    (with-redefs [sandbox/retrieve-sandbox-directories-on-agent
                  (fn [_ _]
                    (pc/map-from-keys #(str "/sandbox/for/" %) @executing-tasks-atom))]
      (let [framework-id "test-framework-id"
            publish-batch-size 10
            publish-interval-ms 20
            sync-interval-ms 20
            max-consecutive-sync-failure 5
            agent-query-cache (-> {} (cache/ttl-cache-factory :ttl cache-timeout-ms) atom)
            {:keys [pending-sync-agent publisher-cancel-fn syncer-cancel-fn task-id->sandbox-agent] :as sandbox-syncer-state}
            (sandbox/prepare-sandbox-publisher
              framework-id db-conn publish-batch-size publish-interval-ms sync-interval-ms max-consecutive-sync-failure
              agent-query-cache)
            sync-agent-sandboxes-fn
            (fn [hostname task-id]
              (sandbox/sync-agent-sandboxes sandbox-syncer-state framework-id hostname task-id))]
        (try
          (-> (dotimes [n num-jobs]
                (let [task-id (get-task-id n)]
                  (swap! executing-tasks-atom conj task-id)
                  (->> {:task-id {:value task-id}, :state :task-running}
                       (sched/handle-status-update db-conn nil {} sync-agent-sandboxes-fn)))
                (Thread/sleep 5))
              async/thread
              async/<!!)

          (dotimes [n num-jobs]
            (let [task-id (get-task-id n)
                  instance-ent (task-id->instance-entity db-conn task-id)]
              (is (= "localhost" (:instance/hostname instance-ent)))
              (is (= :instance.status/running (:instance/status instance-ent)))
              (is (= :executor/mesos (:instance/executor instance-ent)))))

          (Thread/sleep (+ cache-timeout-ms sync-interval-ms))
          (await pending-sync-agent)
          (Thread/sleep (* 2 publish-interval-ms))
          (await task-id->sandbox-agent)

          ;; verify the sandbox-directory stored into the db
          (dotimes [n num-jobs]
            (let [task-id (get-task-id n)
                  instance-ent (task-id->instance-entity db-conn task-id)]
              (is (= (str "/sandbox/for/" task-id) (:instance/sandbox-directory instance-ent)))))

          (finally
            (publisher-cancel-fn)
            (syncer-cancel-fn)))))))

(deftest test-no-sandbox-directory-population-for-cook-executor-tasks
  (let [db-conn (restore-fresh-database! "datomic:mem://test-sandbox-directory-population")
        executing-tasks-atom (atom #{})
        num-jobs 25
        get-task-id #(str "task-test-sandbox-directory-population-" %)
        sync-agent-sandboxes-fn (fn [_] (throw (Exception. "Unexpected call for cook executor task")))]
    (dotimes [n num-jobs]
      (let [task-id (get-task-id n)
            job (create-dummy-job db-conn :task-id task-id)]
        (create-dummy-instance db-conn job :executor :executor/cook :executor-id task-id :task-id task-id)))

    (try
      (-> (dotimes [n num-jobs]
            (let [task-id (get-task-id n)]
              (swap! executing-tasks-atom conj task-id)
              (->> {:task-id {:value task-id}, :state :task-running}
                   (sched/handle-status-update db-conn nil (constantly nil) sync-agent-sandboxes-fn)))
            (Thread/sleep 5))
          async/thread
          async/<!!)

      ;; verify the instance state
      (dotimes [n num-jobs]
        (let [task-id (get-task-id n)
              instance-ent (task-id->instance-entity db-conn task-id)]
          (is (= "localhost" (:instance/hostname instance-ent)))
          (is (= :instance.status/running (:instance/status instance-ent)))
          (is (= :executor/cook (:instance/executor instance-ent)))
          (is (nil? (:instance/sandbox-directory instance-ent))))))))


(deftest test-monitor-tx-report-queue
  (let [conn (restore-fresh-database! "datomic:mem://test-monitor-tx-report-queue")
        job-id-1 (create-dummy-job conn)
        instance-id-1 (create-dummy-instance conn job-id-1)
        job-id-2 (create-dummy-job conn)
        instance-id-2 (create-dummy-instance conn job-id-2)
        killed-tasks (atom [])
        mock-driver (reify msched/SchedulerDriver
                      (kill-task! [_ task-id]
                        (swap! killed-tasks #(conj % task-id))))
        driver-ref (ref mock-driver)]
    (cook.mesos/kill-job conn [(:job/uuid (d/entity (d/db conn) job-id-1))])
    (let [job-ent (d/entity (d/db conn) job-id-1)
          instance-ent (d/entity (d/db conn) instance-id-1)]
      (is (= :job.state/completed (:job/state job-ent)))
      (is (= :instance.status/unknown (:instance/status instance-ent))))
    (let [[report-mult close-fn] (cook.datomic/create-tx-report-mult conn)
          transaction-chan (async/chan (async/sliding-buffer 4096))
          _ (async/tap report-mult transaction-chan)
          kill-fn (cook.mesos.scheduler/monitor-tx-report-queue transaction-chan conn driver-ref)]
      (cook.mesos/kill-job conn [(:job/uuid (d/entity (d/db conn) job-id-2))])
      (let [expected-tasks-killed [{:value (:instance/task-id (d/entity (d/db conn) instance-id-1))}
                                   {:value (:instance/task-id (d/entity (d/db conn) instance-id-2))}]
            tasks-killed? (fn [] (= expected-tasks-killed @killed-tasks))]
        (is (wait-for tasks-killed? :interval 20 :timeout 100 :unit-multiplier 1))))))

(deftest test-launch-matched-tasks!-logs-transaction-timeouts
  (let [conn (restore-fresh-database! "datomic:mem://test-launch-matched-tasks!-logs-transaction-timeouts")
        timeout-exception (ex-info "Transaction timed out." {})
        logged-atom (atom nil)
        job-id (create-dummy-job conn)
        job (d/entity (d/db conn) job-id)
        ^TaskRequest task-request (sched/make-task-request (d/db conn) job)
        matches [{:tasks [(SimpleAssignmentResult. [] nil task-request)]}]]
    (with-redefs [d/transact (fn [_ _]
                               (throw timeout-exception))
                  log/log* (fn [_ level throwable message]
                             (when (= :warn level)
                               (reset! logged-atom {:throwable throwable
                                                    :message message})))]
      (is (thrown? ExceptionInfo (sched/launch-matched-tasks! matches conn nil nil nil nil nil nil)))
      (is (= timeout-exception (:throwable @logged-atom)))
      (is (str/includes? (:message @logged-atom) (str job-id))))))

(deftest test-reconcile-tasks
  (let [conn (restore-fresh-database! "datomic:mem://test-reconcile-tasks")
        fenzo (make-dummy-scheduler)
        task-atom (atom [])
        mock-driver (reify msched/SchedulerDriver
                      (reconcile-tasks [_ tasks]
                        (reset! task-atom tasks)))
        [_ [running-instance-id]] (create-dummy-job-with-instances conn
                                                                   :job-state :job.state/running
                                                                   :instances [{:instance-status :instance.status/running}])
        [_ [unknown-instance-id]] (create-dummy-job-with-instances conn
                                                                   :job-state :job.state/running
                                                                   :instances [{:instance-status :instance.status/unknown}])
        _ (create-dummy-job-with-instances conn
                                           :job-state :job.state/completed
                                           :instances [{:instance-status :instance.status/success}])]
    (sched/reconcile-tasks (d/db conn) mock-driver (constantly fenzo))
    (let [reconciled-tasks (set @task-atom)
          running-instance (d/entity (d/db conn) running-instance-id)
          unknown-instance (d/entity (d/db conn) unknown-instance-id)]
      (is (= 2 (count reconciled-tasks)))
      (is (contains? reconciled-tasks {:task-id {:value (:instance/task-id running-instance)}
                                       :state :task-running
                                       :slave-id {:value (:instance/slave-id running-instance)}}))
      (is (contains? reconciled-tasks {:task-id {:value (:instance/task-id unknown-instance)}
                                       :state :task-staging
                                       :slave-id {:value (:instance/slave-id unknown-instance)}})))))
