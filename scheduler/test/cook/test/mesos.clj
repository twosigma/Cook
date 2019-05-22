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
(ns cook.test.mesos
  (:use clojure.test)
  (:require [clojure.core.async :as async]
            [cook.config :as config]
            [cook.curator :as curator]
            [cook.datomic]
            [cook.mesos :as mesos]
            [cook.scheduler.scheduler :as sched]
            [cook.schema :as schem]
            [datomic.api :as d :refer (q db)])
  (:import [org.apache.curator.framework CuratorFrameworkFactory CuratorFramework]
           [org.apache.curator.retry BoundedExponentialBackoffRetry]
           [org.apache.curator.test TestingServer]))

(defmacro with-zk [[connect-string] & body]
  "Evaluates body with a zookeeper test instance running"
  `(let [zk# (TestingServer.)
         ~connect-string (.getConnectString zk#)]
     (try
       ~@body
       (finally
         (.close zk#)))))

(deftest curator-set-get
  (with-zk [cs]
    (let [framework (doto
                        (.. (CuratorFrameworkFactory/builder)
                            (connectString cs)
                            (retryPolicy (BoundedExponentialBackoffRetry. 1000 30000 3))
                            build)
                      .start)]
      (do
        (curator/set-or-create framework "/my/data" (.getBytes "some-data"))
        (= "some-data" (String. (curator/get-or-nil framework "/my/data")))))))

(deftest curator-get-nil
  (with-zk [cs]
    (let [framework (doto
                        (.. (CuratorFrameworkFactory/builder)
                            (connectString cs)
                            (retryPolicy (BoundedExponentialBackoffRetry. 1000 30000 3))
                            build)
                      .start)]
      (do
        (curator/set-or-create framework "/my/data" (.getBytes "some-data"))
      (= nil (curator/get-or-nil framework "/my/missing/data"))))))

(defn make-jobs-in-db
  "Takes a list of pairs, where the pairs are the cpu/memory of the job, and returns
   the job test DB. The jobs will have consequtive ids, starting from 0"
  [cpu-mem-pairs]
  (let [l (count cpu-mem-pairs)]
    (mapcat (fn [[cpu mem] id]
              [[id :job/resource (+ l id)]
               [(+ l id) :resource/type :resource.type/cpus]
               [(+ l id) :resource/amount (double cpu)]
               [id :job/resource (+ l l id)]
               [(+ l l id) :resource/type :resource.type/mem]
               [(+ l l id) :resource/amount (double mem)]])
            cpu-mem-pairs
            (range))))

(defn make-offer
  [cpus mem]
  {:slave-id "mycoolslave"
   :resources [{:name "cpus" :scalar cpus}
               {:name "mem" :scalar mem}]})

(defn make-fake-job-with-tasks
  "Takes the state of the job and the statuses of the instances. Returns the
   id given to the job, for easy future querying"
  [conn job-state & task-states]
  (let [job-tempid (d/tempid :db.part/user)
        {:keys [tempids db-after]} @(d/transact conn [[:db/add job-tempid :job/state job-state]
                                                      [:db/add job-tempid :job/max-retries 3]])
        job-id (d/resolve-tempid db-after tempids job-tempid)]
    (when (seq task-states)
      @(d/transact conn
                   (mapcat (fn [state]
                             (let [task-tempid (d/tempid :db.part/user)]
                               [[:db/add job-id :job/instance task-tempid]
                                [:db/add task-tempid :instance/status state]]))
                           task-states)))
    job-id))

(defn test-fake-job
  "Takes the initial and target state of the job, and the statuses of the instances. Uses
   `is` to validate."
  [testname conn init-job-state final-job-state & task-states]
  (testing testname
    (let [j (apply make-fake-job-with-tasks
                 conn
                 init-job-state
                 task-states)]
    @(d/transact conn [[:job/update-state j]])
    (is (= final-job-state (:job/state (d/entity (db conn) j)))))))

(deftest test-job-update-state
  (let [test-db-uri "datomic:mem://test-update-state-db"
        _ (d/create-database test-db-uri)
        conn (d/connect test-db-uri)]
    (doseq [init cook.schema/work-item-schema]
      @(d/transact conn init))
    ;; Success means we're done
    (test-fake-job
      "Waiting to completed"
      conn
      :job.state/waiting
      :job.state/completed
      :instance.status/success)
    (test-fake-job
      "Completed in multiple tries"
      conn
      :job.state/waiting
      :job.state/completed
      :instance.status/failed
      :instance.status/failed
      :instance.status/success)
    ;; Start running
    (test-fake-job
      "Running to still running"
      conn
      :job.state/running
      :job.state/running
      :instance.status/running)
    (test-fake-job
      "Waiting to running"
      conn
      :job.state/waiting
      :job.state/running
      :instance.status/running)
    (test-fake-job
      "Waiting to running, some fails"
      conn
      :job.state/waiting
      :job.state/running
      :instance.status/failed
      :instance.status/failed
      :instance.status/running)
    (test-fake-job
      "Not done yet, back to waiting"
      conn
      :job.state/waiting
      :job.state/waiting
      :instance.status/failed)
    (test-fake-job
      "Running to waiting due to fail"
      conn
      :job.state/running
      :job.state/waiting
      :instance.status/failed
      :instance.status/failed)
    (test-fake-job
      "Waiting to still waiting due to fails"
      conn
      :job.state/waiting
      :job.state/waiting
      :instance.status/failed
      :instance.status/failed)
    (test-fake-job
      "Waiting and out of retries means done"
      conn
      :job.state/waiting
      :job.state/completed
      :instance.status/failed
      :instance.status/failed
      :instance.status/failed)
    (test-fake-job
      "Completed and out of retries means still done"
      conn
      :job.state/completed
      :job.state/completed
      :instance.status/failed
      :instance.status/failed
      :instance.status/failed)
    (test-fake-job
      "Running and out of retries means done"
      conn
      :job.state/running
      :job.state/completed
      :instance.status/failed
      :instance.status/failed
      :instance.status/failed)
    (test-fake-job
      "Always stay completed"
      conn
      :job.state/completed
      :job.state/completed
      :instance.status/running)
    (d/delete-database test-db-uri)))

(deftest test-transact-with-retries
  (let [test-db-uri "datomic:mem://test-transact-db"
        _ (d/create-database test-db-uri)
        conn (d/connect test-db-uri)]
    (doseq [init cook.schema/work-item-schema]
      @(d/transact conn init))
    (async/<!! (cook.datomic/transact-with-retries conn [[:db/add (d/tempid :db.part/user) :job/command "txn"]]))
    (is (seq (q '[:find ?j
                  :where
                  [?j :job/command "txn"]]
                (d/db conn))))
    (d/delete-database test-db-uri)))

(def sample-txns
  [[
    {:db/id (d/tempid :db.part/db)
     :db/ident :foo/bar
     :db/valueType :db.type/string
     :db/cardinality :db.cardinality/one
     :db.install/_attribute :db.part/db}
    {:db/id (d/tempid :db.part/db)
     :db/ident :foo/baz
     :db/valueType :db.type/string
     :db/cardinality :db.cardinality/one
     :db.install/_attribute :db.part/db}]
   [
    {:db/id (d/tempid :db.part/db)
     :db/ident :bar/foo
     :db/valueType :db.type/string
     :db/cardinality :db.cardinality/one
     :db.install/_attribute :db.part/db}
    {:db/id (d/tempid :db.part/db)
     :db/ident :bar/baz
     :db/valueType :db.type/string
     :db/cardinality :db.cardinality/one
     :db.install/_attribute :db.part/db}
    {:db/id (d/tempid :db.part/db)
     :db/ident :bar/natan
     :db/valueType :db.type/string
     :db/cardinality :db.cardinality/one
     :db.install/_attribute :db.part/db}]
   [
    {:db/id (d/tempid :db.part/db)
     :db/ident :baz/bar
     :db/valueType :db.type/string
     :db/cardinality :db.cardinality/one
     :db.install/_attribute :db.part/db}
    {:db/id (d/tempid :db.part/db)
     :db/ident :baz/foo
     :db/valueType :db.type/string
     :db/cardinality :db.cardinality/one
     :db.install/_attribute :db.part/db}]
   ])

(deftest test-tx-report-queue
  (let [test-db-uri "datomic:mem://test-transact-db"
        _ (d/create-database test-db-uri)
        conn (d/connect test-db-uri)
        [mult kill-thread] (cook.datomic/create-tx-report-mult conn)
        chan (async/chan)]
    (async/tap mult chan)
    (try
      (doseq [txn sample-txns]
        @(d/transact conn txn))
      (is (= (count sample-txns)
             (loop [seen 0]
               (async/alt!! chan ([c] (recur (+ 1 seen)))
                            (async/timeout 100) ([_] seen)))))
      (finally
        (kill-thread)
        (async/untap mult chan)
        (async/close! chan)
        (d/delete-database test-db-uri)))))


(deftest test-make-trigger-chans
  (with-redefs [config/data-local-fitness-config (constantly {:update-interval-ms nil})]
    (let [trigger-chans (mesos/make-trigger-chans {:interval-seconds 1}
                                                  {:publish-interval-ms 1000}
                                                  {:optimizer-interval-seconds 1}
                                                  {})]
      (is (nil? (:update-data-local-costs-trigger-chan trigger-chans)))))

  (with-redefs [config/data-local-fitness-config (constantly {:update-interval-ms 1000})]
    (let [trigger-chans (mesos/make-trigger-chans {:interval-seconds 1}
                                                  {:publish-interval-ms 1000}
                                                  {:optimizer-interval-seconds 1}
                                                  {})]
      (is (:update-data-local-costs-trigger-chan trigger-chans)))))
