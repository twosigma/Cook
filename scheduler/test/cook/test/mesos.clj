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
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [cook.compute-cluster :as cc]
            [cook.datomic]
            [cook.mesos :as mesos]
            [cook.test.postgres]
            [cook.test.testutil :refer [create-dummy-job-with-instances restore-fresh-database!]]
            [datomic.api :as d :refer (q db)]))

(use-fixtures :once cook.test.postgres/with-pg-db)

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

(defn dumy-new-cluster-configurations-fn [])
(deftest test-dynamic-compute-cluster-configurations-setup
  (let [uri "datomic:mem://test-compute-cluster-config"
        conn (restore-fresh-database! uri)
        name "cluster1"
        cluster-db-id (cc/write-compute-cluster conn {:compute-cluster/cluster-name name})
        _ (create-dummy-job-with-instances
            conn
            :job-state :job.state/running
            :instances [{:instance-status :instance.status/running
                         :task-id "12345"
                         :compute-cluster (reify cc/ComputeCluster
                                            (db-id [_] cluster-db-id)
                                            (compute-cluster-name [_] name))}])
        name "cluster2"
        cluster-db-id (cc/write-compute-cluster conn {:compute-cluster/cluster-name name})
        _ (create-dummy-job-with-instances
            conn
            :job-state :job.state/running
            :instances [{:instance-status :instance.status/unknown
                         :task-id "12346"
                         :compute-cluster (reify cc/ComputeCluster
                                            (db-id [_] cluster-db-id)
                                            (compute-cluster-name [_] name))}])]
    (let [db (d/db conn)
          log-error-invocations-atom (atom nil)
          scheduleAtFixedRate-invocations-atom (atom [])]
      (is (= (set '("cluster1" "cluster2")) (set (->> (cook.tools/get-running-task-ents db) (map (fn [e] (-> e :instance/compute-cluster :compute-cluster/cluster-name)))))))
      (with-redefs [log/log* (fn [_ _ _ message] (reset! log-error-invocations-atom message))
                    cc/get-db-config-ents (fn [_] {})
                    cc/update-compute-clusters (fn [_ _ _])
                    cc/update-compute-clusters-helper (fn [_ _ _ _ _])
                    mesos/make-compute-cluster-config-updater-task (fn [_ _])
                    t/now (constantly (org.joda.time.DateTime/parse "2020-08-26T16:35:35.946Z"))
                    chime/chime-at (fn [a _ & _]
                                     (swap! scheduleAtFixedRate-invocations-atom conj (str "chime called " (first a) (second a)))
                                     nil)]
        (is (= nil (mesos/dynamic-compute-cluster-configurations-setup nil {})))
        (is (= nil (mesos/dynamic-compute-cluster-configurations-setup
                     conn {:new-cluster-configurations-fn 'cook.test.mesos/dumy-new-cluster-configurations-fn})))
        (is (= nil @log-error-invocations-atom))
        (is (= ["chime called 2020-08-26T16:36:35.946Z2020-08-26T16:37:35.946Z"] @scheduleAtFixedRate-invocations-atom))
        (reset! cc/cluster-name->compute-cluster-atom {"cluster2" nil})
        (is (= nil (mesos/dynamic-compute-cluster-configurations-setup
                     conn {:load-clusters-on-startup? true})))
        (reset! cc/cluster-name->compute-cluster-atom {})
        (is (re-matches #"Can't find cluster configurations for some of the running jobs! \{:missing-cluster-names #\{cluster1\}, :cluster-name->instance-ids \{cluster1 \(12345\)\}\}"
                        @log-error-invocations-atom))))))
