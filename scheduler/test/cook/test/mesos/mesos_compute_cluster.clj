(ns cook.test.mesos.mesos-compute-cluster
  (:require [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clojure.data.json :as json]
            [clojure.test :refer :all]
            [cook.mesos.heartbeat :as heartbeat]
            [cook.mesos.mesos-compute-cluster :as mcc]
            [cook.mesos.sandbox :as sandbox]
            [cook.test.postgres]
            [cook.scheduler.scheduler :as sched]
            [cook.test.testutil :as testutil :refer [create-dummy-instance create-dummy-job]]
            [datomic.api :as d]
            [mesomatic.scheduler :as msched]
            [mesomatic.types :as mtypes]
            [plumbing.core :as pc])
  (:import (java.util.concurrent CountDownLatch TimeUnit)
           (org.apache.mesos Protos$TaskStatus$Reason)))

(use-fixtures :once cook.test.postgres/with-pg-db)

(deftest test-in-order-status-update-processing
  (let [status-store (atom {})
        latch (CountDownLatch. 11)]
    (with-redefs [mcc/handle-status-update
                  (fn [_ _ _ _ status]
                    (let [task-id (-> status :task-id :value str)]
                      (swap! status-store update task-id
                             (fn [statuses] (conj (or statuses [])
                                                  (-> status mtypes/pb->data :state)))))
                    (Thread/sleep (rand-int 100))
                    (.countDown latch))]
      (let [s (mcc/create-mesos-scheduler nil true nil nil nil nil nil nil nil nil nil)]
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
      (let [s (mcc/create-mesos-scheduler nil true nil nil nil nil nil nil nil nil nil)
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
      (let [s (mcc/create-mesos-scheduler nil true nil nil nil nil nil nil nil nil nil)
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

(deftest test-get-or-create-entity-id
  (let [conn (testutil/restore-fresh-database! "datomic:mem://compute-cluster-factory")
        mesos-1 {:compute-cluster-name "mesos-1" :framework-id "mesos-1a"}
        mesos-2 {:compute-cluster-name "mesos-2" :framework-id "mesos-1a"}]
    (testing "Start with no clusters"
      (is (= nil (mcc/get-mesos-cluster-entity-id (d/db conn) mesos-1)))
      (is (= nil (mcc/get-mesos-cluster-entity-id (d/db conn) mesos-2))))

    (testing "Create a cluster. Should be a new cluster"
      (let [id1a (mcc/get-or-create-cluster-entity-id conn (:compute-cluster-name mesos-1) (:framework-id mesos-1))]
        ; This should create one cluster in the DB, but not the other.
        (is (not= nil (mcc/get-mesos-cluster-entity-id (d/db conn) mesos-1)))
        (is (= nil (mcc/get-mesos-cluster-entity-id (d/db conn) mesos-2)))
        (let [id2a (mcc/get-or-create-cluster-entity-id conn (:compute-cluster-name mesos-2) (:framework-id mesos-2))
              id1b (mcc/get-or-create-cluster-entity-id conn (:compute-cluster-name mesos-1) (:framework-id mesos-1))
              id2b (mcc/get-or-create-cluster-entity-id conn (:compute-cluster-name mesos-2) (:framework-id mesos-2))]
          ; Should see both clusters created.
          (is (not= nil (mcc/get-mesos-cluster-entity-id (d/db conn) mesos-1)))
          (is (not= nil (mcc/get-mesos-cluster-entity-id (d/db conn) mesos-2)))
          (is (not= (mcc/get-mesos-cluster-entity-id (d/db conn) mesos-1)
                    (mcc/get-mesos-cluster-entity-id (d/db conn) mesos-2)))
          ; Now, we should only have two unique db-id's.
          (is (= id1a id1b))
          (is (= id2a id2b))

          (is (and id1a (< 0 id1a)))
          (is (and id2a (< 0 id2a)))
          (is (and id1b (< 0 id1b)))
          (is (and id2b (< 0 id2b))))))))

(defn make-dummy-status-update
  [task-id reason state & {:keys [progress] :or {progress nil}}]
  {:task-id {:value task-id}
   :reason reason
   :state state})

(deftest test-handle-status-update
  (let [uri "datomic:mem://test-handle-status-update"
        tasks-killed (atom #{})
        driver (reify msched/SchedulerDriver
                 (kill-task! [_ task] (swap! tasks-killed conj (:value task)))) ; Conjoin the task-id
        fenzo-state (sched/make-fenzo-state 1500 nil 0.8)
        synced-agents-atom (atom [])
        sync-agent-sandboxes-fn (fn sync-agent-sandboxes-fn [hostname _]
                                  (swap! synced-agents-atom conj hostname))]
    (testing "Tasks of completed jobs are killed"
      (let [conn (testutil/restore-fresh-database! uri)
            compute-cluster (testutil/fake-test-compute-cluster-with-driver conn uri driver)
            job-id (create-dummy-job conn
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
             (mcc/handle-status-update conn compute-cluster sync-agent-sandboxes-fn (constantly fenzo-state)))
        (is (true? (contains? @tasks-killed task-id-a)))
        (is (= ["www.test-host.com"] @synced-agents-atom))))

    (testing "task-killed-during-launch reason"
      (let [conn (testutil/restore-fresh-database! uri)
            compute-cluster (testutil/fake-test-compute-cluster-with-driver conn uri driver)
            job-id (create-dummy-job conn :job-state :job.state/running)
            task-id-a "taska"
            mapped-reason (atom nil)]
        (create-dummy-instance conn job-id
                               :instance-status :instance.status/running
                               :task-id task-id-a
                               :reason :unknown)
        (with-redefs [sched/write-status-to-datomic (fn [_ _ {:keys [reason]}]
                                                      (reset! mapped-reason reason))]
          (->> (make-dummy-status-update task-id-a
                                         Protos$TaskStatus$Reason/REASON_TASK_KILLED_DURING_LAUNCH
                                         :task-running)
               (mcc/handle-status-update conn compute-cluster sync-agent-sandboxes-fn (constantly fenzo-state))))
        (is (= :reason-killed-during-launch @mapped-reason))))))

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
  (testutil/setup)
  (let [db-conn (testutil/restore-fresh-database! "datomic:mem://test-sandbox-directory-population")
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
                       (mcc/handle-status-update db-conn nil sync-agent-sandboxes-fn {})))
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
  (let [db-conn (testutil/restore-fresh-database! "datomic:mem://test-sandbox-directory-population")
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
                   (mcc/handle-status-update db-conn nil sync-agent-sandboxes-fn (constantly nil))))
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