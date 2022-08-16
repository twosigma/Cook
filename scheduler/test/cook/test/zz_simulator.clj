;; This tests leaves state behind that messes up cook.test.mesos.mesos-mock, cook.test.unscheduled, rest.api, and data-locality.
;; It seems to leave behind a thread that changes job states; maybe restoring consistency between task state and
;; job state?
(ns cook.test.zz-simulator
  (:gen-class)
  (:require [cheshire.core :as cheshire]
            [chime :refer [chime-ch]]
            [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.data.csv :as csv]
            [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.tools.logging :as log]
            [clojure.walk :refer [keywordize-keys]]
            [com.rpl.specter :refer [ALL FIRST MAP-KEYS MAP-VALS select transform]]
            [cook.config :refer [default-fitness-calculator executor-config init-logger pool-schedulers]]
            [cook.datomic :as datomic]
            [cook.mesos :as c]
            [cook.mesos.mesos-compute-cluster :as mcc]
            [cook.mesos.mesos-mock :as mm]
            [cook.plugins.completion :as completion]
            [cook.test.postgres]
            [cook.progress :as progress]
            [cook.scheduler.scheduler :as sched]
            [cook.scheduler.share :as share]
            [cook.test.testutil :as testutil :refer [poll-until restore-fresh-database!]]
            [cook.tools :as util]
            [datomic.api :as d]
            [plumbing.core :refer [map-from-vals map-keys map-vals]])
  (:import (java.util Date)
           (java.util.concurrent.locks ReentrantReadWriteLock)
           (org.apache.curator.framework CuratorFrameworkFactory)
           (org.apache.curator.framework.state ConnectionStateListener)
           (org.apache.curator.retry BoundedExponentialBackoffRetry)
           (org.joda.time DateTimeUtils)))

(use-fixtures :once cook.test.postgres/with-pg-db)

;;; This namespace contains a simulator for cook scheduler that accepts a trace file
;;; (defined later on) of jobs to "run" through the simulation as well as how much
;;; faster to run through time (e.g. 2x faster). The output is a "run trace" which
;;; is a csv with the following fields:
;;;  [job_uuid, task_uuid, submit_time, start_time, end_time, host_name, slave_id, status]

;;; Job trace must have the following keys per job:
;;; [:submit-time-ms :run-time-ms :job/uuid :job/command :job/user :job/name :job/max-retries
;;;  :job/max-runtime :job/priority :job/disable-mea-culpa-retries :job/resource]

(defn setup-test-curator-framework
  ([]
   (setup-test-curator-framework 2282))
  ([zk-port]
    ;; Copied from src/cook/components
    ;; TODO: don't copy from components
   (let [retry-policy (BoundedExponentialBackoffRetry. 100 120000 10)
         ;; The 180s session and 30s connection timeouts were pulled from a google group
         ;; recommendation
         zookeeper-server (org.apache.curator.test.TestingServer. zk-port false)
         zk-str (str "localhost:" zk-port)
         curator-framework (CuratorFrameworkFactory/newClient zk-str 180000 30000 retry-policy)]
     (.start zookeeper-server)
     (.. curator-framework
         getConnectionStateListenable
         (addListener (reify ConnectionStateListener
                        (stateChanged [_ client newState]
                          (log/info "Curator state changed:"
                                    (str newState))))))
     (.start curator-framework)
     [zookeeper-server curator-framework])))

(def default-rebalancer-config {:interval-seconds 5
                                :dru-scale 1.0
                                :safe-dru-threshold 1.0
                                :min-dru-diff 0.5
                                :max-preemption 100.0
                                :pool-regex ".*"})

(def default-schedulers-config [{:pool-regex ".*"
                                 :scheduler-config {:scheduler "fenzo"
                                                    :good-enough-fitness 1.0
                                                    :fenzo-fitness-calculator default-fitness-calculator
                                                    :fenzo-max-jobs-considered 2000
                                                    :fenzo-scaleback 0.95
                                                    :fenzo-floor-iterations-before-warn 10
                                                    :fenzo-floor-iterations-before-reset 1000}}])

(def default-task-constraints {:timeout-hours 1
                               :timeout-interval-minutes 1
                               :memory-gb 140
                               :cpus 40
                               :retry-limit 5})

(defmacro with-cook-scheduler
  [conn make-mesos-driver-fn scheduler-config trigger-matching? schedulers-config & body]
  `(let [conn# ~conn
         [zookeeper-server# curator-framework#] (setup-test-curator-framework)
         mesos-mult# (or (:mesos-datomic-mult ~scheduler-config)
                         (async/mult (async/chan)))
         pool-name->pending-jobs-atom# (or (:pool-name->pending-jobs-atom ~scheduler-config)
                                           (atom {}))
         zk-prefix# (or (:zk-prefix ~scheduler-config)
                        "/cook")
         offer-incubate-time-ms# (or (:offer-incubate-time-ms ~scheduler-config)
                                     1000)
         mea-culpa-failure-limit# (or (:mea-culpa-failure-limit ~scheduler-config)
                                      5)
         task-constraints# (merge default-task-constraints (:task-constraints ~scheduler-config))
         executor-config# {:command "cook-executor"
                           :default-progress-regex-string "regex-string"
                           :log-level "INFO"
                           :max-message-length 512
                           :portion 0.25
                           :progress-sample-interval-ms 1000
                           :uri {:cache true
                                 :executable true
                                 :extract false
                                 :value "file:///path/to/cook/executor"}}
         progress-config# {:batch-size 100
                           :pending-threshold 1000
                           :publish-interval-ms 2000
                           :sequence-cache-threshold 1000}
         rebalancer-config# (merge default-rebalancer-config (:rebalancer-config ~scheduler-config))
         framework-id# "cool-framework-id"
         exit-code-syncer-state# {:task-id->exit-code-agent (agent {})}
         sandbox-syncer-state# {:task-id->sandbox-agent (agent {})}
         host-settings# {:server-port 12321 :hostname "localhost"}
         leadership-atom# (atom false)
         optimizer-config# (or (:optimizer-config ~scheduler-config)
                               {})
         trigger-chans# (or (:trigger-chans ~scheduler-config)
                            (c/make-trigger-chans rebalancer-config# progress-config# optimizer-config# task-constraints#))
         progress-update-chans# (or (:progress-update-chans ~scheduler-config)
                                    (progress/make-progress-update-channels
                                      (:progress-updater-trigger-chan trigger-chans#) progress-config# conn#))
         mesos-heartbeat-chan# (async/chan 1024)
         create-compute-cluster# (fn [compute-cluster-name# framework-id# db-id# driver-atom#]
                                   (mcc/->MesosComputeCluster compute-cluster-name#
                                                              framework-id#
                                                              db-id#
                                                              driver-atom#
                                                              sandbox-syncer-state#
                                                              exit-code-syncer-state#
                                                              mesos-heartbeat-chan#
                                                              progress-update-chans#
                                                              trigger-chans#
                                                              {}
                                                              {"no-pool" (async/chan 100)}
                                                              {}
                                                              cook.rate-limit/AllowAllRateLimiter
                                                              (ReentrantReadWriteLock. true)))
         prepare-match-trigger-chan-orig# ~sched/prepare-match-trigger-chan]
     (try
       (with-redefs [executor-config (constantly executor-config#)
                     completion/plugin completion/no-op
                     ; This initializatioon is needed so the code to validate that the
                     ; registration responses matches the configured cook scheduler passes simulator
                     ; and mesos-mock unit tests. (cook.scheduler, lines 1428 create-mesos-scheduler)
                     mcc/make-mesos-driver ~make-mesos-driver-fn
                     datomic/conn conn#
                     sched/prepare-match-trigger-chan (fn [match-trigger-chan# pools#]
                                                        (when
                                                          ~trigger-matching?
                                                          (prepare-match-trigger-chan-orig# match-trigger-chan# pools#)))
                     pool-schedulers (constantly (if (empty? ~schedulers-config)
                                                          default-schedulers-config
                                                          ~schedulers-config))]
         (testutil/fake-test-compute-cluster-with-driver conn#
                                                         testutil/fake-test-compute-cluster-name
                                                         nil ; no dummy driver - simulator is going to call initialize
                                                         create-compute-cluster#
                                                         framework-id#)
         (c/start-leader-selector
           {:curator-framework curator-framework#
            :mea-culpa-failure-limit mea-culpa-failure-limit#
            :mesos-datomic-conn conn#
            :mesos-datomic-mult mesos-mult#
            :mesos-heartbeat-chan mesos-heartbeat-chan#
            :leadership-atom leadership-atom#
            :pool-name->pending-jobs-atom pool-name->pending-jobs-atom#
            :mesos-run-as-user nil
            :offer-incubate-time-ms offer-incubate-time-ms#
            :optimizer-config optimizer-config#
            :rebalancer-config rebalancer-config#
            :server-config host-settings#
            :task-constraints task-constraints#
            :trigger-chans trigger-chans#
            :zk-prefix zk-prefix#
            :api-only false})
         (do ~@body))
       (finally
         (.close curator-framework#)
         (.stop zookeeper-server#)
         (doseq [[trigger-service# trigger-chan#] trigger-chans#]
           (log/info "Shutting down" trigger-service#)
           (async/close! trigger-chan#))))))

(defn generate-task-trace-map
  [task]
  (let [job (:job/_instance task)
        resources (util/job-ent->resources job)
        group (first (:group/_job job))]
    {:job_id (str (:job/uuid job))
     :instance_id (:instance/task-id task)
     :submit_time_ms (.getTime (:job/submit-time job))
     :mesos_start_time_ms (if (:instance/mesos-start-time task)
                            (.getTime (:instance/mesos-start-time task))
                            -1)
     :group_id (:group/uuid group)
     :start_time_ms (.getTime (:instance/start-time task))
     :end_time_ms (.getTime (or (:instance/end-time task) (tc/to-date (t/now))))
     :expected_run_time (:job/expected-runtime job)
     :status (:instance/status task)
     :hostname (:instance/hostname task)
     :slave_id (:instance/slave-id task)
     :reason (or (when (= (:instance/status task) :instance.status/failed)
                   (:reason/string (:instance/reason task)))
                 "")
     :user (:job/user job)
     :mem (or (:mem resources) -1)
     :cpus (or (:cpus resources) -1)
     :job_name (or (:job/name job) "")
     :requested_run_time (->> (:job/label job)
                              (filter #(= (:label/key %) "JOB-RUNTIME"))
                              first
                              :label/value)
     :requested_status (->> (:job/label job)
                            (filter #(= (:label/key %) "JOB-STATUS"))
                            first
                            :label/value)}))

(defn dump-jobs-to-csv
  "Given a mesos db, dump a csv with a row per task"
  [task-ents file & {:keys [add-headers append] :or {add-headers true append false}}]
  ;; Use snake case to make it easier for downstream tools to consume
  (let [headers [:job_id :instance_id :group_id :submit_time_ms :mesos_start_time_ms :start_time_ms
                 :end_time_ms :hostname :slave_id :status :reason :user :mem :cpus :job_name
                 :requested_run_time :expected_run_time :requested_status]
        tasks (map generate-task-trace-map task-ents)]
    (with-open [out-file (io/writer file :append append)]
      (csv/write-csv out-file
                     (concat (when add-headers [(mapv name headers)])
                             (map (apply juxt headers) tasks))))))

(defn pull-all-task-ents
  "Returns a seq of task entities from the db"
  [mesos-db]
  (->> (d/q '[:find [?i ...]
              :where
              [?i :instance/task-id _]]
            mesos-db)
       (map (partial d/entity mesos-db))))

(defn pull-all-task-ents-completed-in-time-range
  "Returns a seq of task entities from the db for the specified time range."
  [mesos-db start-time-inc end-time-exc]
  (->> (d/q '[:find [?i ...]
              :in $ ?st ?et
              :where
              [?i :instance/task-id _]
              [?i :instance/end-time ?t]
              [(>= ?t ?st)]
              [(< ?t ?et)]]
            mesos-db start-time-inc end-time-exc)
       (map (partial d/entity mesos-db))))

(defn submit-job
  "Submits a job to datomic"
  [conn job]
  (let [job-keys [:job/command :job/disable-mea-culpa-retries
                  :job/max-retries :job/max-runtime
                  :job/name :job/priority :job/resource
                  :job/user :job/uuid :job/expected-runtime]]
    (let [runtime-label-id (d/tempid :db.part/user)
          runtime-env {:db/id runtime-label-id
                       :label/key "JOB-RUNTIME"
                       :label/value (str (:run-time-ms job))}
          status-label-id (d/tempid :db.part/user)
          status-env {:db/id status-label-id
                      :label/key "JOB-STATUS"
                      :label/value (get job :status "finished")}
          commit-latch-id (d/tempid :db.part/user)
          commit-latch {:db/id commit-latch-id
                        :commit-latch/committed? true}
          group-uuid (:job/group job)
          job-id (d/tempid :db.part/user)
          group (when group-uuid
                  [{:db/id (d/tempid :db.part/user)
                    :group/uuid (java.util.UUID/fromString group-uuid)
                    :group/job job-id}])
          txn [runtime-env
               status-env
               commit-latch
               (-> (select-keys job job-keys)
                   (assoc :db/id job-id
                          :job/commit-latch commit-latch-id
                          :job/custom-executor false
                          :job/label [status-label-id runtime-label-id]
                          :job/state :job.state/waiting
                          :job/submit-time (tc/to-date (t/now)))
                   (update :job/uuid #(java.util.UUID/fromString %))
                   (update :job/command #(or % ""))
                   (update :job/name #(or % ""))
                   (update :job/max-runtime #(int (or % (-> 7 t/days t/in-millis)))))]
          txn (concat txn group)]
      @(d/transact conn txn))))

(defn task->runtime-ms
  "Function passed into the mesos mock to get the runtime of a given task"
  [task]
  (->> task
       :labels
       :labels
       (filter #(= (:key %) "JOB-RUNTIME"))
       first
       :value
       read-string))

(defn task->complete-status
  "Function passed into the mesos mock to get the completion status of the task"
  [task]
  (->> task
       :labels
       :labels
       (filter #(= (:key %) "JOB-STATUS"))
       first
       :value
       (str "task-")
       (keyword)))

(defn sorted-order?
  "Returns true if the coll is sorted, false otherwise"
  [coll]
  (= coll (sort coll)))

(defn- write-completed-tasks
  "Writes completed tasks between start-time and current-time to the temp-out-trace-file."
  [mesos-db time-ms-between-incremental-output temp-out-trace-file simulation-start-time start-time current-time]
  (log/info "Starting temporary output iteration" start-time "to" current-time)
  (let [start-date (Date. ^long start-time)
        end-date (Date. ^long current-time)
        completed-task-ents (pull-all-task-ents-completed-in-time-range mesos-db start-date end-date)]
    (if (seq completed-task-ents)
      (do
        (log/info "Writing" (count completed-task-ents) "tasks to temporary output for simulation time" current-time)
        (dump-jobs-to-csv completed-task-ents temp-out-trace-file
                          :add-headers (= start-time simulation-start-time)
                          :append true))
      (log/info "No completed tasks in last" time-ms-between-incremental-output "ms"))))

;; TODO need way of setting share
(defn simulate
  "Starts cook scheduler connected to a mock of mesos and submits jobs in the
   trace between:
    (simulation-time, simulation-time+cycle-step-ms]
   The start simulation time is the min submit time in the trace.

   Returns a list of the task entities run"
  [mesos-hosts trace cycle-step-ms config schedulers-config temp-out-trace-file]
  (let [simulation-time (-> trace first :submit-time-ms)
        mesos-datomic-conn (restore-fresh-database! (get config :datomic-url "datomic:mem://mock-mesos"))
        offer-trigger-chan (async/chan)
        complete-trigger-chan (async/chan)
        ranker-trigger-chan (async/chan)
        matcher-trigger-chan (async/chan)
        rebalancer-trigger-chan (async/chan)
        optimizer-trigger-chan (async/chan)
        state-atom (atom {})
        make-mesos-driver-fn (fn [config scheduler framework-id]
                               (mm/mesos-mock mesos-hosts offer-trigger-chan scheduler
                                              :task->runtime-ms task->runtime-ms
                                              :task->complete-status task->complete-status
                                              :complete-trigger-chan complete-trigger-chan
                                              :state-atom state-atom))
        config (merge {:shares [{:user "default" :mem 4000.0 :cpus 4.0 :gpus 1.0}]
                       :time-ms-between-incremental-output (-> 10 t/minutes t/in-millis)
                       :time-ms-between-optimizer-calls (-> 3 t/minutes t/in-millis)
                       :time-ms-between-rebalancing (-> 30 t/minutes t/in-millis)}
                      config)
        opt-config {:host-feed {:create-fn 'cook.scheduler.optimizer/create-dummy-host-feed
                                :config {}}
                    :optimizer {:create-fn 'cook.scheduler.optimizer/create-dummy-optimizer
                                :config {}}}
        scheduler-config (merge (:scheduler-config config)
                                {:optimizer-config opt-config}
                                {:trigger-chans {:rank-trigger-chan ranker-trigger-chan
                                                 :match-trigger-chan matcher-trigger-chan
                                                 :rebalancer-trigger-chan rebalancer-trigger-chan
                                                 :optimizer-trigger-chan optimizer-trigger-chan
                                                 ;; Don't care about these yet
                                                 :progress-updater-trigger-chan (async/chan)
                                                 :straggler-trigger-chan (async/chan)
                                                 :lingering-task-trigger-chan (async/chan)
                                                 :cancelled-task-trigger-chan (async/chan)}})
        {:keys [time-ms-between-incremental-output time-ms-between-optimizer-calls time-ms-between-rebalancing]} config]
    ;; launch a thread to perform incremental writes of completed jobs
    (when temp-out-trace-file
      (let [last-start-time-atom (atom simulation-time)
            write-thread (Thread.
                           ^Runnable
                           (fn write-completed-tasks-runner []
                             (while true
                               (Thread/sleep time-ms-between-incremental-output)
                               (try
                                 (let [start-time @last-start-time-atom
                                       current-time (.getMillis (t/now))
                                       mesos-db (d/db mesos-datomic-conn)]
                                   (write-completed-tasks
                                     mesos-db time-ms-between-incremental-output temp-out-trace-file
                                     simulation-time start-time current-time)
                                   (reset! last-start-time-atom current-time))
                                 (catch Throwable ex
                                   (log/error ex "Error writing incremental completed job output!"))))))]
        (log/info "Temporary output will be updated every" time-ms-between-incremental-output "ms")
        (.setDaemon write-thread true)
        (.start write-thread)))
    ;; We are setting time to enable us to have deterministic runs
    ;; of the simulator while hooking into the scheduler as non-invasively
    ;; as possible. A longer explanation can be found in the simulator dev docs.
    (DateTimeUtils/setCurrentMillisFixed simulation-time)
    (log/info "Starting simulation at" simulation-time)
    ;; launch the simulator
    (with-cook-scheduler
      mesos-datomic-conn
      make-mesos-driver-fn
      scheduler-config
      false
      schedulers-config
      (try
        (doseq [{:keys [user mem cpus gpus]} (:shares config)]
          (share/set-share! mesos-datomic-conn user nil "simulation" :mem mem :cpus cpus :gpus gpus))
        (loop [trace trace
               simulation-time simulation-time
               time-ms-since-last-rebalancer 0
               time-ms-since-last-optimizer-call 0]

          (DateTimeUtils/setCurrentMillisFixed simulation-time)

          (let [start-ms (System/currentTimeMillis)
                submission-batch (take-while #(<= (:submit-time-ms %) simulation-time) trace)
                send-offers-complete-chan (async/chan)
                flush-complete-chan (async/chan)
                match-complete-chan (async/chan)
                rank-complete-chan (async/chan)
                optimizer-complete-chan (async/chan)
                rebalancer-complete-chan (async/chan)]
            (when-not (sorted-order? (map :submit-time-ms submission-batch))
              (throw (ex-info "Trace jobs are expected to be sorted by submit-time-ms"
                              {:simulation-time simulation-time
                               :first-100-times (vec (take 100 (map :submit-time-ms submission-batch)))})))
            (log/info "Simulation time: " simulation-time)
            (log/info "submission-batch size: " (count submission-batch))

            (log/info "Submitting batch")
            ;; Submit new jobs
            (doseq [job submission-batch]
              (DateTimeUtils/setCurrentMillisFixed (inc (.getTime (tc/to-date (t/now)))))
              (submit-job mesos-datomic-conn job))
            ;; Ensure peer has acknowledged the new jobs
            (when (seq submission-batch)
              (poll-until #(d/q '[:find ?j .
                                  :in $ ?t
                                  :where
                                  [?j :job/submit-time ?t]]
                                (d/db mesos-datomic-conn)
                                ;; This relies on
                                ;; 1. Time is controlled
                                ;; 2. We increment time for each job submitted
                                (tc/to-date (t/now)))
                          50
                          60000))
            (log/info "Batch submission complete")

            ;; Request for jobs that are complete to have cook be notified
            (log/info "Send completion status to scheduler")
            (DateTimeUtils/setCurrentMillisFixed (inc (.getTime (tc/to-date (t/now)))))
            (async/>!! complete-trigger-chan flush-complete-chan)
            (async/<!! flush-complete-chan)

            ;; Ensure mesos and cook state of running jobs matches
            (poll-until #(= (set (map (comp str :instance/task-id)
                                      (util/get-running-task-ents (d/db mesos-datomic-conn))))
                            (set (map str
                                      (keys (:task-id->task @state-atom)))))
                        50
                        60000
                        #(let [cook-tasks (set (map (comp str :instance/task-id)
                                                    (util/get-running-task-ents (d/db mesos-datomic-conn))))
                               mesos-tasks (set (map str
                                                     (keys (:task-id->task @state-atom))))]
                           {:running-tasks-ents (count cook-tasks)
                            :tasks-in-mesos (count mesos-tasks)
                            :cook-minus-mesos (count (clojure.set/difference cook-tasks mesos-tasks))
                            :mesos-minus-cook (count (clojure.set/difference mesos-tasks cook-tasks))}))
            (log/info "Completion statuses sent")

            ;; Request rank occurs
            (log/info "Starting rank")
            (DateTimeUtils/setCurrentMillisFixed (inc (.getTime (tc/to-date (t/now)))))
            (async/>!! ranker-trigger-chan rank-complete-chan)
            (async/<!! rank-complete-chan)
            (log/info "Rank complete")

            (when (> time-ms-since-last-optimizer-call time-ms-between-optimizer-calls)
              (log/info "Starting optimizer")
              (async/>!! optimizer-trigger-chan optimizer-complete-chan)
              (async/<!! optimizer-complete-chan)
              (log/info "Optimizer complete"))

            ;; Match
            (log/info "Starting match")
            (DateTimeUtils/setCurrentMillisFixed (inc (.getTime (tc/to-date (t/now)))))
            (async/>!! offer-trigger-chan send-offers-complete-chan)
            (async/<!! send-offers-complete-chan)
            (async/>!! matcher-trigger-chan match-complete-chan)
            (async/<!! match-complete-chan)
            ;; Ensure the launch has been processed by mesos
            (poll-until #(every? :instance/mesos-start-time
                                 (util/get-running-task-ents (d/db mesos-datomic-conn)))
                        50
                        60000)
            (log/info "Match complete")

            ;; Rebalance
            (when (> time-ms-since-last-rebalancer time-ms-between-rebalancing)
              (log/info "Starting rebalance")
              (DateTimeUtils/setCurrentMillisFixed (inc (.getTime (tc/to-date (t/now)))))
              (async/>!! rebalancer-trigger-chan rebalancer-complete-chan)
              (async/<!! rebalancer-complete-chan)
              (log/info "Rebalance complete"))

            ;; Periodically perform full gc under hypothesis that holding onto
            ;; lot of memory is causing problems
            (when (> (rand) 0.9)
              (log/warn "Forcing GC")
              (System/gc))

            (when (seq trace)
              (recur (drop (count submission-batch) trace)
                     (+ simulation-time cycle-step-ms)
                     (if (> time-ms-since-last-rebalancer time-ms-between-rebalancing)
                       0
                       (+ time-ms-since-last-rebalancer cycle-step-ms))
                     (if (> time-ms-since-last-optimizer-call time-ms-between-optimizer-calls)
                       0
                       (+ time-ms-since-last-optimizer-call cycle-step-ms))))))
        (println "count of jobs submitted " (count (d/q '[:find ?e
                                                          :where
                                                          [?e :job/uuid _]]
                                                        (d/db mesos-datomic-conn))))
        (pull-all-task-ents (d/db mesos-datomic-conn))

        (catch Throwable t
          (println t) ;; TODO for some reason I'm not seeing exceptions, hoping this helps
          (throw t))))))

(def cli-options
  [[nil "--trace-file TRACE_FILE" "File of jobs to submit"]
   [nil "--host-file HOST_FILE" "File of hosts available in the mesos cluster"]
   [nil "--cycle-step-ms CYCLE_STEP"
    "How much time passes between cycles to move through trace file."
    :parse-fn #(Integer/parseInt %)]
   [nil "--out-trace-file TRACE_FILE" "File to output trace of tasks run"]
   [nil "--config-file CONFIG_FILE" "File in edn format containing config for the simulation"]
   ["-h" "--help"]])

(def required-options
  #{:trace-file :host-file :out-trace-file})

(defn usage
  "Returns a string describing the usage of the simulator"
  [summary]
  (str "lein run -m cook.test.simulator [OPTS]\n" summary))

;; Consider having a simulation config file
(defn -main
  [& args]
  (println "Starting simulation")
  (System/setProperty "COOK.SIMULATION" (str true))
  (init-logger)
  (let [{:keys [options errors summary]} (parse-opts args cli-options)
        {:keys [trace-file host-file cycle-step-ms out-trace-file config-file help]} options]
    (when errors
      (println errors)
      (println (usage summary))
      (System/exit 1))
    (when help
      (println (usage summary))
      (System/exit 0))
    (when (not= (count required-options) (count (select-keys options required-options)))
      (println "Missing required options: "
               (->> options
                    keys
                    set
                    (clojure.set/difference required-options)
                    (map name)))
      (println (usage summary))
      (System/exit 1))
    (log/info "Pulling input files")
    (let [hosts (->> (json/read-str (slurp host-file))
                     keywordize-keys
                     ;; This is needed because we want the roles to be strings
                     (transform [ALL :resources MAP-VALS MAP-KEYS] name))
          config (if config-file
                        (edn/read-string (slurp config-file))
                        {})
          cycle-step-ms (or cycle-step-ms (:cycle-step-ms config))
          _ (when-not cycle-step-ms
              (throw (ex-info "Must configure cycle-step-ms on command line or config file" {})))
          task-ents (simulate hosts
                              (cheshire/parse-stream (clojure.java.io/reader trace-file) true)
                              cycle-step-ms
                              config
                              (get-in config [:settings :pools :schedulers])
                              (str out-trace-file ".temp-" (System/nanoTime)))]
      (println "tasks run: " (count task-ents))
      (dump-jobs-to-csv task-ents out-trace-file)
      (println "Done writing trace")
      (System/exit 0))))

(defn create-trace-job
  "Returns a job that can be used in the trace"
  [run-time-ms submit-time-ms &
   {:keys [user uuid command ncpus memory name retry-count max-runtime priority job-state submit-time custom-executor? gpus group committed?
           disable-mea-culpa-retries]
    :or {user (System/getProperty "user.name")
         uuid (d/squuid)
         committed? true
         command "dummy command"
         ncpus 1.0
         memory 10.0
         name "dummy_job"
         submit-time (java.util.Date.)
         retry-count 5
         max-runtime (* 1000 60 60 24 5)
         priority 50
         job-state :job.state/waiting
         disable-mea-culpa-retries false}}]
  (let [job-info (merge {:job/uuid (str uuid)
                         :job/command command
                         :job/user user
                         :job/name name
                         :job/max-retries retry-count
                         :job/max-runtime max-runtime
                         :job/priority priority
                         :job/disable-mea-culpa-retries disable-mea-culpa-retries
                         :job/resource [{:resource/type :resource.type/cpus
                                         :resource/amount (double ncpus)}
                                        {:resource/type :resource.type/mem
                                         :resource/amount (double memory)}]}
                        {:submit-time-ms submit-time-ms
                         :run-time-ms run-time-ms})
        job-info (if gpus
                   (update-in job-info [:job/resource] conj {:resource/type :resource.type/gpus
                                                             :resource/amount (double gpus)})
                   job-info)]
    job-info))

(defn trace-host
  [host-name mem cpus]
  {:hostname (str host-name)
   :attributes {}
   :resources {:cpus {"*" cpus}
               :mem {"*" mem}
               :ports {"*" [{:begin 1
                             :end 100}]}}
   :slave-id (java.util.UUID/randomUUID)})

(defn summary-stats [jobs]
  (->> jobs
       (map util/job-ent->resources)
       (map #(assoc % :count 1))
       (reduce (partial merge-with +))))

(defn normalize-trace
  "Normalizes the trace so that all times are based off the min submit time"
  [trace]
  (let [min-submit-time (apply min (map :submit_time_ms trace))]
    (->> trace
         (transform [ALL :submit_time_ms] #(- % min-submit-time))
         (transform [ALL :start_time_ms] #(- % min-submit-time))
         (transform [ALL :end_time_ms] #(- % min-submit-time)))))

(defn trace-diffs
  [trace-a trace-b]
  (let [job-trace-fn (fn job-trace-fn [tasks]
                       {:job-id (:job_id (first tasks))
                        :submit-time (:submit_time_ms (first tasks))
                        :start-times (vec (sort (map :start_time_ms tasks)))
                        :tasks tasks})
        ks [:job-id :submit-time :start-times]
        job-trace-a (->> trace-a
                         (map generate-task-trace-map)
                         normalize-trace
                         (group-by :job_id)
                         (map-vals job-trace-fn)
                         vals)
        job-trace-b (->> trace-b
                         (map generate-task-trace-map)
                         normalize-trace
                         (group-by :job_id)
                         (map-vals job-trace-fn)
                         vals)
        joined-trace (group-by :job-id (concat job-trace-a job-trace-b))]
    (->> joined-trace
         (remove (comp #(when (= (count %) 2)
                          (= (select-keys (first %) ks)
                             (select-keys (second %) ks)))
                       second)))))

(defn traces-equivalent?
  [trace-a trace-b]
  (not (seq (trace-diffs trace-a trace-b))))

(deftest test-simulator
  (cook.test.testutil/setup)
  (let [users ["a" "b" "c" "d"]
        jobs (-> (for [minute (range 5)
                       sim-i (range (+ (rand-int 50) 30))]
                   (create-trace-job (+ (rand-int 1200000) 600000) ; 1 to 20 minutes
                                     (+ (* 1000 60 minute) (+ (rand-int 2000) -1000))
                                     :user (first (shuffle users))
                                     :command "sleep 10" ;; Doesn't matter
                                     :custom-executor? false
                                     :memory (+ (rand-int 1000) 2000)
                                     :ncpus (+ (rand-int 3) 1)))
                 (conj (create-trace-job 10000
                                         (-> 1 t/hours t/in-millis)
                                         :user "e"
                                         :command "sleep 10"
                                         :custom-executor? false
                                         :memory 10000
                                         :ncpus 3)))
        jobs (sort-by :submit-time-ms jobs)
        num-hosts 120
        host-mem 20000.0
        host-cpus 20.0
        hosts (for [i (range num-hosts)]
                (trace-host i host-mem host-cpus))
        cycle-step-ms 30000
        config {:shares [{:cpus (/ host-cpus 10) :gpus 1.0 :mem (/ host-mem 10) :user "default"}]
                :scheduler-config {:rebalancer-config {:max-preemption 1.0}}}
        schedulers-config [{:pool-regex ".*"
                            :scheduler-config {:scheduler "fenzo"
                                               :good-enough-fitness 1.0
                                               :fenzo-fitness-calculator default-fitness-calculator
                                               :fenzo-max-jobs-considered 200
                                               :fenzo-scaleback 0.95
                                               :fenzo-floor-iterations-before-warn 10
                                               :fenzo-floor-iterations-before-reset 1000}}]
        out-trace-a (simulate hosts jobs cycle-step-ms config schedulers-config nil)
        out-trace-b (simulate hosts jobs cycle-step-ms config schedulers-config nil)]
    (is (> (count out-trace-a) 0))
    (is (> (count out-trace-b) 0))
    (is (traces-equivalent? out-trace-a out-trace-b)
        {:diffs (sort-by (comp :submit-time first second)
                         (trace-diffs out-trace-a out-trace-b))})))
