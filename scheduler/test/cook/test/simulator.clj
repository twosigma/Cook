(ns cook.test.simulator
  (:use clojure.test)
  (:require [cheshire.core :refer (generate-string)]
            [chime :refer [chime-ch]]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]
            [clj-time.periodic :as periodic]
            [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clojure.edn :as edn]
            [clojure.data.csv :as csv]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.tools.cli :refer  [parse-opts]]
            [clojure.tools.logging :as log]
            [clojure.walk :refer (keywordize-keys)]
            [com.rpl.specter :refer (transform ALL MAP-VALS MAP-KEYS select FIRST)]
            [cook.components :refer (init-logger)]
            [cook.mesos :as c]
            [cook.mesos.mesos-mock :as mm]
            [cook.mesos.share :as share]
            [cook.mesos.scheduler :as sched]
            [cook.mesos.util :as util]
            [cook.test.testutil :refer (restore-fresh-database! poll-until)]
            [datomic.api :as d]
            [mesomatic.scheduler :as mesos]
            [mesomatic.types :as mesos-types]
            [plumbing.core :refer (map-vals map-keys map-from-vals)])
  (:import org.apache.curator.framework.CuratorFrameworkFactory
           org.apache.curator.framework.state.ConnectionStateListener
           org.apache.curator.retry.BoundedExponentialBackoffRetry
           org.joda.time.DateTimeUtils)
  (:gen-class))

;;; This namespace contains a simulator for cook scheduler that accepts a trace file
;;; (defined later on) of jobs to "run" through the simulation as well as how much
;;; faster to run through time (e.g. 2x faster). The output is a "run trace" which
;;; is a csv with the following fields:
;;;  [job_uuid, task_uuid, submit_time, start_time, end_time, host_name, slave_id, status]

;;; Job trace must have the following keys per job:
;;; [:submit-time-ms :run-time-ms :job/uuid :job/command :job/user :job/name :job/max-retries
;;;  :job/max-runtime :job/priority :job/disable-mea-culpa-retries :job/resource]

;;TODO provide a way of getting utilization from mesos mock

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
                                :min-utilization-threshold 0.0
                                :safe-dru-threshold 1.0
                                :min-dru-diff 0.5
                                :max-preemption 100.0})

(def default-fenzo-config {:fenzo-max-jobs-considered 2000
                           :fenzo-scaleback 0.95
                           :fenzo-floor-iterations-before-warn 10
                           :fenzo-floor-iterations-before-reset 1000
                           :good-enough-fitness 0.8})

(def default-task-constraints {:timeout-hours 1
                               :timeout-interval-minutes 1
                               :memory-gb 140
                               :cpus 40
                               :retry-limit 5})

(defmacro with-cook-scheduler
  [conn make-mesos-driver-fn scheduler-config & body]
  `(let [[zookeeper-server# curator-framework#] (setup-test-curator-framework)
         mesos-mult# (or (:mesos-datomic-mult ~scheduler-config)
                         (async/mult (async/chan)))
         pending-jobs-atom# (or (:mesos-pending-jobs-atom ~scheduler-config)
                                (atom []))
         offer-cache# (or (:offer-cache ~scheduler-config)
                          (-> {}
                              (cache/fifo-cache-factory :threshold 100)
                              (cache/ttl-cache-factory :ttl 10000)
                              atom))
         trigger-chans# (or (:trigger-chans ~scheduler-config)
                           {:ranker-trigger-chan (chime-ch (periodic/periodic-seq (t/now) (t/seconds 5)))
                            :matcher-trigger-chan (chime-ch (periodic/periodic-seq (t/now) (t/seconds 1)))
                            :rebalancer-trigger-chan (chime-ch (periodic/periodic-seq (t/now) (t/seconds 5)))
                            })
         get-mesos-utilization# (or (:get-mesos-utilization ~scheduler-config)
                                    (fn [] 0.9) ; get this from mesos-mock
                                    )
         zk-prefix# (or (:zk-prefix ~scheduler-config)
                        "/cook")
         offer-incubate-time-ms# (or (:offer-incubate-time-ms ~scheduler-config)
                                    1000)
         mea-culpa-failure-limit# (or (:mea-culpa-failure-limit ~scheduler-config)
                                      5)
         task-constraints# (merge default-task-constraints (:task-constraints ~scheduler-config))
         riemann-host# (:riemann-host ~scheduler-config)
         riemann-port# (:riemann-port ~scheduler-config)
         gpu-enabled?# (or (:gpus-enabled? ~scheduler-config) false)
         rebalancer-config# (merge default-rebalancer-config (:rebalancer-config ~scheduler-config))
         fenzo-config# (merge default-fenzo-config (:fenzo-config ~scheduler-config))]
     (try
       (let [scheduler# (c/start-mesos-scheduler ~make-mesos-driver-fn get-mesos-utilization#
                                                 curator-framework# ~conn
                                                 mesos-mult# zk-prefix#
                                                 offer-incubate-time-ms# mea-culpa-failure-limit#
                                                 task-constraints# riemann-host# riemann-port#
                                                 pending-jobs-atom# offer-cache#
                                                 gpu-enabled?# rebalancer-config# fenzo-config#
                                                 trigger-chans#)]
         ~@body)
       (finally
         (.close curator-framework#)
         (.stop zookeeper-server#)
         ;; TODO: Ensure cook scheduler shuts down fully
         ))))

(defn dump-jobs-to-csv
  "Given a mesos db, dump a csv with a row per task

   Columns:
   * job_id
   * instance_id
   * submit_time
   * mesos_start_time
   * start_time
   * end_time
   * hostname
   * slave_id
   * status
   * reason
   * user
   * mem
   * cpus
   * job-name
   * requested-run-time
   * requested-status"
  [task-ents file]
  ;; Use snake case to make it easier for downstream tools to consume
  (let [headers [:job_id :instance_id :submit_time_ms :mesos_start_time_ms :start_time_ms
                 :end_time_ms :hostname :slave_id :status :reason :user :mem :cpus :job_name
                 :requested_run_time :requested_status]
        tasks (for [task task-ents
                    :let [job (:job/_instance task)
                          resources (util/job-ent->resources job)]]
                {:job_id  (str (:job/uuid job))
                 :instance_id  (:instance/task-id task)
                 :submit_time_ms  (.getTime (:job/submit-time job))
                 :mesos_start_time_ms  (if (:instance/mesos-start-time task)
                                        (.getTime (:instance/mesos-start-time task))
                                        -1)
                 :start_time_ms  (.getTime (:instance/start-time task))
                 :end_time_ms  (.getTime (or (:instance/end-time task) (tc/to-date (t/now))))
                 :status  (:instance/status task)
                 :hostname  (:instance/hostname task)
                 :slave_id  (:instance/slave-id task)
                 :reason  (or (when (= (:instance/status task) :instance.status/failed)
                               (:reason/string (:instance/reason task)))
                             "")
                 :user  (:job/user job)
                 :mem  (or (:mem resources) -1)
                 :cpus  (or (:cpus resources) -1)
                 :job_name  (or (:job/name job) "")
                 :requested_run_time  (->> (:job/label job)
                                          (filter #(= (:label/key %) "JOB-RUNTIME"))
                                          first
                                          :label/value)
                 :requested_status  (->> (:job/label job)
                                         (filter #(= (:label/key %) "JOB-STATUS"))
                                         first
                                         :label/value)})]
    (with-open [out-file (io/writer file)]
      (csv/write-csv out-file
                     (concat [(mapv name headers)]
                             (map (apply juxt headers) tasks))))))

(defn pull-all-task-ents
  "Returns a seq of task entities from the db"
  [mesos-db]
  (->> (d/q '[:find [?i ...]
         :where
         [?i :instance/task-id _]]
       mesos-db)
       (map (partial d/entity mesos-db))))

(defn submit-job
  "Submits a job to datomic"
  [conn job]
  (let [job-keys [:job/uuid :job/command :job/user :job/name :job/max-retries
                  :job/max-runtime :job/priority :job/disable-mea-culpa-retries
                  :job/resource]]
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
          txn [runtime-env
               status-env
               commit-latch
               (-> (select-keys job job-keys)
                   (assoc :db/id (d/tempid :db.part/user))
                   (update :job/uuid #(java.util.UUID/fromString %))
                   (update :job/command #(or % ""))
                   (update :job/name #(or % ""))
                   (assoc :job/commit-latch commit-latch-id)
                   (assoc :job/submit-time (tc/to-date (t/now)))
                   (assoc :job/state :job.state/waiting)
                   (assoc :job/label [status-label-id runtime-label-id])
                   (assoc :job/custom-executor false)
                   (update :job/max-runtime #(int (or % (-> 7 t/days t/in-millis)) )))]]
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

;; TODO need way of setting share
(defn simulate
  "Starts cook scheduler connected to a mock of mesos and submits jobs in the
   trace between:
    (simulation-time, simulation-time+cycle-step-ms]
   The start simulation time is the min submit time in the trace.

   Returns a list of the task entities run"
  [mesos-hosts trace cycle-step-ms config]
  (let [trace (sort-by :submit-time-ms trace)
        simulation-time (-> trace first :submit-time-ms)
        mesos-datomic-conn (restore-fresh-database! (str "datomic:mem://mock-mesos"))
        offer-trigger-chan (async/chan)
        complete-trigger-chan (async/chan)
        state-atom (atom {})
        make-mesos-driver-fn (fn [scheduler _];; _ is framework-id
                               (mm/mesos-mock mesos-hosts offer-trigger-chan scheduler
                                              :task->runtime-ms task->runtime-ms
                                              :task->complete-status task->complete-status
                                              :complete-trigger-chan complete-trigger-chan
                                              :state-atom state-atom))
        ranker-trigger-chan (async/chan)
        matcher-trigger-chan (async/chan)
        rebalancer-trigger-chan (async/chan)
        initial-time (System/currentTimeMillis)
        config (merge {:shares [{:user "default" :mem 4000.0 :cpus 4.0 :gpus 1.0}]}
                      config)
        scheduler-config (merge (:scheduler-config config)
                                {:trigger-chans {:ranker-trigger-chan ranker-trigger-chan
                                                 :matcher-trigger-chan matcher-trigger-chan
                                                 :rebalancer-trigger-chan rebalancer-trigger-chan}})]
    ;; We are setting time to enable us to have deterministic runs
    ;; of the simulator while hooking into the scheduler as non-invasively
    ;; as possible. A longer explanation can be found in the simulator dev docs.
    (org.joda.time.DateTimeUtils/setCurrentMillisFixed initial-time)
    (log/info "Starting simulation.")
    (with-cook-scheduler mesos-datomic-conn make-mesos-driver-fn
      scheduler-config
      (try
        (doseq [{:keys [user mem cpus gpus]} (:shares config)]
          (share/set-share! mesos-datomic-conn user "simulation" :mem mem :cpus cpus :gpus gpus))
        (loop [trace trace
               simulation-time simulation-time
               fake-real-time initial-time]

          (org.joda.time.DateTimeUtils/setCurrentMillisFixed fake-real-time)

          (let [start-ms (System/currentTimeMillis)
                submission-batch (take-while #(<= (:submit-time-ms %) simulation-time) trace)
                rank-complete-chan (async/chan)
                rebalancer-complete-chan (async/chan)]
            (log/info "Simulation time: " simulation-time)
            (log/info "submission-batch size: " (count submission-batch))

            (log/info "Submitting batch")
            ;; Submit new jobs
            (doseq [job submission-batch]
              (org.joda.time.DateTimeUtils/setCurrentMillisFixed (inc (.getTime (tc/to-date (t/now)))))
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

            ;; Rebalance
            (log/info "Starting rebalance")
            (org.joda.time.DateTimeUtils/setCurrentMillisFixed (inc (.getTime (tc/to-date (t/now)))))
            (async/>!! rebalancer-trigger-chan rebalancer-complete-chan)
            (async/<!! rebalancer-complete-chan)
            (log/info "Rebalance complete")

            ;; Request for jobs that are complete to have cook be notified
            (log/info "Send completion status to scheduler")
            (org.joda.time.DateTimeUtils/setCurrentMillisFixed (inc (.getTime (tc/to-date (t/now)))))
            (async/>!! complete-trigger-chan :trigger)
            (async/>!! complete-trigger-chan :trigger)
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
                           :mesos-minus-cook (count (clojure.set/difference mesos-tasks cook-tasks))})
                       )
            (log/info "Completion statuses sent")

            ;; Request rank occurs
            (log/info "Starting rank")
            (org.joda.time.DateTimeUtils/setCurrentMillisFixed (inc (.getTime (tc/to-date (t/now)))))
            (async/>!! ranker-trigger-chan rank-complete-chan)
            (async/<!! rank-complete-chan)
            (log/info "Rank complete")

            ;; Match
            (log/info "Starting match")
            (org.joda.time.DateTimeUtils/setCurrentMillisFixed (inc (.getTime (tc/to-date (t/now)))))
            (async/>!! offer-trigger-chan :trigger)
            (async/>!! offer-trigger-chan :trigger)
            ;; When scheduler receives offers, it starts a match, this ensures match has occurred
            (async/>!! matcher-trigger-chan :trigger)
            ;; Ensure the launch has been processed by mesos
            (poll-until #(every? :instance/mesos-start-time
                                (util/get-running-task-ents (d/db mesos-datomic-conn)))
                        50
                        60000)
            (log/info "Match complete")

            (when (seq trace)
              (recur (drop (count submission-batch) trace)
                     (+ simulation-time cycle-step-ms)
                     (+ fake-real-time cycle-step-ms)))))
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
    (let [trace (keywordize-keys (json/read-str (slurp trace-file)))
          hosts (->> (json/read-str (slurp host-file))
                     keywordize-keys
                     ;; This is needed because we want the roles to be strings
                     (transform [ALL :resources MAP-VALS MAP-KEYS] name))
          config (if config-file
                   (edn/read-string (slurp config-file))
                   {})
          cycle-step-ms (or cycle-step-ms (:cycle-step-ms config))
          _ (when-not cycle-step-ms
              (throw (ex-info "Must configure cycle-step-ms on command line or config file")))
          task-ents (simulate hosts trace cycle-step-ms config)]
      (println "tasks run: " (count task-ents))
      (dump-jobs-to-csv task-ents out-trace-file)
      (println "Done writing trace")
      (System/exit 0))))

(defn create-trace-job
  "Returns a job that can be used in the trace"
  [run-time-ms submit-time-ms &
   {:keys [user uuid command ncpus memory name retry-count max-runtime priority job-state submit-time custom-executor? gpus group committed?
           disable-mea-culpa-retries ]
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

(defn summary-stats [jobs]
  (->> jobs
       (map util/job-ent->resources)
       (map #(assoc % :count 1))
       (reduce (partial merge-with +))))

(comment

  (def users ["a" "b" "c" "d"])

  (def jobs (for [minute (range 15)
                  sim-i (range (+ (rand-int 100) 20))]
                (create-trace-job (+ (rand-int 1200000) 60000) ; 1 to 20 minutes
                                  (+ (* 1000 60 minute) (+ (rand-int 2000) -1000))
                                  :user (first (shuffle users))
                                  :command "sleep 10" ;; Doesn't matter
                                  :custom-executor? false
                                  :memory (+ (rand-int 2000) 2000)
                                  :ncpus (+ (rand-int 3) 1))))

  (for [_ (range 20)]
    (create-trace-job 10000
                      0
                      :user "a"
                      :command "sleep 10"
                      :custom-executor? false
                      :memory 2000.0
                      :ncpus 2.0))
  (def jobs (concat (for [_ (range 20)]
                      (create-trace-job 10000
                                        0
                                        :user "a"
                                        :command "sleep 10"
                                        :memory 2000.0
                                        :ncpus 2.0))
                    (for [_ (range 5)]
                      (create-trace-job 1000
                                        5000
                                        :user "b"
                                        :command "eco 1000"
                                        :memory 2000.0
                                        :ncpus 2.0))))


  (spit "big-trace-nice.json" (generate-string jobs {:pretty true}))

  (defn trace-host
    [host-name mem cpus]
    {:hostname (str host-name)
     :attributes {}
     :resources {:cpus {"*" cpus}
                 :mem {"*" mem}
                 :ports {"*" [{:begin 1
                               :end 100}]}}
     :slave-id (java.util.UUID/randomUUID)})

  (def hosts (map #(trace-host % 20000 20) (range 30)))



  (spit "medium-hosts.json" (generate-string hosts {:pretty true}))

  )
