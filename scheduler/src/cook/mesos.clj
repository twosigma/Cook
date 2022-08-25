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
(ns cook.mesos
  (:require [chime :refer [chime-ch]]
            [clj-time.core :as time]
            [clojure.core.async :as async]
            [clojure.data :as data]
            [clojure.tools.logging :as log]
            [cook.compute-cluster :as cc]
            [cook.config :as config]
            [cook.datomic :refer [transact-with-retries]]
            [cook.mesos.heartbeat]
            [cook.monitor]
            [cook.prometheus-metrics :as prom]
            [cook.queue-limit :as queue-limit]
            [cook.rebalancer]
            [cook.scheduler.optimizer]
            [cook.scheduler.scheduler :as sched]
            [cook.tools :as tools]
            [cook.util :as util]
            [datomic.api :as d :refer [q]]
            [mesomatic.scheduler]
            [mesomatic.types]
            [metatransaction.utils :as dutils]
            [metrics.counters :as counters]
            [plumbing.core :refer [map-from-keys]]
            [swiss.arrows :refer :all])
  (:import (com.google.common.cache CacheBuilder)
           (java.util.concurrent TimeUnit)
           (org.apache.curator.framework.recipes.leader LeaderSelector LeaderSelectorListener)
           (org.apache.curator.framework.state ConnectionState)))

;; ============================================================================
;; mesos scheduler etc.

(defn submit-to-mesos
  "Takes a sequence of jobs in jobsystem format and submits them to the mesos
   DB."
  ([conn user jobs]
   (submit-to-mesos conn user jobs []))
  ([conn user jobs additional-txns-per-job]
   (log/info "Submitting jobs to mesos" conn)
   (try
     (let [submit-time (java.util.Date.)]
       (doseq [{:keys [uuid command ncpus memory name retry-count priority]} jobs
               :let [txn {:db/id (d/tempid :db.part/user)
                          :job/command command
                          :job/name "cooksim"
                          :job/max-retries retry-count
                          :job/priority priority
                          :job/resource [{:resource/type :resource.type/cpus
                                          :resource/amount (double ncpus)}
                                         {:resource/type :resource.type/mem
                                          :resource/amount (double memory)}]
                          :job/state :job.state/waiting
                          :job/submit-time submit-time
                          :job/user user
                          :job/uuid uuid}
                     retries 5
                     base-wait 500 ; millis
                     opts {:retry-schedule (util/rand-exponential-seq retries base-wait)}]]
         (if (and (<= memory 200000) (<= ncpus 32))
           (dutils/transact-with-retries!! conn opts (into [txn] additional-txns-per-job))
           (log/error "We chose not to schedule the job" uuid
                      "because it required too many resources:" ncpus
                      "cpus and" memory "MB of memory"))))
     (catch Exception e
       (log/error e "Occurred while trying to 'submit to mesos' (transact with mesos datomic)")
       (throw (ex-info "Exception occurred while trying to submit to mesos"
                       {:exception e
                        :conn conn}
                       e))))))

(counters/defcounter [cook-mesos mesos mesos-leader])

(defn make-trigger-chans
  "Creates a map of of the trigger channels expected by `start-leader-selector`
   Each channel receives chime triggers at particular intervals and it is
   possible to send additional events as desired"
  [rebalancer-config progress-config optimizer-config
   {:keys [timeout-interval-minutes]
    :or {timeout-interval-minutes 1}
    :as task-constraints}]
  (let [prepare-trigger-chan (fn prepare-trigger-chan [interval]
                               (let [ch (async/chan (async/sliding-buffer 1))]
                                 (async/pipe (chime-ch (util/time-seq (time/now) interval))
                                             ch)
                                 ch))]
    (cond->
        {:cancelled-task-trigger-chan (prepare-trigger-chan (time/seconds 3))
         :lingering-task-trigger-chan (prepare-trigger-chan (time/minutes timeout-interval-minutes))
         :match-trigger-chan (async/chan (async/sliding-buffer 1))
         :optimizer-trigger-chan (prepare-trigger-chan (time/seconds (:optimizer-interval-seconds optimizer-config 10)))
         :progress-updater-trigger-chan (prepare-trigger-chan (time/millis (:publish-interval-ms progress-config)))
         :rank-trigger-chan (prepare-trigger-chan (time/seconds 5))
         :rebalancer-trigger-chan (prepare-trigger-chan (time/seconds (:interval-seconds rebalancer-config)))
         :straggler-trigger-chan (prepare-trigger-chan (time/minutes timeout-interval-minutes))})))

(defn make-compute-cluster-config-updater-task
  "Create a Runnable that periodically checks for updated compute cluster configurations"
  [conn new-cluster-configurations-fn]
  (fn compute-cluster-config-updater-task [_]
    (try
      (cc/update-compute-clusters conn (new-cluster-configurations-fn) false)
      (catch Exception ex
        (log/error ex "Failed to update cluster configurations")))))

(defn dynamic-compute-cluster-configurations-setup
  "Set up dynamic compute cluster configurations. This includes loading existing configurations from the database
  at startup, and launching a background worker to periodically check for new configurations"
  [conn {:keys [load-clusters-on-startup? cluster-update-period-seconds new-cluster-configurations-fn]}]
  (when load-clusters-on-startup?
    (locking cc/cluster-name->compute-cluster-atom
      (let [db (d/db conn)
            saved-cluster-configurations (cc/get-db-configs db)
            cluster-name->running-task-ents (->> db cook.tools/get-running-task-ents
                                                 (group-by #(-> %
                                                              :instance/compute-cluster
                                                              :compute-cluster/cluster-name)))
            cluster-names-for-running-tasks (set (keys cluster-name->running-task-ents))
            known-cluster-names (set (concat (keys @cc/cluster-name->compute-cluster-atom) (keys saved-cluster-configurations)))
            [missing-cluster-names _ _] (data/diff cluster-names-for-running-tasks known-cluster-names)]
        (when missing-cluster-names
          (log/error "Can't find cluster configurations for some of the running jobs!"
                     {:missing-cluster-names missing-cluster-names
                      :cluster-name->instance-ids (->> missing-cluster-names
                                                       (map-from-keys
                                                         #(->> (take 20 (cluster-name->running-task-ents %))
                                                               (map :instance/task-id))))}))
        (cc/update-compute-clusters-helper conn db saved-cluster-configurations saved-cluster-configurations false))))
  (when new-cluster-configurations-fn
    (let [cluster-update-period-seconds (or cluster-update-period-seconds 60)]
      (chime/chime-at
        (util/time-seq (time/plus (time/now) (time/seconds cluster-update-period-seconds))
                       (time/seconds cluster-update-period-seconds))
        (make-compute-cluster-config-updater-task conn (util/lazy-load-var new-cluster-configurations-fn))
        {:error-handler (fn compute-cluster-config-updater-error-handler [ex]
                          (log/error ex "Failed to update cluster configurations"))}))))

(defn start-leader-selector
  "Starts a leader elector. When the process is leader, it starts the mesos
   scheduler and associated threads to interact with mesos.

   Parameters
   make-mesos-driver-fn          -- fn, function that accepts a mesos scheduler and framework id
                                    and returns a mesos driver
   curator-framework             -- curator object, object for interacting with zk
   mesos-datomic-conn            -- datomic conn, connection to datomic db for interacting with datomic
   mesos-datomic-mult            -- async channel, feed of db writes
   zk-prefix                     -- str, prefix in zk for cook data
   offer-incubate-time-ms        -- long, time in millis that offers are allowed to sit before they are declined
   mea-culpa-failure-limit       -- long, max failures of mea culpa reason before it is considered a 'real' failure
                                    see scheduler/docs/configuration.adoc for more details
   task-constraints              -- map, constraints on task. See scheduler/docs/configuration.adoc for more details
   pool-name->pending-jobs-atom  -- atom, Populate (and update) map from pool name to list of pending jobs into atom
   gpu-enabled?                  -- boolean, whether cook will schedule gpus
   rebalancer-config             -- map, config for rebalancer. See scheduler/docs/rebalancer-config.adoc for details
   progress-config               -- map, config for progress publishing. See scheduler/docs/configuration.adoc
   framework-id                  -- str, the Mesos framework id from the cook settings
   sandbox-syncer-state          -- map, representing the sandbox syncer object
   api-only?                     -- bool, true if this instance should not actually join the leader selection"
  [{:keys [curator-framework mea-culpa-failure-limit mesos-datomic-conn mesos-datomic-mult
           mesos-heartbeat-chan leadership-atom pool-name->pending-jobs-atom mesos-run-as-user
           offer-incubate-time-ms optimizer-config rebalancer-config server-config task-constraints trigger-chans
           zk-prefix api-only?]}]
  (let [{:keys [cancelled-task-trigger-chan lingering-task-trigger-chan optimizer-trigger-chan
                rebalancer-trigger-chan straggler-trigger-chan]} trigger-chans
        {:keys [hostname server-port server-https-port]} server-config
        datomic-report-chan (async/chan (async/sliding-buffer 4096))
        cluster-name->compute-cluster @cc/cluster-name->compute-cluster-atom
        rebalancer-reservation-atom (atom {})
        _ (log/info "Starting leader selection" :api-only? api-only?)
        _ (log/info "Using path" zk-prefix "for leader selection")
        leader-selector (LeaderSelector.
                          curator-framework
                          zk-prefix
                          ;(ThreadUtils/newThreadFactory "mesos-leader-selector")
                          ;clojure.lang.Agent/pooledExecutor
                          (reify LeaderSelectorListener
                            (takeLeadership [_ client]
                              (let [agent-attributes-cache
                                    (-> (CacheBuilder/newBuilder)
                                        ;; When we store in this cache, we only visit nodes with offers.
                                        ;; When we check it in the rebalancer, we only visit nodes that have
                                        ;; running jobs on them.
                                        ;; So, set a 2 hour timeout; if we've not seen a node in 2 hours, or our
                                        ;; offer processing is *that* slow, losing the state won't make things any worse.
                                        (.expireAfterAccess 2 TimeUnit/HOURS)
                                        (.expireAfterWrite 2 TimeUnit/HOURS)
                                        ; *ALWAYS* prevent runaway.
                                        (.maximumSize 1000000)
                                        (.build))
                                    {:keys [pool-name->fenzo-state view-incubating-offers] :as scheduler}
                                    (sched/create-datomic-scheduler
                                      {:conn mesos-datomic-conn
                                       :cluster-name->compute-cluster-atom cook.compute-cluster/cluster-name->compute-cluster-atom
                                       :mea-culpa-failure-limit mea-culpa-failure-limit
                                       :mesos-run-as-user mesos-run-as-user
                                       :agent-attributes-cache agent-attributes-cache
                                       :offer-incubate-time-ms offer-incubate-time-ms
                                       :pool-name->pending-jobs-atom pool-name->pending-jobs-atom
                                       :rebalancer-reservation-atom rebalancer-reservation-atom
                                       :task-constraints task-constraints
                                       :trigger-chans trigger-chans})]
                                ; we need to make sure to initialize cc/pool-name->fenzo-state-atom before we take leadership
                                ; after we take leadership, we should be able to create dynamic clusters, so cc/pool-name->fenzo-state-atom
                                ; needs to be set
                                (reset! cc/pool-name->fenzo-state-atom (:pool-name->fenzo-state scheduler))
                                (dynamic-compute-cluster-configurations-setup mesos-datomic-conn (config/compute-cluster-options))
                                (log/warn "Taking leadership")
                                (reset! leadership-atom true)
                                ;; TODO: get the framework ID and try to reregister
                                (let [normal-exit (atom true)]
                                  (try
                                    (let [cluster-connected-chans (locking cc/cluster-name->compute-cluster-atom
                                                                    (->> cluster-name->compute-cluster
                                                                         (remove (fn [[_ compute-cluster]] (:dynamic-cluster-config? compute-cluster)))
                                                                         (map (fn [[compute-cluster-name compute-cluster]]
                                                                                (try
                                                                                  (cc/initialize-cluster compute-cluster
                                                                                                         pool-name->fenzo-state)
                                                                                  (catch Throwable t
                                                                                    (log/error t "Error launching compute cluster" compute-cluster-name)
                                                                                    ; Return a chan that never gets a message on it.
                                                                                    (async/chan 1)))))
                                                                         ; Note: This doall has a critical side effect of actually initializing
                                                                         ; all of the clusters.
                                                                         doall))]
                                      (cook.monitor/start-collecting-stats)
                                      ; Many of these should look at the compute-cluster of the underlying jobs, and not use driver at all.
                                      (cook.scheduler.scheduler/lingering-task-killer mesos-datomic-conn
                                                                                      task-constraints lingering-task-trigger-chan)
                                      (cook.scheduler.scheduler/straggler-handler mesos-datomic-conn straggler-trigger-chan)
                                      (cook.scheduler.scheduler/cancelled-task-killer mesos-datomic-conn
                                                                                      cancelled-task-trigger-chan)
                                      (cook.mesos.heartbeat/start-heartbeat-watcher! mesos-datomic-conn mesos-heartbeat-chan)
                                      (cook.rebalancer/start-rebalancer! {:config rebalancer-config
                                                                          :conn mesos-datomic-conn
                                                                          :agent-attributes-cache agent-attributes-cache
                                                                          :pool-name->pending-jobs-atom pool-name->pending-jobs-atom
                                                                          :rebalancer-reservation-atom rebalancer-reservation-atom
                                                                          :trigger-chan rebalancer-trigger-chan
                                                                          :view-incubating-offers view-incubating-offers})
                                      (when (seq optimizer-config)
                                        (cook.scheduler.optimizer/start-optimizer-cycles! (fn get-queue []
                                                                                            ;; TODO Use filter of queue that scheduler uses to filter to considerable.
                                                                                            ;;      Specifically, think about filtering to jobs that are waiting and
                                                                                            ;;      think about how to handle quota
                                                                                            @pool-name->pending-jobs-atom)
                                                                                          (fn get-running []
                                                                                            (tools/get-running-task-ents (d/db mesos-datomic-conn)))
                                                                                          view-incubating-offers
                                                                                          optimizer-config
                                                                                          optimizer-trigger-chan))
                                      ; This counter exists so that the cook leader has a '1' and the others have a '0'.
                                      ; So we can see who the leader is in the graphs by graphing the value.
                                      (prom/set prom/is-leader 1)
                                      (counters/inc! mesos-leader)
                                      (async/tap mesos-datomic-mult datomic-report-chan)
                                      (cook.scheduler.scheduler/monitor-tx-report-queue datomic-report-chan mesos-datomic-conn)
                                      ; Curator expects takeLeadership to block until voluntarily surrendering leadership.
                                      ; We need to block here until we're willing to give up leadership.
                                      ;
                                      ; We block until any of the cluster-connected-chans unblock. This happens when the compute cluster
                                      ; loses connectivity to the backend. For now, we want to treat mesos as special. When we lose our mesos
                                      ; driver connection to the backend, we want cook to suicide. If we lose any of our kubernetes connections
                                      ; we ignore it and work with the remaining cluster.
                                      ;
                                      ; WARNING: This code is very misleading. It looks like we'll suicide if ANY of the clusters lose leadership.
                                      ; However, the kubernetes compute clusters never put anything on their chan, so this is the equivalent of only looking at mesos.
                                      ; We didn't want to implement the special case for mesos.
                                      (let [res (async/<!! (async/merge (or (seq cluster-connected-chans) [(async/chan 1)])))]
                                        (when (instance? Throwable res)
                                          (throw res))))
                                    (catch Throwable e
                                      (log/error e "Lost leadership due to exception")
                                      (reset! normal-exit false))
                                    (finally
                                      (prom/set prom/is-leader 0)
                                      (counters/dec! mesos-leader)
                                      (when @normal-exit
                                        (log/warn "Lost leadership naturally"))
                                      ;; Better to fail over and rely on start up code we trust then rely on rarely run code
                                      ;; to make sure we yield leadership correctly (and fully)
                                      (log/fatal "Lost leadership. Exiting. Expecting a supervisor to restart me!")
                                      (System/exit 0))))))
                            (stateChanged [_ client newState]
                              ;; We will give up our leadership whenever it seems that we lost
                              ;; ZK connection
                              (when (#{ConnectionState/LOST ConnectionState/SUSPENDED} newState)
                                (when @leadership-atom
                                  (prom/set prom/is-leader 0)
                                  (counters/dec! mesos-leader)
                                  ;; Better to fail over and rely on start up code we trust then rely on rarely run code
                                  ;; to make sure we yield leadership correctly (and fully)
                                  (if (-> "COOK.SIMULATION" System/getProperty str Boolean/parseBoolean)
                                    (log/warn "Lost leadership in zookeeper. Not exiting as simulation is running.")
                                    (do
                                      (log/fatal "Lost leadership in zookeeper. Exiting. Expecting a supervisor to restart me!")
                                      (System/exit 0))))))))]
    (.setId leader-selector (str hostname \#
                                 (or server-port server-https-port) \#
                                 (if server-port "http" "https") \#
                                 (java.util.UUID/randomUUID)))
    (if api-only?
      ; If this is an api-only instance, we still want all the leader election configuration to be set,
      ; without actually joing the election process.
      ; This allows the instance to correctly forward client requests to the leader as necessary.
      (log/info "Will not join leader election, running as api-only")
      (do
        (.autoRequeue leader-selector)
        (.start leader-selector)
        (log/info "Started the leader selector")))
    {:submitter (partial submit-to-mesos mesos-datomic-conn)
     :leader-selector leader-selector}))


(defn kill-job
  "Kills jobs. It works by marking them completed, which will trigger the subscription
   monitor to attempt to kill any instances"
  [conn job-uuids]
  (when (seq job-uuids)
    (log/info "Killing some jobs!!")

    ; Decrement the queue lengths that are
    ; used for queue limiting purposes
    (let [db (d/db conn)
          jobs
          (map
            (fn [job-uuid]
              (d/entity
                db
                [:job/uuid job-uuid]))
            job-uuids)
          pending-jobs
          (filter
            (fn [job]
              (= (tools/job-ent->state job)
                 "waiting"))
            jobs)]
      (queue-limit/dec-queue-length! pending-jobs))

    ; Transact the state changes
    (doseq [uuids (partition-all 50 job-uuids)]
      (async/<!!
        (transact-with-retries conn
                               (mapv
                                 (fn [job-uuid]
                                   [:db/add [:job/uuid job-uuid] :job/state :job.state/completed])
                                 uuids)
                               (into (repeat 10 500) (repeat 10 1000)))))))

(defn kill-instances
  "Kills instances.  Marks them as cancelled in datomic;
  the cancelled-task-killer will notice this and kill the actual Mesos tasks."
  [conn task-uuids]
  (let [txns (mapv (fn [task-uuid]
                     [:db/add
                      [:instance/task-id (str task-uuid)]
                      :instance/cancelled true])
                   task-uuids)]
    @(d/transact conn txns)))
