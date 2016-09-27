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
  (:require [clojure.core.async :as async]
            [datomic.api :as d :refer (q)]
            [metatransaction.core :as mt :refer (db)]
            [metatransaction.utils :as dutils]
            [clojure.tools.logging :as log]
            [mesomatic.scheduler]
            [mesomatic.types]
            [metrics.counters :as counters]
            [cook.datomic :refer (transact-with-retries)]
            [cook.mesos.scheduler :as sched]
            [cook.mesos.heartbeat]
            [cook.mesos.rebalancer]
            [cook.mesos.monitor]
            [cook.util]
            [cook.curator :as curator])
  (:import [org.apache.curator.framework.recipes.leader LeaderSelector LeaderSelectorListener]
           org.apache.curator.framework.state.ConnectionState))

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
                          :job/uuid uuid
                          :job/command command
                          :job/name "cooksim"
                          :job/user user
                          :job/max-retries retry-count
                          :job/priority priority
                          :job/submit-time submit-time
                          :job/state :job.state/waiting
                          :job/resource [{:resource/type :resource.type/cpus
                                          :resource/amount (double ncpus)}
                                         {:resource/type :resource.type/mem
                                          :resource/amount (double memory)}]}
                     retries 5
                     base-wait 500 ; millis
                     opts {:retry-schedule (cook.util/rand-exponential-seq retries base-wait)}]]
         (if (and (<= memory 200000) (<= ncpus 32))
           (dutils/transact-with-retries!! conn opts (concat [txn] additional-txns-per-job))
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

(defn start-mesos-scheduler
  "Starts a leader elector that runs a mesos."
  [mesos-master mesos-master-hosts curator-framework mesos-datomic-conn mesos-datomic-mult zk-prefix mesos-failover-timeout mesos-principal mesos-role offer-incubate-time-ms fenzo-max-jobs-considered fenzo-scaleback fenzo-floor-iterations-before-warn fenzo-floor-iterations-before-reset task-constraints riemann-host riemann-port mesos-pending-jobs-atom dru-scale gpu-enabled? rebalancer-config]
  (let [zk-framework-id (str zk-prefix "/framework-id")
        datomic-report-chan (async/chan (async/sliding-buffer 4096))
        mesos-heartbeat-chan (async/chan (async/buffer 4096))
        current-driver (atom nil)
        {:keys [scheduler view-incubating-offers]}
        (sched/create-datomic-scheduler
                   mesos-datomic-conn
                   (fn set-or-create-framework-id [framework-id]
                     (curator/set-or-create
                      curator-framework
                      zk-framework-id
                      (.getBytes (-> framework-id mesomatic.types/pb->data :value) "UTF-8")))
                   current-driver
                   mesos-pending-jobs-atom
                   mesos-heartbeat-chan
                   offer-incubate-time-ms
                   fenzo-max-jobs-considered
                   fenzo-scaleback
                   fenzo-floor-iterations-before-warn
                   fenzo-floor-iterations-before-reset
                   task-constraints
                   gpu-enabled?)
        framework-id (when-let [bytes (curator/get-or-nil curator-framework zk-framework-id)]
                       (String. bytes))
        leader-selector (LeaderSelector.
                          curator-framework
                          zk-prefix
                          ;(ThreadUtils/newThreadFactory "mesos-leader-selector")
                          ;clojure.lang.Agent/pooledExecutor
                          (reify LeaderSelectorListener
                            (takeLeadership [_ client]
                              (log/warn "Taking mesos leadership")
                              ;; TODO: get the framework ID and try to reregister
                              (let [normal-exit (atom true)
                                    shutdown-hooks (atom ())]
                                (try
                                  (let [driver (apply mesomatic.scheduler/scheduler-driver
                                                      scheduler
                                                      (merge
                                                        {:user ""
                                                         :name (str "Cook-" cook.util/version)
                                                         :checkpoint true}
                                                        (when mesos-role
                                                          {:role mesos-role})
                                                        (when mesos-principal
                                                          {:principal mesos-principal})
                                                        (when gpu-enabled?
                                                          {:capabilities [{:type :framework-capability-gpu-resources}]})
                                                        (when mesos-failover-timeout
                                                          {:failover-timeout mesos-failover-timeout})
                                                        (when framework-id
                                                          {:id framework-id}))
                                                      mesos-master
                                                      (when mesos-principal
                                                        [{:principal mesos-principal}]))]
                                    (mesomatic.scheduler/start! driver)
                                    (reset! current-driver driver)
                                    (if riemann-host
                                      (swap! shutdown-hooks conj (cook.mesos.monitor/riemann-reporter mesos-datomic-conn :riemann-host riemann-host :riemann-port riemann-port)))
                                    #_(swap! shutdown-hooks conj (cook.mesos.scheduler/reconciler mesos-datomic-conn driver))
                                    (swap! shutdown-hooks conj (cook.mesos.scheduler/lingering-task-killer mesos-datomic-conn driver task-constraints))
                                    (swap! shutdown-hooks conj (cook.mesos.heartbeat/start-heartbeat-watcher! mesos-datomic-conn mesos-heartbeat-chan))
                                    (swap! shutdown-hooks conj (cook.mesos.rebalancer/start-rebalancer! {:conn  mesos-datomic-conn
                                                                                                         :driver driver
                                                                                                         :mesos-master-hosts mesos-master-hosts
                                                                                                         :pending-jobs-atom mesos-pending-jobs-atom
                                                                                                         :dru-scale dru-scale
                                                                                                         :config rebalancer-config
                                                                                                         :view-incubating-offers view-incubating-offers}))
                                    (counters/inc! mesos-leader)
                                    (async/tap mesos-datomic-mult datomic-report-chan)
                                    (let [kill-monitor (cook.mesos.scheduler/monitor-tx-report-queue datomic-report-chan mesos-datomic-conn current-driver)]
                                      (swap! shutdown-hooks conj (fn []
                                                                   (kill-monitor)
                                                                   (async/untap mesos-datomic-mult datomic-report-chan))))
                                    (mesomatic.scheduler/join! driver)
                                    (reset! current-driver nil))
                                  (catch Exception e
                                    (log/error e "Lost mesos leadership due to exception")
                                    (reset! normal-exit false))
                                  (finally
                                    (counters/dec! mesos-leader)
                                    (doseq [hook @shutdown-hooks]
                                      (try
                                        (hook)
                                        (catch Exception e
                                          (log/error e "Hook shutdown error"))))
                                    (when @normal-exit
                                      (log/warn "Lost mesos leadership naturally"))))))
                            (stateChanged [_ client newState]
                              ;; We will give up our leadership whenever it seems that we lost
                              ;; ZK connection
                              (when (#{ConnectionState/LOST ConnectionState/SUSPENDED} newState)
                                (when-let [driver @current-driver]
                                  (log/warn "Forfeiting mesos leadership")
                                  (mesomatic.scheduler/stop! driver true))))))]
    (.setId leader-selector (str (java.net.InetAddress/getLocalHost) \- (java.util.UUID/randomUUID)))
    (.autoRequeue leader-selector)
    (.start leader-selector)
    (log/info "Started the mesos leader selector")
    {:submitter (partial submit-to-mesos mesos-datomic-conn)
     :driver current-driver
     :leader-selector leader-selector
     :framework-id framework-id}))

(defn kill-job
  "Kills jobs. It works by marking them completed, which will trigger the subscription
   monitor to attempt to kill any instances"
  [conn job-uuids]
  (when (seq job-uuids)
    (log/info "Killing some jobs!!")
    (doseq [uuids (partition-all 50 job-uuids)]
      (async/<!!
        (transact-with-retries conn
                               (mapv
                                 (fn [job-uuid]
                                   [:db/add [:job/uuid job-uuid] :job/state :job.state/completed])
                                 uuids)
                               (concat (repeat 10 500) (repeat 10 1000)))))))
