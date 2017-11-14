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
(ns cook.mesos.sandbox
  (:require [chime :as chime]
            [clj-http.client :as http]
            [clj-time.core :as time]
            [clj-time.periodic :as periodic]
            [clojure.core.cache :as cache]
            [clojure.tools.logging :as log]
            [datomic.api :as d]
            [metrics.counters :as counters]
            [metrics.histograms :as histograms]
            [metrics.meters :as meters]
            [metrics.timers :as timers]))

(def sandbox-aggregator-message-rate (meters/meter ["cook-mesos" "scheduler" "sandbox-aggregator-message-rate"]))
(def sandbox-aggregator-pending-count (counters/counter ["cook-mesos" "scheduler" "sandbox-aggregator-pending-count"]))
(def sandbox-updater-pending-entries (histograms/histogram ["cook-mesos" "scheduler" "sandbox-updater-pending-entries"]))
(def sandbox-updater-publish-duration (timers/timer ["cook-mesos" "scheduler" "sandbox-updater-publish-duration"]))
(def sandbox-updater-publish-rate (meters/meter ["cook-mesos" "scheduler" "sandbox-updater-publish-rate"]))
(def sandbox-updater-tx-duration (timers/timer ["cook-mesos" "scheduler" "sandbox-updater-tx-duration"]))
(def sandbox-updater-tx-rate (meters/meter ["cook-mesos" "scheduler" "sandbox-updater-tx-rate"]))

(defn agent->task-id->sandbox
  "Returns the sandbox in the current agent state."
  [task-id->sandbox-agent task-id]
  (get @task-id->sandbox-agent task-id))

(defn clear-agent-state
  "Clears the published entries from the aggregated state of the agent.
   Since we expect instances to have the same sandbox, we do not check for value equality."
  [task-id->sandbox published-task-id->sandbox]
  (let [task-id->sandbox' (apply dissoc task-id->sandbox (keys published-task-id->sandbox))]
    (counters/clear! sandbox-aggregator-pending-count)
    (counters/inc! sandbox-aggregator-pending-count (count task-id->sandbox'))
    task-id->sandbox'))

(defn aggregate-sandbox
  "Aggregates the sandbox specified in `data` into the current sandbox state `task-id->sandbox`.
   Existing entries in task-id->sandbox take precedence during aggregation.
   It provides two overloaded versions:
   1. Aggregates an individual task-id and sandbox pair.
   2. Aggregates a collection (map) of task-id to sandbox mappings.
   It returns the new task-id->sandbox state."
  ([task-id->sandbox task-id sandbox]
   (meters/mark! sandbox-aggregator-message-rate)
   (if (and task-id sandbox (not (contains? task-id->sandbox task-id)))
     (let [task-id->sandbox' (assoc task-id->sandbox task-id sandbox)]
       (counters/inc! sandbox-aggregator-pending-count)
       task-id->sandbox')
     task-id->sandbox))
  ([task-id->sandbox candidate-task-id->sandbox]
   (reduce (fn [accum-task-id->sandbox [task-id sandbox]]
             (aggregate-sandbox accum-task-id->sandbox task-id sandbox))
           task-id->sandbox
           candidate-task-id->sandbox)))

(defn publish-sandbox-to-datomic!
  "Transacts the latest aggregated task-id->sandbox to datomic.
   No more than batch-size facts are updated in individual datomic transactions."
  [datomic-conn batch-size task-id->sandbox-agent]
  (let [task-id->sandbox @task-id->sandbox-agent]
    (log/info "Publishing" (count task-id->sandbox) "instance sandbox directories")
    (histograms/update! sandbox-updater-pending-entries (count task-id->sandbox))
    (meters/mark! sandbox-updater-publish-rate)
    (timers/time!
      sandbox-updater-publish-duration
      (doseq [task-id->sandbox-partition (partition-all batch-size task-id->sandbox)]
        (try
          (letfn [(build-sandbox-txns [[task-id sandbox]]
                    [:db/add [:instance/task-id task-id] :instance/sandbox-directory sandbox])]
            (let [txns (map build-sandbox-txns task-id->sandbox-partition)]
              (when (seq txns)
                (log/info "Inserting" (count txns) "facts in sandbox state update")
                (meters/mark! sandbox-updater-tx-rate)
                (timers/time!
                  sandbox-updater-tx-duration
                  @(d/transact datomic-conn txns)))))
          (send task-id->sandbox-agent clear-agent-state task-id->sandbox-partition)
          (catch Exception e
            (log/error e "Sandbox batch update error")))))
    {}))

(defn retrieve-sandbox-directories-on-agent
  "Builds an indexed version of all task-id to sandbox directory on the specified agent.
   Usually takes 100-500ms to run."
  [framework-id agent-hostname]
  (log/info "Retrieving sandbox directories for tasks on" agent-hostname)
  (let [timeout-millis (* 5 1000)
        ;; Throw SocketTimeoutException or ConnectionTimeoutException when timeout
        {:strs [completed_frameworks frameworks]} (-> (str "http://" agent-hostname ":5051/state.json")
                                                      (http/get
                                                        {:as :json-string-keys
                                                         :conn-timeout timeout-millis
                                                         :socket-timeout timeout-millis
                                                         :spnego-auth true})
                                                      :body)
        framework-filter (fn framework-filter [{:strs [id] :as framework}]
                           (when (= framework-id id) framework))
        ;; there should be at most one framework entry for a given framework-id
        target-framework (or (some framework-filter frameworks)
                             (some framework-filter completed_frameworks))
        {:strs [completed_executors executors]} target-framework
        framework-executors (reduce into [] [completed_executors executors])]
    (->> framework-executors
         (map (fn executor-state->task-id->sandbox-directory [{:strs [id directory]}]
                [id directory]))
         (into {}))))

(defn refresh-agent-cache-entry
  "If the entry for the specified agent is not cached:
   - Triggers building an indexed version of all task-id to sandbox directory for the specified agent;
   - After the indexed version is built, the tasks which have sandbox directories are removed;
   - The filtered version of tasks without sandbox directories is then synced into the task-id->sandbox-agent.
   The function call is a no-op if the specified agent already exists in the cache."
  [{:keys [mesos-agent-query-cache]} framework-id hostname]
  (try
    (let [run (delay
                (try
                  (let [task-id->sandbox-directory (retrieve-sandbox-directories-on-agent framework-id hostname)]
                    (log/info "Found" (count task-id->sandbox-directory) "tasks without sandbox directories on" hostname)
                    {:result :success
                     :data task-id->sandbox-directory})
                  (catch Exception e
                    (log/error e "Failed to get mesos agent state on" hostname)
                    {:result :error})))
          cs (swap! mesos-agent-query-cache
                    (fn mesos-agent-query-cache-swap-fn [c]
                      (if (cache/has? c hostname)
                        (cache/hit c hostname)
                        (cache/miss c hostname run))))
          val (cache/lookup cs hostname)]
      (if val @val @run))
    (catch Exception e
      (log/error e "Failed to refresh mesos agent state" {:agent hostname}))))

(defn sync-agent-sandboxes
  "Asynchronously triggers state syncing from the mesos agent."
  [publisher-state framework-id agent-hostname]
  (when (and framework-id agent-hostname)
    (future
      (refresh-agent-cache-entry publisher-state framework-id agent-hostname))))

(defn hostname->task-id->sandbox-directory
  "Retrieves the sandbox directory of a task on a specific mesos agent."
  [publisher-state framework-id hostname task-id]
  (let [{:keys [data]} (refresh-agent-cache-entry publisher-state framework-id hostname)]
    (get data task-id)))

(defn update-sandbox
  "Sends a message to the agent to update the sandbox information."
  [{:keys [task-id->sandbox-agent]} {:strs [sandbox-directory task-id]}]
  (send task-id->sandbox-agent aggregate-sandbox task-id sandbox-directory))

(defn start-sandbox-publisher
  "Launches a timer task that triggers publishing of the task-id->sandbox state to datomic.
   The task is invoked at intervals of publish-interval-ms ms."
  [task-id->sandbox-agent datomic-conn publish-batch-size publish-interval-ms]
  (log/info "Starting sandbox publisher at intervals of" publish-interval-ms "ms")
  (chime/chime-at
    (periodic/periodic-seq (time/now) (time/millis publish-interval-ms))
    (fn sandbox-publisher-task [_]
      (log/info "Requesting publishing of instance sandbox directories")
      (publish-sandbox-to-datomic! datomic-conn publish-batch-size task-id->sandbox-agent))
    {:error-handler (fn sandbox-publisher-error-handler [ex]
                      (log/error ex "Instance sandbox directory publish failed"))}))

(defn prepare-sandbox-publisher
  "This function initializes the sandbox publisher as well as helper functions to send individual
   sandbox entries and trigger sandbox syncing of all tasks on a mesos agent.
   It returns a map with the following entries:
   :hostname->task-id->sandbox-directory-fn - fn that returns the sandbox directory given the hostname and task-id.
   :mesos-agent-query-cache - the cache that throttles the state sync calls to the mesos agents.
   :publisher-cancel-fn - fn that take no arguments and that terminates the publisher.
   :task-id->sandbox-agent - The agent that manages the task-id->sandbox aggregation and publishing."
  [framework-id datomic-conn publish-batch-size publish-interval-ms mesos-agent-query-cache]
  (let [task-id->sandbox-agent (agent {}) ;; stores all the pending task-id->sandbox state
        publisher-state {:mesos-agent-query-cache mesos-agent-query-cache
                         :task-id->sandbox-agent task-id->sandbox-agent}
        publisher-cancel-fn (start-sandbox-publisher
                              task-id->sandbox-agent datomic-conn publish-batch-size publish-interval-ms)
        hostname->task-id->sandbox-directory-fn (fn hostname->task-id->sandbox-directory-fn [agent-hostname task-id]
                                                  (hostname->task-id->sandbox-directory
                                                    publisher-state framework-id agent-hostname task-id))]
    (assoc publisher-state
      :hostname->task-id->sandbox-directory-fn hostname->task-id->sandbox-directory-fn
      :publisher-cancel-fn publisher-cancel-fn)))
