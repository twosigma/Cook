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
(ns cook.progress
  (:require [chime :refer [chime-at chime-ch]]
            [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clojure.tools.logging :as log]
            [cook.prometheus-metrics :as prom]
            [cook.tools :as util]
            [datomic.api :as d]
            [metrics.counters :as counters]
            [metrics.histograms :as histograms]
            [metrics.meters :as meters]
            [metrics.timers :as timers]))

(meters/defmeter [cook-mesos scheduler progress-aggregator-message-rate])
(meters/defmeter [cook-mesos scheduler progress-aggregator-drop-rate])
(counters/defcounter [cook-mesos scheduler progress-aggregator-drop-count])
(counters/defcounter [cook-mesos scheduler progress-aggregator-pending-states-count])

(defn progress-aggregator
  "Aggregates the progress state specified in `data` into the current progress state `instance-id->progress-state`.
   It drops messages if the aggregated state has more than `pending-threshold` different entries.
   It returns the new progress state.
   The aggregator also makes a local best-effort to avoid updating the progress state of individual instances with
   stale data based on the value of progress-sequence in the message.
   We avoid this stale check in the datomic transaction as it is relatively expensive to perform the checks on a per
   instance basis in the query."
  [pending-threshold sequence-cache-store instance-id->progress-state {:keys [instance-id] :as data}]
  (prom/inc prom/progress-aggregator-message-count)
  (meters/mark! progress-aggregator-message-rate)
  (if (or (< (count instance-id->progress-state) pending-threshold)
          (contains? instance-id->progress-state instance-id))
    (if (integer? (:progress-sequence data))
      (let [progress-state (select-keys data [:progress-message :progress-percent :progress-sequence])
            instance-id->progress-state' (update instance-id->progress-state instance-id
                                                 (fn [current-state]
                                                   (let [new-progress-sequence (:progress-sequence progress-state)
                                                         old-progress-sequence (util/cache-lookup! sequence-cache-store instance-id -1)]
                                                     (if (or (nil? current-state)
                                                             (< old-progress-sequence new-progress-sequence))
                                                       (do
                                                         (util/cache-update! sequence-cache-store instance-id new-progress-sequence)
                                                         progress-state)
                                                       current-state))))]
        (let [old-count (count instance-id->progress-state)
              new-count (count instance-id->progress-state')]
          (when (zero? old-count)
            (prom/set prom/progress-aggregator-pending-states-count 0)
            (counters/clear! progress-aggregator-pending-states-count))
          (prom/inc prom/progress-aggregator-pending-states-count (- new-count old-count))
          (counters/inc! progress-aggregator-pending-states-count (- new-count old-count)))
        instance-id->progress-state')
      (do
        (log/warn "skipping" data "as it is missing an integer progress-sequence")
        instance-id->progress-state))
    (do
      (prom/inc prom/progress-aggregator-drop-count)
      (meters/mark! progress-aggregator-drop-rate)
      (counters/inc! progress-aggregator-drop-count)
      (log/debug "Dropping" data "as threshold has been reached")
      instance-id->progress-state)))

(defn progress-update-aggregator
  "Launches a long running go block that triggers publishing the latest aggregated instance-id->progress-state
   wrapped inside a chan whenever there is a read on the progress-state-chan.
   It drops messages if the progress-aggregator-chan queue is larger than pending-threshold or the aggregated
   state has more than pending-threshold different entries.
   It returns the progress-aggregator-chan which can be used to send progress-state messages to the aggregator.

   Note: the wrapper chan is used due to our use of `util/xform-pipe`"
  [{:keys [pending-threshold publish-interval-ms sequence-cache-threshold]} progress-state-chan]
  (log/info "Starting progress update aggregator")
  (let [progress-aggregator-chan (async/chan (async/sliding-buffer pending-threshold))
        sequence-cache-store (-> {}
                                 (cache/lru-cache-factory :threshold sequence-cache-threshold)
                                 (cache/ttl-cache-factory :ttl (* 10 publish-interval-ms))
                                 atom)
        progress-aggregator-fn (fn progress-aggregator-fn [instance-id->progress-state data]
                                 (progress-aggregator pending-threshold sequence-cache-store instance-id->progress-state data))
        aggregator-go-chan (util/reducing-pipe progress-aggregator-chan progress-aggregator-fn progress-state-chan
                                               :initial-state {})]
    (async/go
      (async/<! aggregator-go-chan)
      (log/info "Progress update aggregator exited"))
    progress-aggregator-chan))

(defn- task-id->instance-id
  "Retrieves the instance-id given a task-id"
  [db task-id]
  (-> (d/entity db [:instance/task-id task-id])
      :db/id))

(defn handle-progress-message!
  "Processes a progress message by sending it along the progress-aggregator-chan channel."
  [db task-id progress-aggregator-chan progress-message-map]
  (let [instance-id (task-id->instance-id db task-id)]
    (when-not instance-id
      (throw (ex-info "No instance found!" {:task-id task-id})))
    (log/debug "Updating instance" instance-id "progress to" progress-message-map)
    (async/put! progress-aggregator-chan
                (assoc progress-message-map :instance-id instance-id))))

(histograms/defhistogram [cook-mesos scheduler progress-updater-pending-states])
(meters/defmeter [cook-mesos scheduler progress-updater-publish-rate])
(timers/deftimer [cook-mesos scheduler progress-updater-publish-duration])
(meters/defmeter [cook-mesos scheduler progress-updater-tx-rate])
(timers/deftimer [cook-mesos scheduler progress-updater-tx-duration])

(defn- publish-progress-to-datomic!
  "Transacts the latest aggregated instance-id->progress-state to datomic.
   No more than batch-size facts are updated in individual datomic transactions."
  [conn instance-id->progress-state batch-size]
  (prom/observe prom/progress-updater-pending-states (count instance-id->progress-state))
  (histograms/update! progress-updater-pending-states (count instance-id->progress-state))
  (meters/mark! progress-updater-publish-rate)
  (prom/with-duration
    prom/progress-updater-publish-duration {}
    (timers/time!
      progress-updater-publish-duration
      (doseq [instance-id->progress-state-partition
              (partition-all batch-size instance-id->progress-state)]
        (try
          (letfn [(build-progress-txns [[instance-id {:keys [progress-percent progress-message]}]]
                    (cond-> []
                      progress-percent (conj [:db/add instance-id :instance/progress (int progress-percent)])
                      progress-message (conj [:db/add instance-id :instance/progress-message progress-message])))]
            (let [txns (mapcat build-progress-txns instance-id->progress-state-partition)]
              (when (seq txns)
                (log/info "Performing" (count txns) "in progress state update")
                (meters/mark! progress-updater-tx-rate)
                (prom/with-duration
                  prom/progress-updater-transact-duration {}
                  (timers/time!
                    progress-updater-tx-duration
                    @(d/transact conn txns))))))
          (catch Exception e
            (log/error e "Progress batch update error")))))))

(defn progress-update-transactor
  "Launches a long running go block that triggers transacting the latest aggregated instance-id->progress-state
   to datomic whenever there is a message on the progress-updater-trigger-chan.
   No more than batch-size facts are updated in individual datomic transactions.
   It returns a map containing the progress-state-chan which can be used to send messages about the latest
   instance-id->progress-state which must be wrapped inside a channel. The producer must guarantee that this
   channel is promptly fulfilled if it is successfully put in the progress-state-chan.
   If no such message is on the progress-state-chan, then no datomic interactions occur."
  [progress-updater-trigger-chan batch-size conn]
  (log/info "Starting progress update transactor")
  (let [progress-state-chan (async/chan)]
    (letfn [(progress-update-transactor-error-handler [e]
              (log/error e "Failed to update progress message on tasks!"))
            (progress-update-transactor-on-finished []
              (log/info "Exiting progress update transactor"))
            (process-progress-update-transactor-event []
              ;; progress-state-chan is expected to receive a promise-chan that contains the instance-id->progress-state
              (let [[instance-id->progress-state _] (async/alts!! [progress-state-chan (async/timeout 100)] :priority true)]
                (when instance-id->progress-state
                  (log/info "Received" (count instance-id->progress-state) "in progress-update-transactor")
                  (publish-progress-to-datomic! conn instance-id->progress-state batch-size))))]
      {:cancel-handle (util/chime-at-ch progress-updater-trigger-chan process-progress-update-transactor-event
                                        {:error-handler progress-update-transactor-error-handler
                                         :on-finished progress-update-transactor-on-finished})
       :progress-state-chan progress-state-chan})))

(defn make-progress-update-channels
  "Top-level function for building a progress-update-transactor and progress-update-aggregator."
  [progress-updater-trigger-chan progress-config conn]
  (let [{:keys [batch-size]} progress-config
        {:keys [progress-state-chan]} (progress-update-transactor progress-updater-trigger-chan batch-size conn)
        progress-update-aggregator-chan (progress-update-aggregator progress-config progress-state-chan)]
    {:progress-state-chan progress-state-chan
     :progress-aggregator-chan progress-update-aggregator-chan}))
