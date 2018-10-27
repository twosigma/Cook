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
(ns cook.mesos.heartbeat
  (:require [chime :refer [chime-at chime-ch]]
            [clj-time.coerce :as tc]
            [clj-time.core :as time]
            [clojure.core.async :as async :refer [alts! go-loop go >!]]
            [clojure.tools.logging :as log]
            [cook.mesos.util :as util]
            [datomic.api :as d :refer (q)]
            [metrics.meters :as meters]
            [metrics.timers :as timers]))

"State is a vector of three things.
 - The first is a set that contains all the timeout channels.
 - The second is a map from task id to its timeout channel.
 - The third is a timeout channel to task id."

(defn create-timeout-ch
  "Create a chime-ch that will send event 15 minutes after creation"
  []
  (chime-ch [(-> 15 time/minutes time/from-now)]))

(defn notify-heartbeat
  "Notify a heartbeat."
  [ch executor-id slave-id {:strs [task-id] :as data}]
  (try
    (case (async/alt!!
            [[ch data]] :success
            :default :dropped
            :priority true)
      :dropped (log/error "Dropped heartbeat from task" task-id)
      :success (log/debug "Received heartbeat from task" task-id)
      nil)
    (catch Throwable ex
      (log/error ex "Failed to handle heartbeat" {:data data, :executor-id executor-id, :slave-id slave-id}))))

(defn update-heartbeat
  "Update state upon receiving a heartbeat."
  [[timeout-chs task->ch ch->task] task-id new-ch]
  (let [old-ch (get task->ch task-id)]
    (log/debug "Replacing the timeout channel for task" task-id "from" old-ch "to" new-ch)
    [(-> timeout-chs
         (disj old-ch)
         (conj new-ch))
     (assoc task->ch task-id new-ch)
     (-> ch->task
         (dissoc old-ch)
         (assoc new-ch task-id))]))

(defn handle-timeout
  "Update state upon task timeout. Return the new state, task id and transaction to kill the task."
  [[timeout-chs task->ch ch->task] db timeout-ch]
  (let [task-id (get ch->task timeout-ch)
        new-state [(disj timeout-chs timeout-ch)
                   (dissoc task->ch task-id)
                   (dissoc ch->task timeout-ch)]
        instance-id (ffirst (q '[:find ?e
                                 :in $ ?task-id
                                 :where
                                 [?e :instance/task-id ?task-id]]
                               db task-id))
        txns [[:instance/update-state instance-id :instance.status/failed [:reason/name :hearbeat-lost]] 
              [:db/add instance-id :instance/reason [:reason/name :heartbeat-lost]]]]
    [new-state task-id txns]))

(timers/deftimer [cook-mesos heartbeat datomic-sync-duration])

(defn sync-with-datomic
  "Synchronize state with datomic database.
   It will add new timeout channels for missing tasks which are using custom executor."
  [[timeout-chs task->ch ch->task] db]
  (timers/time!
    datomic-sync-duration
    (let [missing-task-ids (->> (q '[:find ?task-id ?job
                                     :in $ [?status ...]
                                     :where
                                     [?i :instance/status ?status]
                                     [?i :instance/task-id ?task-id]
                                     [?job :job/instance ?i]]
                                   db [:instance.status/unknown :instance.status/running])
                                ;; Filter out tasks which have been tracked and tasks which do not use custom executor.
                                (keep (fn [[task-id job]]
                                       ;; If the custom-executor attr is not set, it by default uses a custom executor.
                                       (when (and (not (contains? task->ch task-id))
                                                  (:job/custom-executor (d/entity db job) true))
                                         task-id))))
          missing-chs (repeatedly (count missing-task-ids) create-timeout-ch)
          new-timeout-chs (into timeout-chs missing-chs)
          new-task->ch (merge task->ch (zipmap missing-task-ids missing-chs))
          new-ch->task (merge ch->task (zipmap missing-chs missing-task-ids))]
      (when (seq missing-task-ids)
        (log/info "Found running tasks in datomic that don't have timeout channels. Tasks:" missing-task-ids))
      [new-timeout-chs new-task->ch new-ch->task])))

(defn transact-timeout-async
  "Takes a timeout txn and transact it asynchronously. Log timeout warning if this txn does actually timeout the task.
   (Completed/failed tasks will have timeout event too since we don't clean up timeout channels for completed/failed tasks."
  [conn task-id txn]
  ;; The query is an optimization. It will reduce the number of transactions significantly.
  (let [db (d/db conn)
        status (:instance/status (d/entity db [:instance/task-id task-id]))]
    (when (#{:instance.status/running :instance.status/unknown} status)
      #_(log/info "Timed out task:" task-id "(But not really...)")
      (log/info "Timed out task:" task-id)
      (d/transact-async conn txn))))

(meters/defmeter [cook-mesos heartbeat heartbeats])
(meters/defmeter [cook-mesos heartbeat timeouts])

(defn start-heartbeat-watcher!
  "Start heartbeat watcher. Return a fn to stop heartbeat watcher."
  [conn heartbeat-ch]
  (let [shutdown-ch (async/chan (async/dropping-buffer 1))
        sync-datomic-ch (chime-ch (util/time-seq (time/now) (time/minutes 5)) (async/sliding-buffer 1))]
    (go-loop [[timeout-chs _ _ :as state] [#{} {} {}]]
             (let [;; Order is important here. We want to avoid the case where we timeout a task before having the chance to process its heartbeat.
                   [v ch] (async/alts! (into [shutdown-ch heartbeat-ch sync-datomic-ch] timeout-chs) :priority true)]
               (log/debug "Got" v "from" ch)
               (when-let [new-state
                          (cond
                           (= shutdown-ch ch)
                           (do
                             (log/info "Heartbeat watcher stopped")
                             nil)

                           (= heartbeat-ch ch)
                           (try
                             (meters/mark! heartbeats)
                             (let [{:strs [task-id timestamp]} v
                                   new-ch (create-timeout-ch)
                                   state' (update-heartbeat state task-id new-ch)]
                               (log/debug "Updated heartbeat for task" task-id
                                          "Heartbeat creation timestamp:" (when timestamp (tc/from-long timestamp)))
                               state')
                             (catch Throwable t
                               (log/error t "Failed to update heartbeat")
                               state))

                           (= sync-datomic-ch ch)
                           (try
                             (let [state' (sync-with-datomic state (d/db conn))]
                               state')
                             (catch Throwable t
                               (log/error t "Failed to sync heartbeat with datomic")
                               state))

                           (timeout-chs ch)
                           (try
                             (meters/mark! timeouts)
                             (let [[state' task-id txn] (handle-timeout state (d/db conn) ch)]
                               (transact-timeout-async conn task-id txn)
                               state')
                             (catch Throwable t
                               (log/error t "Failed to handle task timeout")
                               state)))]
                 (recur new-state))))
    (log/info "Heartbeat watcher started")
    (fn [] (async/close! shutdown-ch))))

(comment
  (let [ch1 (async/chan)
        ch2 (async/chan)]
    (go-loop [channels #{ch1 ch2}]
             (let [[v ch] (async/alts! (conj (seq channels) ch1))]
               (cond
                (= ch ch1) (println "Got" v "from ch1!")
                (= ch ch2) (println "Got" v "from ch2!"))
               (recur [ch1 ch2])
               ))
    (async/go (async/>! ch1 "something"))
    (async/go (async/>! ch2 "something else"))))

(comment
  (let [conn (d/connect  "datomic:mem://mesos-jobs")
        db (d/db conn)]
    (q '[:find ?instance ?state
         :in $ ?task-id
         :where
         [?instance :instance/status ?s]
         [?instance :instance/task-id ?task-id]
         [?s :db/ident ?state]]
       db "54111014-b33c-46e6-9e7e-2f3e9c8f7f90")))

(comment
  (let [conn (d/connect  "datomic:mem://mesos-jobs")
        db (d/db conn)]
    (->>
     (q '[:find ?task-id
          :in $ [?status ...]
          :where
          [?e :instance/status ?status]
          [?e :instance/task-id ?task-id]]
        db [:instance.status/unknown :instance.status/running])
     (map (fn [[x]] x)))))
