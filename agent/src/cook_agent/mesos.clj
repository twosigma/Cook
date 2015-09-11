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
(ns cook-agent.mesos
  (:require [clj-mesos.executor :as mesos]
            [clojure.tools.logging :as log]
            [clojure.data.json :as json]
            [clj-time.core :as time]
            [clj-time.coerce :as tc]
            [clj-time.periodic :as periodic]
            [chime :refer [chime-at]]))

(declare executor)

(defn get-mesos-environment
  "Returns the environment map."
  []
  (when-not (and (System/getenv "MESOS_SLAVE_ID")
                 (System/getenv "MESOS_EXECUTOR_ID"))
    (println "Not in mesos!")
    (System/exit 0))
  (println "Creating the mesos environment")
  (let [instance (promise)
        ipc-sink (promise)
        task-id (promise)
        driver (org.apache.mesos.MesosExecutorDriver. (executor {:instance instance
                                                                 :ipc-sink ipc-sink
                                                                 :task-id task-id}))]
    (println "Created the driver")
    (mesos/start driver)
    (println "Started the driver")
    (let [x {:output-dir (System/getProperty "user.dir")
             :instance @instance
             :ipc-sink @ipc-sink
             :task-id  @task-id
             :type :mesos
             :finished-success (fn []
                                 (println "Sending update to mesos--success")
                                 (mesos/send-status-update driver {:task-id @task-id
                                                                   :state :task-finished}))
             :finished-fail (fn []
                              (println "Sending update to mesos--failed")
                              (mesos/send-status-update driver {:task-id @task-id
                                                                :state :task-failed}))}]
      (println "Success!")
      x)))

(defn start-heartbeat!
  "Starts the heartbeat in a seperate thread. Returns a fn to stop heartbeating."
  [driver task-info]
  (letfn [(send-heartbeat [heartbeat-creation-time]
            (try
              (let [data (->  {:type "heartbeat"
                               :task-id (:task-id task-info)
                               :timestamp (tc/to-long heartbeat-creation-time)}
                              (json/write-str)
                              (.getBytes "UTF-8"))]
                (mesos/send-framework-message driver data)
                (println (time/now) "Heartbeat sent. Creation time:" heartbeat-creation-time))
              (catch Throwable t
                (println "Failed to send heartbeat. Exception:" t)
                (.printStackTrace t))))]
    (println "Starting heartbeat routine...")
    (send-heartbeat (time/now))
    (chime-at (periodic/periodic-seq (time/now) (time/seconds 60))
              send-heartbeat)))

(defn executor
  "Creates and returns a mesos executor"
  [{:keys [instance ipc-sink task-id]}]
  (mesos/executor
    (registered [driver executor-info framework-info slave-info])
    (reregistered [driver slave-info])
    (disconnected [driver])
    (launchTask [driver task-info]
                (println "Got task info:" task-info)
                (deliver task-id (:task-id task-info))
                (let [data (read-string (String. (:data task-info) "UTF-8"))]
                  (mesos/send-status-update
                    driver
                    {:task-id (:task-id task-info)
                     :state :task-running})
                  (println "Acked task starting")
                  (deliver instance (:instance data))
                  (deliver ipc-sink (fn send-update [{:keys [percent progress]}]
                                      (mesos/send-status-update
                                        driver
                                        {:task-id (:task-id task-info)
                                         :state :task-running
                                         :data (-> {:percent percent :progress progress}
                                                   (pr-str)
                                                   (.getBytes "UTF-8"))})))
                  (start-heartbeat! driver task-info)))

    (killTask [driver task-id']
        (if (not= @task-id task-id')
          (log/error "Kill task failed: expected to get task-id " @task-id ", but got " task-id')
          (future
            (log/warn "Preparing to die...please wait")
            (Thread/sleep 6000)
            (System/exit 2))))
    (frameworkMessage [driver data])
    (shutdown [driver])
    (error [driver message]
           (println "Got a mesos error!" message))))

#_(clj-mesos.marshalling/proto->map
  (clj-mesos.marshalling/map->proto org.apache.mesos.Protos$TaskInfo
                                    {:name "mycooljob"
                                     :slave-id "myslaveid"
                                     :task-id (str (java.util.UUID/randomUUID))
                                     :executor {:executor-id (str (java.util.UUID/randomUUID))
                                                :command {:value "do stuff"}}
                                     :data (.getBytes (pr-str {:instance (rand-int 22)}) "UTF-8")}
                                    ))
