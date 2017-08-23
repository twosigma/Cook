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

(ns cook.test.testutil
  (:use clojure.test)
  (:require [clj-logging-config.log4j :as log4j-conf]
            [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clojure.tools.logging :as log]
            [cook.mesos.api :as api]
            [cook.mesos.schema :as schema]
            [datomic.api :as d :refer (q db)]
            [qbits.jet.server :refer (run-jetty)]
            [ring.middleware.params :refer (wrap-params)])
  (:import (java.util UUID)
           (org.apache.log4j ConsoleAppender Logger PatternLayout)))

(defn run-test-server-in-thread
  "Runs a minimal cook scheduler server for testing inside a thread. Note that it is not properly kerberized."
  [conn port]
  (let [authorized-fn (fn [x y z] true)
        api-handler (wrap-params
                      (api/main-handler conn
                                        "my-framework-id"
                                        (fn [] [])
                                        (atom (cache/lru-cache-factory {}))
                                        {:is-authorized-fn authorized-fn
                                         :mesos-gpu-enabled false
                                         :task-constraints {:cpus 12 :memory-gb 100 :retry-limit 200}}
                                        (Object.)
                                        (atom true)))
        ; Mock kerberization, not testing that
        api-handler-kerb (fn [req]
                           (api-handler (assoc req :authorization/user (System/getProperty "user.name"))))
        exit-chan (async/chan)]
    (async/thread
      (let [server (run-jetty {:port port :ring-handler api-handler-kerb :join? false})]
        (async/<!! exit-chan)
        (.stop server)))
    exit-chan))

(defmacro with-test-server [[conn port] & body]
  `(let [exit-chan# (run-test-server-in-thread ~conn ~port)]
     (try
       ~@body
       (finally
         (async/close! exit-chan#)))))

(defn restore-fresh-database!
  "Completely delete all data, start a fresh database and apply transactions if
   provided.

   Return a connection to the fresh database."
  [uri & txn]
  (d/delete-database uri)
  (d/create-database uri)
  (let [conn (d/connect uri)]
    (doseq [t schema/work-item-schema]
      @(d/transact conn t))
    (doseq [t txn]
      @(d/transact conn t))
    conn))

(defn create-dummy-job
  "Return the entity id for the created dummy job."
  [conn & {:keys [command committed? container custom-executor? disable-mea-culpa-retries env gpus group job-state
                  max-runtime memory name ncpus priority retry-count submit-time user uuid]
           :or {command "dummy command"
                committed? true
                disable-mea-culpa-retries false
                job-state :job.state/waiting
                max-runtime Long/MAX_VALUE
                memory 10.0
                ncpus 1.0
                name "dummy_job"
                priority 50
                retry-count 5
                submit-time (java.util.Date.)
                user (System/getProperty "user.name")
                uuid (d/squuid)}}]
  (let [id (d/tempid :db.part/user)
        commit-latch-id (d/tempid :db.part/user)
        commit-latch {:db/id commit-latch-id
                      :commit-latch/committed? committed?}
        container (when container
                    (let [container-var-id (d/tempid :db.part/user)]
                      [[:db/add id :job/container container-var-id]
                       (assoc container :db/id container-var-id)]))
        job-info (merge {:db/id id
                         :job/command command
                         :job/commit-latch commit-latch-id
                         :job/disable-mea-culpa-retries disable-mea-culpa-retries
                         :job/max-retries retry-count
                         :job/max-runtime max-runtime
                         :job/name name
                         :job/priority priority
                         :job/resource [{:resource/type :resource.type/cpus
                                         :resource/amount (double ncpus)}
                                        {:resource/type :resource.type/mem
                                         :resource/amount (double memory)}]
                         :job/state job-state
                         :job/submit-time submit-time
                         :job/user user
                         :job/uuid uuid}
                        (when (not (nil? custom-executor?)) {:job/custom-executor custom-executor?})
                        (when group {:group/_job group}))
        job-info (if gpus
                   (update-in job-info [:job/resource] conj {:resource/type :resource.type/gpus
                                                             :resource/amount (double gpus)})
                   job-info)
        environment (when (seq env)
                      (mapcat (fn [[k v]]
                                (let [env-var-id (d/tempid :db.part/user)]
                                  [[:db/add id :job/environment env-var-id]
                                   {:db/id env-var-id
                                    :environment/name k
                                    :environment/value v}]))
                              env))
        tx-data (cond-> [job-info commit-latch]
                        environment (into environment)
                        container (concat container))
        val @(d/transact conn tx-data)]
    (d/resolve-tempid (db conn) (:tempids val) id)))

(defn create-dummy-instance
  "Return the entity id for the created instance."
  [conn job & {:keys [cancelled end-time executor-id exit-code hostname instance-status job-state preempted?
                      progress progress-message reason sandbox-directory slave-id start-time task-id ]
               :or  {end-time nil
                     executor-id  (str (UUID/randomUUID))
                     hostname "localhost"
                     instance-status :instance.status/unknown
                     job-state :job.state/running
                     preempted? false
                     progress 0
                     progress-message nil
                     reason nil
                     slave-id  (str (UUID/randomUUID))
                     start-time (java.util.Date.)
                     task-id (str (str (UUID/randomUUID)))} :as cfg}]
  (let [id (d/tempid :db.part/user)
        val @(d/transact conn [(cond->
                                 {:db/id id
                                  :instance/executor-id executor-id
                                  :instance/hostname hostname
                                  :instance/preempted? preempted?
                                  :instance/progress progress
                                  :instance/slave-id slave-id
                                  :instance/start-time start-time
                                  :instance/status instance-status
                                  :instance/task-id task-id
                                  :job/_instance job}
                                 cancelled (assoc :instance/cancelled true)
                                 end-time (assoc :instance/end-time end-time)
                                 exit-code (assoc :instance/exit-code exit-code)
                                 progress-message (assoc :instance/progress-message progress-message)
                                 reason (assoc :instance/reason [:reason/name reason])
                                 sandbox-directory (assoc :instance/sandbox-directory sandbox-directory))])]
    (d/resolve-tempid (db conn) (:tempids val) id)))

(defn create-dummy-group
  "Return the entity id for the created group"
  [conn & {:keys [group-uuid group-name host-placement straggler-handling]
           :or  {group-uuid (UUID/randomUUID)
                 group-name "my-cool-group"
                 host-placement {:host-placement/type :host-placement.type/all}
                 straggler-handling {:straggler-handling/type :straggler-handling.type/none}}}]
  (let [id (d/tempid :db.part/user)
        group-txn {:db/id id
                   :group/uuid group-uuid
                   :group/name group-name
                   :group/host-placement host-placement
                   :group/straggler-handling straggler-handling}
        val @(d/transact conn [group-txn])]
    (d/resolve-tempid (db conn) (:tempids val) id)))

(defn init-offer-cache
  [& init]
  (-> init
      (or {})
      (cache/fifo-cache-factory :threshold 10000)
      (cache/ttl-cache-factory :ttl (* 1000 60))
      atom))

(defn poll-until
  "Polls `pred` every `interval-ms` and checks if it is true.
   If true, returns. Otherwise, `poll-until` will wait
   up to `max-wait-ms` at which point `poll-until` returns raises an exception
   The exception will include the output of `on-exceed-str-fn`"
  ([pred interval-ms max-wait-ms]
   (poll-until pred interval-ms max-wait-ms (fn [] "")))
  ([pred interval-ms max-wait-ms on-exceed-str-fn]
   (let [start-ms (.getTime (java.util.Date.))]
     (loop []
       (when-not (pred)
         (if (< (.getTime (java.util.Date.)) (+ start-ms max-wait-ms))
           (recur)
           (throw (ex-info (str "pred not true : " (on-exceed-str-fn))
                           {:interval-ms interval-ms
                            :max-wait-ms max-wait-ms}))))))))

(defmethod report :begin-test-var
  [m]
  (when (System/getProperty "cook.test.logging.console")
    (log4j-conf/set-loggers! (Logger/getRootLogger)
                             {:level :info
                              :out (ConsoleAppender. (PatternLayout. "%d{ISO8601} %-5p %c [%t] - %m%n"))}))
  (log/info "Running" (:var m)))
