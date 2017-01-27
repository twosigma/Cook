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
  (:require [cook.mesos.schema :as schema]
            [cook.mesos.api :as api :refer (main-handler)]
            [clojure.core.async :as async]
            [ring.middleware.params :refer (wrap-params)]
            [qbits.jet.server :refer (run-jetty)]
            [datomic.api :as d :refer (q db)]))

(defn run-test-server-in-thread
  "Runs a minimal cook scheduler server for testing inside a thread. Note that it is not properly kerberized."
  [conn port]
  (let [authorized-fn (fn [x y z] true)
        api-handler (wrap-params
                      (main-handler conn
                                    "my-framework-id"
                                    {:cpus 12 :memory-gb 100 :retry-limit 200}
                                    false
                                    (fn [] [])
                                    authorized-fn))
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
  [conn & {:keys [user uuid command ncpus memory name retry-count max-runtime priority job-state submit-time custom-executor? gpus group]
           :or {user (System/getProperty "user.name")
                uuid (d/squuid)
                command "dummy command"
                ncpus 1.0
                memory 10.0
                name "dummy job"
                submit-time (java.util.Date.)
                retry-count 5
                max-runtime Long/MAX_VALUE
                priority 50
                job-state :job.state/waiting}}]
  (let [id (d/tempid :db.part/user)
        job-info (merge {:db/id id
                         :job/uuid uuid
                         :job/command command
                         :job/user user
                         :job/name name
                         :job/max-retries retry-count
                         :job/max-runtime max-runtime
                         :job/priority priority
                         :job/state job-state
                         :job/submit-time submit-time
                         :job/resource [{:resource/type :resource.type/cpus
                                         :resource/amount (double ncpus)}
                                        {:resource/type :resource.type/mem
                                         :resource/amount (double memory)}]}
                        (when (not (nil? custom-executor?)) {:job/custom-executor custom-executor?})
                        (when group {:group/_job group}))
        job-info (if gpus
                   (update-in job-info [:job/resource] conj {:resource/type :resource.type/gpus
                                                             :resource/amount (double gpus)})
                   job-info)
        val @(d/transact conn [job-info])]
    (d/resolve-tempid (db conn) (:tempids val) id)))

(defn create-dummy-instance
  "Return the entity id for the created instance."
  [conn job & {:keys [job-state instance-status start-time end-time hostname task-id progress backfilled? reason slave-id executor-id]
               :or  {job-state :job.state/running
                     instance-status :instance.status/unknown
                     start-time (java.util.Date.)
                     end-time nil
                     hostname "localhost"
                     task-id (str (str (java.util.UUID/randomUUID)))
                     backfilled? false
                     progress 0
                     reason nil
                     slave-id  (str (java.util.UUID/randomUUID))
                     executor-id  (str (java.util.UUID/randomUUID))} :as cfg}]
  (let [id (d/tempid :db.part/user)
        val @(d/transact conn [(merge
                                 {:db/id id
                                  :job/_instance job
                                  :instance/hostname hostname
                                  :instance/progress progress
                                  :instance/backfilled? backfilled?
                                  :instance/status instance-status
                                  :instance/start-time start-time
                                  :instance/task-id task-id
                                  :instance/executor-id executor-id
                                  :instance/slave-id slave-id}
                                  (if (nil? reason) {} {:instance/reason [:reason/name reason]})
                                  (when end-time {:instance/end-time end-time}))])]
    (d/resolve-tempid (db conn) (:tempids val) id)))

(defn create-dummy-group
  "Return the entity id for the created group"
  [conn & {:keys [group-uuid group-name straggler-handling]
           :or  {group-uuid (java.util.UUID/randomUUID)
                 group-name "my-cool-group"
                 straggler-handling {:straggler-handling/type :straggler-handling.type/none}}}]
  (let [id (d/tempid :db.part/user)
        group-txn {:db/id id
                   :group/uuid group-uuid
                   :group/name group-name
                   :group/straggler-handling straggler-handling}
        val @(d/transact conn [group-txn])]
    (d/resolve-tempid (db conn) (:tempids val) id)))
