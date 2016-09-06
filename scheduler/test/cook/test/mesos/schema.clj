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

(ns cook.test.mesos.schema
  (:use clojure.test)
  (:require   [cook.mesos.schema :as schema]
              [datomic.api :as d :refer (q db)]))

(def datomic-uri "datomic:mem://test")

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
  [conn & {:keys [user uuid command ncpus memory name retry-count max-runtime priority job-state submit-time custom-executor? gpus]
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
        job-info {:db/id id
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
        job-info (if gpus
                   (update-in job-info [:job/resource] conj {:resource/type :resource.type/gpus
                                                             :resource/amount (double gpus)})
                   job-info)
        val @(d/transact conn [(if (nil? custom-executor?)
                                 job-info
                                 (assoc job-info :job/custom-executor custom-executor?))])]
    (d/resolve-tempid (db conn) (:tempids val) id)))

(defn create-dummy-instance
  "Return the entity id for the created instance."
  [conn job & {:keys [job-state instance-status start-time hostname task-id progress backfilled?]
               :or  {job-state :job.state/running
                     instance-status :instance.status/unknown
                     start-time (java.util.Date.)
                     hostname "localhost"
                     task-id (str (str (java.util.UUID/randomUUID)))
                     backfilled? false
                     progress 0} :as cfg}]
  (let [id (d/tempid :db.part/user)
        val @(d/transact conn [{:db/id job
                                :job/state job-state}
                               {:db/id id
                                :job/_instance job
                                :instance/hostname hostname
                                :instance/progress progress
                                :instance/backfilled? backfilled?
                                :instance/status instance-status
                                :instance/start-time start-time
                                :instance/task-id task-id}])]
    (d/resolve-tempid (db conn) (:tempids val) id)))

(deftest test-instance-update-state
  (let [uri datomic-uri
        conn (restore-fresh-database! uri)
        job (create-dummy-job conn)
        verify-state-transition (fn [old-state target-state new-state]
                                  (let [instance (create-dummy-instance conn job :instance-status old-state)]
                                    (d/transact conn [[:instance/update-state instance target-state]])
                                    (is (= new-state
                                           (ffirst (q '[:find ?status
                                                        :in $ ?i
                                                        :where
                                                        [?i :instance/status ?s]
                                                        [?s :db/ident ?status]]
                                                      (db conn) instance))))))]
    (testing
        (verify-state-transition :instance.status/unknown :instance.status/unknown :instance.status/unknown))
    (testing
        (verify-state-transition :instance.status/unknown :instance.status/running :instance.status/running))
    (testing
        (verify-state-transition :instance.status/unknown :instance.status/success :instance.status/unknown))
    (testing
        (verify-state-transition :instance.status/unknown :instance.status/failed :instance.status/failed))

    (testing
        (verify-state-transition :instance.status/running :instance.status/unknown :instance.status/running))
    (testing
        (verify-state-transition :instance.status/running :instance.status/running :instance.status/running))
    (testing
        (verify-state-transition :instance.status/running :instance.status/success :instance.status/success))
    (testing
        (verify-state-transition :instance.status/running :instance.status/failed :instance.status/failed))

    (testing
        (verify-state-transition :instance.status/success :instance.status/unknown :instance.status/success))
    (testing
        (verify-state-transition :instance.status/success :instance.status/running :instance.status/success))
    (testing
        (verify-state-transition :instance.status/success :instance.status/success :instance.status/success))
    (testing
        (verify-state-transition :instance.status/success :instance.status/failed :instance.status/success))

    (testing
        (verify-state-transition :instance.status/failed :instance.status/unknown :instance.status/failed))
    (testing
        (verify-state-transition :instance.status/failed :instance.status/running :instance.status/failed))
    (testing
        (verify-state-transition :instance.status/failed :instance.status/success :instance.status/failed))
    (testing
        (verify-state-transition :instance.status/failed :instance.status/failed :instance.status/failed))))

(comment
  (run-tests))
