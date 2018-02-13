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
(ns cook.scratch
  (:require [clj-time.core :as t]
            [cook.datomic :refer (transact-with-retries)]
            [cook.mesos.scheduler :as sched]
            [cook.mesos.util :as util]
            [cook.test.testutil :as testutil]
            [datomic.api :as d :refer (q)]
            [metatransaction.core :refer (db)]))

(comment
  ;; Mark instance failed.
  (let [conn (d/connect "datomic:riak://db.example.com:8098/datomic/mesos-jobs?interface=http")
        db (db conn)
        user "testuser"
        running-instances (q '[:find ?i
                               :in $ ?user [?status ...]
                               :where
                               [?j :job/user ?user]
                               [?j :job/instance ?i]
                               [?i :instance/status ?status]]
                             db user [:instance.status/running])]
    (println (count running-instances))
    (doseq [[instance] running-instances]
      (transact-with-retries conn
                             [[:db/add instance
                               :instance/status :instance.status/failed]])))

  ;; Query a job
  (let [conn (d/connect "datomic:riak://db.example.com:8098/datomic/mesos-jobs?interface=http")
        db (db conn)
        user "testuser"
        running-instances (q '[:find ?i ?state ?task-id
                               :in $ ?user [?status ...]
                               :where
                               [?j :job/user ?user]
                               [?j :job/state ?s]
                               [?s :db/ident ?state]
                               [?j :job/instance ?i]
                               [?i :instance/task-id ?task-id]
                               [?i :instance/status ?status]
                               ]
                             db user [:instance.status/c])]
    (println running-instances))

  ;; Kill a user's jobs
  (let [conn (d/connect "datomic:riak://db.example.com:8098/datomic/mesos-jobs?interface=http")
        job-uuids (q '[:find ?uuid
                       :in $ [?status ...]
                       :where
                       [?j :job/user "testuser"]
                       [?j :job/state ?status]
                       [?j :job/uuid ?uuid]]
                     (db conn) [:job.state/waiting
                                :job.state/running])]
    (->> job-uuids
         (map first)
         (sched/kill-job conn)))

  (let [conn (d/connect "datomic:riak://db.example.com:8098/datomic/mesos-jobs?interface=http")]
    (println "testuser" (sched/get-used-quota (db conn) "testuser"))
    (println "testuser2" (sched/get-quota (db conn) "testuser2")))

  ;; List users who has pre-defined quota.
  (let [conn (d/connect "datomic:riak://db.example.com:8098/datomic/mesos-jobs?interface=http")
        user "testuser"
        db (db conn)
        users (q '[:find ?u
                   :in $
                   :where
                   [?e :quota/user ?u]
                   [?e :quota/resource ?r]]
                 db)]
    (println users))

  ;; Count the job which has multiple instances.
  (let [conn (d/connect "datomic:riak://db.example.com:8098/datomic/mesos-jobs?interface=http")
        db (db conn)
        jobs (q '[:find ?j
                  :in $
                  :where
                  [?j :job/state :job.state/running]]
                db)
        njobs (atom 0)]
    (doseq [[job] jobs]
      (let [insts (q '[:find ?job ?i
                       :in $ ?job
                       :where
                       [?job :job/instance ?i]
                       [?i :instance/status :instance.status/running]]
                     db job)]
        (when (> (count insts) 1)
          (swap! njobs inc)
          (println insts @njobs)))))

  ;; Adjust the quota.
  (let [conn (d/connect "datomic:riak://db.example.com:8098/datomic/mesos-jobs?interface=http")]
    (sched/set-quota! conn "promised" :resource.type/cpus 3000.0 :resource.type/mem 2500000.0))

  ;; Kill jobs running for more than a week
  (let [conn (d/connect "datomic:riak://db.example.com:8098/datomic/mesos-jobs?interface=http")
        db (d/db conn)
        one-week-ago (- (.getTime (java.util.Date.)) (* 7 24 60 60 1000))
        job-eids (->>
                   (q '[:find ?j ?start-time
                        :where
                        [?j :job/instance ?i]
                        [?j :job/state :job.state/running]
                        [?i :instance/start-time ?start-time]
                        [?i :instance/status :instance.status/running]]
                      db)
                   (filter (fn [[j start-time]]
                             (<= (.getTime start-time) one-week-ago)))
                   (sort-by second)
                   (map first))
        txns (map (fn [job-eid]
                    [:db/add job-eid :job/state :job.state/completed])
                  job-eids)]
    (d/transact conn txns))

  ;; Retract quota.
  (let [conn (d/connect "datomic:riak://db.example.com:8098/datomic/mesos-jobs?interface=http")]
    (sched/retract-quota! conn "testuser")))

(defn create-job
  "Creates a new job with one instance"
  [conn host instance-status start-time end-time & args]
  (let [job (apply testutil/create-dummy-job (cons conn args))
        inst (testutil/create-dummy-instance conn job
                                             :instance-status instance-status
                                             :hostname host
                                             :start-time start-time
                                             :end-time end-time)]
    [job inst]))

(defn rand-str
  "Returns a random string of length len"
  [len]
  (apply str (take len (repeatedly #(char (+ (rand 26) 65))))))

(defn populate-dummy-data
  "Populates 100 jobs, each with one completed instance, with some random fields"
  [conn]
  (run!
    (fn [_]
      (let [host (rand-str 16)
            instance-status (rand-nth [:instance.status/failed :instance.status/success])
            user (rand-nth ["alice" "bob" "claire" "doug" "emily" "frank" "gloria" "henry"])
            start-time (.toDate (t/ago (t/minutes (inc (rand-int 120)))))
            end-time (.toDate (t/now))]
        (create-job conn host instance-status start-time end-time :user user :job-state :job.state/completed)))
    (range 100)))

(defn update-stats
  "Updates the provided stats map with the data from the provided task entity"
  [stats task-ent]
  (let [job-ent (:job/_instance task-ent)
        user (:job/user job-ent)
        resources (util/job-ent->resources job-ent)
        run-time (-> task-ent util/task-run-time)
        hours (-> run-time .toDurationMillis (/ 1000) (/ 60) (/ 60))]
    (-> stats
        (conj (assoc resources
                :user user
                :run-time run-time
                :cpu-hours (* hours (:cpus resources))
                :mem-hours (* hours (:mem resources)))))))

(defn get-completed-tasks
  "Gets all tasks that completed in the specified time range and with the specified
  status. Assumes that tasks complete within max-run-time of starting."
  [db end-time-start end-time-end instance-status max-run-time]
  (let [start-entity-id (d/entid-at db :db.part/user (.toDate (t/minus end-time-start max-run-time)))
        end-entity-id (d/entid-at db :db.part/user (.toDate (t/plus end-time-end (t/hours 1))))
        instance-status-entid (d/entid db instance-status)
        instance-status-attribute-entid (d/entid db :instance/status)
        end-time-start-millis (.getTime (.toDate end-time-start))
        end-time-end-millis (.getTime (.toDate end-time-end))]
    (->> (d/seek-datoms db :avet :instance/status instance-status-entid start-entity-id)
         (take-while #(and
                        (< (.e %) end-entity-id)
                        (= (.a %) instance-status-attribute-entid)
                        (= (.v %) instance-status-entid)))
         (map #(.e %))
         (map (partial d/entity db))
         (filter #(<= end-time-start-millis (.getTime (:instance/end-time %))))
         (filter #(< (.getTime (:instance/end-time %)) end-time-end-millis)))))

(defn percentile
  "Calculates the p-th percentile of the values in coll
  (where 0 < p <= 100), using the Nearest Rank method:
  https://en.wikipedia.org/wiki/Percentile#The_Nearest_Rank_method
  Assumes that coll is sorted (see percentiles below for context)"
  [coll p]
  (if (or (empty? coll) (not (number? p)) (<= p 0) (> p 100))
    nil
    (nth coll
         (-> p
             (/ 100)
             (* (count coll))
             (Math/ceil)
             (dec)))))

(defn percentiles
  "Calculates the p-th percentiles of the values in coll for
  each p in p-list (where 0 < p <= 100), and returns a map of
  p -> value"
  [coll & p-list]
  (let [sorted (sort coll)]
    (into {} (map (fn [p] [p (percentile sorted p)]) p-list))))

(defn generate-stats
  "TODO(DPO)"
  [task-entries]
  (let [percentiles #(percentiles % 50 75 95 99 100)]
    {:run-time  (percentiles (->> task-entries (map :run-time) (map #(.toDurationMillis %))))
     :cpu-hours (percentiles (->> task-entries (map :cpu-hours)))
     :mem-hours (percentiles (->> task-entries (map :mem-hours)))}))

(defn get-completed-task-stats
  "Returns a map from status -> user -> stats"
  [db end-time-start end-time-end max-run-time]
  (let [failed-tasks (get-completed-tasks db end-time-start end-time-end :instance.status/failed max-run-time)
        success-tasks (get-completed-tasks db end-time-start end-time-end :instance.status/success max-run-time)
        failed (reduce update-stats [] failed-tasks)
        success (reduce update-stats [] success-tasks)
        ]
    {:failed  (generate-stats failed)
     :success (generate-stats success)}))
