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
  (:require [cook.datomic :refer (transact-with-retries)]
            [cook.mesos.monitor :refer (riemann-reporter)]
            [cook.mesos.scheduler :as sched]
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
                                (<=(.getTime start-time) one-week-ago)))
                      (sort-by second)
                      (map first))
        txns (map (fn [job-eid]
                    [:db/add job-eid :job/state :job.state/completed])
                  job-eids)]
    (d/transact conn txns))

  ;; Retract quota.
  (let [conn (d/connect "datomic:riak://db.example.com:8098/datomic/mesos-jobs?interface=http")]
    (sched/retract-quota! conn "testuser")))
