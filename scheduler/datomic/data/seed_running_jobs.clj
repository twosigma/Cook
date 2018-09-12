(ns data.seed-running-jobs
  (:require [cook.datomic :as datomic]
            [cook.mesos.util :as util]
            [datomic.api :as d])
  (:import (java.util Date UUID)))

(def uri (second *command-line-args*))
(println "Datomic URI is" uri)

(defn create-instance
  [conn job]
  @(d/transact conn [{:db/id (d/tempid :db.part/user)
                      :instance/executor-id (str (UUID/randomUUID))
                      :instance/hostname "localhost"
                      :instance/preempted? false
                      :instance/progress 0
                      :instance/slave-id (str (UUID/randomUUID))
                      :instance/start-time (Date.)
                      :instance/status :instance.status/unknown
                      :instance/task-id (str (UUID/randomUUID))
                      :job/_instance job}]))

(defn create-job
  [conn name]
  (let [id (d/tempid :db.part/user)
        commit-latch-id (d/tempid :db.part/user)
        commit-latch {:db/id commit-latch-id
                      :commit-latch/committed? true}
        job-info {:db/id id
                  :job/command "echo hello"
                  :job/commit-latch commit-latch-id
                  :job/disable-mea-culpa-retries false
                  :job/max-retries 1
                  :job/max-runtime Long/MAX_VALUE
                  :job/name name
                  :job/priority 50
                  :job/resource [{:resource/type :resource.type/cpus
                                  :resource/amount (double 1.0)}
                                 {:resource/type :resource.type/mem
                                  :resource/amount (double 10.0)}]
                  :job/state :job.state/running
                  :job/submit-time (Date.)
                  :job/under-investigation false
                  :job/user "seed_running_jobs_user"
                  :job/uuid (d/squuid)}
        tx-data [job-info commit-latch]]
    (d/resolve-tempid (d/db conn) (:tempids @(d/transact conn tx-data)) id)))

(defn create-running-job
  [conn name]
  (create-instance conn (create-job conn name)))

(try
  (let [conn (datomic/create-connection {:settings {:mesos-datomic-uri uri}})]
    (println "Connected to Datomic:" conn)
    (create-running-job conn "running_job_1")
    (create-running-job conn "running_job_2")
    (create-running-job conn "running_job_3")
    (create-running-job conn "running_job_4")
    (println "Running Jobs:")
    (run! clojure.pprint/pprint (util/get-running-job-ents (d/db conn)))
    (System/exit 0))
  (catch Throwable t
    (println "Failed to seed running jobs:" t)
    (System/exit 1)))
