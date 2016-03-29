(ns cook.sim.reporting
  (:require [clojure.pprint :as pprint]
            [datomic.api :as d]
            [cook.sim.database :as data]
            [cook.sim.util :as u]))

(defn jobs-for-sim
  [sim-db sim-id]
  (d/q '[:find ?jobid ?tx-time
         :in $ ?simid
         :where [?log :job/uuid ?jobid ?tx]
         [?log :actionLog/sim ?simid]
         [?tx :db/txInstant ?tx-time]]
       sim-db sim-id))

(defn pull-job-entity
  "Pulls a job entity from the cook database."
  [db uuid]
  (d/q '[:find (pull ?e [*]) .
         :in $ ?jid
         :where [?e :job/uuid ?jid]]
       db uuid))

(defn wait-time
  [scheduled-at first-cook-instance]
  (if first-cook-instance
    (- (.getTime (:instance/start-time first-cook-instance))
       (.getTime scheduled-at))))

(defn job-result
  [cook-db [job-id action-at]]
  (let [cook-job (pull-job-entity cook-db job-id)
        first-instance (-> cook-job :job/instance first)]
    {:id job-id
     :details cook-job
     :wait-time (wait-time action-at first-instance)}))

(defn warn-about-unscheduled
  [jobs]
  (let [unscheduled-jobs (remove :wait-time jobs)]
    (if (-> unscheduled-jobs count pos?)
      (do
       (println "Warning!  This simulation had jobs that were never scheduled by Cook.")
       (println "This could be because you are analyzing before the jobs had a chance to finish...")
       (println "or it could mean that there's a problem with Cook,")
       (println "or with the Simulator's ability to connect to Cook.")
       (doseq [job unscheduled-jobs]
         (println "Job" (:id job))
         (println "details from Cook DB:" (:details job)))))))

(defn show-average-wait
  [jobs]
  (let [wait-times (remove nil? (map :wait-time jobs))]
    (if (-> wait-times count pos?)
      (let [avg-wait (/ (apply + wait-times) (count wait-times))]
        (println "Average wait time for jobs is" (float (/ avg-wait 1000)) "seconds."))
      (println "No jobs were scheduled; thus, there is no average wait time."))))

(defn analyze
  [settings sim-db cook-db sim-id]
  (println "Analyzing sim " sim-id "...")
  (let [cook-db-val (-> cook-db :conn d/db)
        sim-db-val (-> sim-db :conn d/db)
        job-results (map (partial job-result cook-db-val)
                         (jobs-for-sim sim-db-val sim-id))]
    (println (count job-results) "jobs in sim...")
    (warn-about-unscheduled job-results)
    (show-average-wait job-results)))

(defn- format-sim-result
  "Prepares a simulation id result from db/pull for output via print-table"
  [db s]
  (let [id (:db/id s)]
    {"ID" id
     "When" (u/created-at db id)}))

(defn list-sims
  "A Command line task; prints out info about all sims for specified schedule"
  [sim-db id-num]
  (println "listing simulation runs for test" id-num "...")
  (let [db (-> sim-db :conn d/db)]
    (pprint/print-table (map (partial format-sim-result db)
                             (:test/sims (d/pull db "[:test/sims]" id-num))))))
