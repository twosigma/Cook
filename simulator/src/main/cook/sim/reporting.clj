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
  [cook-db [job-id action-at]]
  ;; TODO: better exception reporting in order to point the way to identifying
  ;; that the jobs weren't actually run by cook.
  (- (.getTime (-> (pull-job-entity cook-db job-id)
                   :job/instance first :instance/start-time))
     (.getTime action-at)))

(defn analyze
  [settings sim-db cook-db sim-id]
  (println "Analyzing sim " sim-id "...")
  (let [cook-db-val (-> cook-db :conn d/db)
        sim-db-val (-> sim-db :conn d/db)
        millis-for-jobs (map (partial wait-time cook-db-val)
                             (jobs-for-sim sim-db-val sim-id))
        avg-millis (/ (apply + millis-for-jobs) (count millis-for-jobs))]
    (println "Average wait time for jobs is" (float (/ avg-millis 1000)) "seconds.")))

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
