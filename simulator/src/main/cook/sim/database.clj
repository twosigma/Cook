(ns cook.sim.database
  (:require [clojure.java.io :as io]
            [datomic.api :as d]))

(defn recreate-database!
  "Given a Datomic database uri, deletes any existing database at the database,
  and creates a new one."
  [uri]
  (d/delete-database uri)
  (d/create-database uri))

(defn load-schema
  "Given a Datomic database connection and an IO resource location (e.g. filename),
  transacts the contents of the file."
  [conn resource]
  (let [m (-> resource io/resource slurp read-string)]
    (doseq [v (vals m)
            tx v]
      @(d/transact conn tx))))

(defn setup-database!
  "Given the settings, loads both the Simulant schema and extensions to support
  Cook Simulator specifically into the database at :sim-db-uri"
  [settings]
  (prn "setting up the schema...")
  (-> settings :sim-db-uri recreate-database!)
  (let [conn (-> settings :sim-db-uri d/connect)]
    (load-schema conn "simulant/schema.edn")
    (load-schema conn "job_schedule.edn")))
