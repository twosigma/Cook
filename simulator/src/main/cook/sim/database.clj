(ns cook.sim.database
  (:require [clojure.java.io :as io]
            [datomic.api :as d]))

(defn recreate-database!
  ([uri]
   (d/delete-database uri)
   (d/create-database uri)))

(defn load-schema
  [conn resource]
  (let [m (-> resource io/resource slurp read-string)]
    (doseq [v (vals m)
            tx v]
      @(d/transact conn tx))))

(defn setup-database!
  [settings]
  (prn "setting up the schema...")
  (-> settings :sim-db-uri recreate-database!)
  (let [conn (-> settings :sim-db-uri d/connect)]
    (load-schema conn "simulant/schema.edn")
    (load-schema conn "job_schedule.edn")))
