(ns cook.sim.repl
  (:require [clojure.java.shell :use sh]
            [clojure.data.generators :as gen]
            [datomic.api :as d]
            [cook.sim.database :as db]
            [cook.sim.schedule :as schedule]
            [cook.sim.runner :as runner]
            [cook.sim.reporting :as report]
            [cook.sim.system :as sys]
            [com.stuartsierra.component :as component]
            [reloaded.repl :refer [system init start stop go reset reset-all]]
            ))

(reloaded.repl/set-init! #(sys/system "config/settings.edn"))

(defn list-models
  [{conn :conn}]
  (prn "listing models...")
  (let [db (d/db conn)]
    (prn (d/q '[:find [?e ...]
                :where [?e :model/type :model.type/cook]]
              db))))

(defn settings
  []
  (-> system :config :settings))

(defn sim-db
  []
  (-> system :sim-db))

(defn sim-db-val
  []
  (-> (sim-db) :conn d/db))

(defn cook-db
  []
  (-> system :cook-db))

(defn cook-db-val
  []
  (-> (cook-db) :conn d/db))

(defn setup
  []
  (db/setup-database! {})
  (schedule/generate-job-schedule! {}))

(defn kill-cook-db
  []
  (-> (settings) :cook-db-uri datomic.api/delete-database))

(defn backup-cook!
  [{sim-id :entity-id}]
  (let [{:keys [backup-directory cook-db-uri]} settings
        backup-dest (str backup-directory sim-id)]
    (println "backing up cook db up to " backup-dest)
    (println (sh "datomic" "backup-db" cook-db-uri backup-dest))))

(defn pull-entity
  [db eid]
  (d/pull db "[*]" eid))

(comment
  (if backup-after-seconds
    (do
      (println "waiting " backup-after-seconds "seconds before backing up...")
      (Thread/sleep (* 1000 backup-after-seconds))
      (backup-cook! {:entity-id (:db/id cook-sim)}))
    (println "Not configured to do a backup of cook db.")))
