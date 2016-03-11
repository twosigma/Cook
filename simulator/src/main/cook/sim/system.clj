(ns cook.sim.system
  (:require [clojure.edn :as edn]
            [com.stuartsierra.component :as component]
            [datomic.api :as d]
            ))

(defrecord Config [path settings]
  component/Lifecycle

  (start [component]
    (println "Loading settings from " path)
    (assoc component :settings (-> path slurp edn/read-string)))

  (stop [component]
    (assoc component :settings nil)))

(defn new-config
  "Base system component; encapsulates application configuration.  Config
  is loaded from an edn file."
  [path]
  (map->Config {:path path}))


(defrecord SimDb [config conn]
  component/Lifecycle

  (start [component]
    (println "Connecting to simulation database...")
    (assoc component :conn (-> config :settings :sim-db-uri d/connect)))

  (stop [component]
    (assoc component :conn nil)))

(defn new-sim-db
  "SimDb is a Datomic database that stores everything the Simulator wants to remember
  about simulations - workload descriptors, the users therein, the jobs those users
  will request during a simulation, etc."
  ([] (map->SimDb {}))
  ([config] (map->SimDb {:config config})))


(defrecord CookDb [config conn]
  component/Lifecycle

  (start [component]
    (println "Connecting to Cook database...")
    (assoc component :conn (-> config :settings :cook-db-uri d/connect)))

  (stop [component]
    (assoc component :conn nil)))

(defn new-cook-db
  "CookDb is a reference to Cook Scheduler's own Datomic database.  Many functions
  of the Simulator depend on having a connection available to this database.  For
  example, the Cook database is queried to figure out what happened to various jobs
  in a Simulation in order to analyze how the Scheduler performed."
  ([] (map->CookDb {}))
  ([config] (map->CookDb {:config config})))


(defn system
  "Top level access point for all of the system components."
  [config-path]
  (component/system-map
   :config (new-config config-path)
   :sim-db (component/using (new-sim-db) [:config])
   :cook-db (component/using (new-cook-db) [:config])))
