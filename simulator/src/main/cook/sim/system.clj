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
    (assoc component :settings nil)
    ))

(defn new-config [path]
  (map->Config {:path path}))


(defrecord SimDb [config conn]
  component/Lifecycle

  (start [component]
    (println "Connecting to simulation database...")
    (assoc component :conn (-> config :settings :sim-db-uri d/connect)))

  (stop [component]
    (println ";; Stopping SimDb")
    (assoc component :conn nil)
    ))

(defn new-sim-db
  ([] (map->SimDb {}))
  ([config] (map->SimDb {:config config})))


(defrecord CookDb [config conn]
  component/Lifecycle

  (start [component]
    (println "Connecting to Cook database...")
    (assoc component :conn (-> config :settings :cook-db-uri d/connect)))

  (stop [component]
    (assoc component :conn nil)
    ))

(defn new-cook-db
  ([] (map->CookDb {}))
  ([config] (map->CookDb {:config config})))


(defn system
  [config-path]
  (component/system-map
   :config (new-config config-path)
   :sim-db (component/using (new-sim-db) [:config])
   :cook-db (component/using (new-cook-db) [:config])))
