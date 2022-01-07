(ns data.seed-k8s-pools
  (:require [cook.datomic :as datomic]
            [cook.postgres :as pg]
            [cook.quota :as quota]
            [datomic.api :as d]))

(def uri (second *command-line-args*))
(println "Datomic URI is" uri)

(defn create-pool
  [conn name state]
  (println "Creating pool" name)
  @(d/transact conn [{:db/id (d/tempid :db.part/user)
                      :pool/name name
                      :pool/purpose "This is a pool for testing purposes"
                      :pool/state state
                      :pool/dru-mode :pool.dru-mode/default}]))

(defn pools
  [db]
  (->> (d/q '[:find [?p ...]
              :in $ [?state ...]
              :where
              [?p :pool/state ?state]]
            db [:pool.state/active :pool.state/inactive])
       (map (partial d/entity db))
       (map d/touch)))

(try
  (let [conn (datomic/create-connection {:settings {:mesos-datomic-uri uri}})]
    (->> (System/getenv "COOK_DB_TEST_PG_SCHEMA")
         (pg/make-database-connection-dictionary-from-env-vars)
         (reset! pg/saved-pg-config-dictionary))
    (println "Connected to Datomic:" conn)
    (create-pool conn "k8s-alpha" :pool.state/active)
    (create-pool conn "k8s-beta" :pool.state/inactive)
    (create-pool conn "k8s-gamma" :pool.state/active)
    (create-pool conn "k8s-delta" :pool.state/inactive)
    (create-pool conn "k8s-quota" :pool.state/active)
    (quota/set-quota! conn "default" "k8s-alpha" "For quota-related testing." :cpus 8 :mem 1024)
    (quota/set-quota! conn "default" "k8s-gamma" "For quota-related testing." :cpus 9 :mem 2048)
    (println "Pools & Quotas:")
    (run! (fn [{:keys [pool/name] :as p}]
            (clojure.pprint/pprint p)
            (clojure.pprint/pprint (quota/get-quota (d/db conn) "default" name)))
          (pools (d/db conn)))
    (System/exit 0))
  (catch Throwable t
    (println "Failed to seed pools:" t)
    (System/exit 1)))
