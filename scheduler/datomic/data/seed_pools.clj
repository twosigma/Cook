(ns data.seed-pools
  (:require [cook.datomic :as datomic]
            [cook.mesos.quota :as quota]
            [datomic.api :as d]))

(def uri (second *command-line-args*))
(println "Datomic URI is" uri)

(defn create-pool
  [conn name state]
  (println "Creating pool" name)
  @(d/transact conn [{:db/id (d/tempid :db.part/user)
                      :pool/name name
                      :pool/purpose "This is a pool for testing purposes"
                      :pool/state state}]))

(defn pools
  [db]
  (->> (d/q '[:find [?p ...]
              :in $ [?state ...]
              :where
              [?p :pool/state ?state]]
            db [:pool.state/active :pool.state/inactive])
       (map (partial d/entity db))
       (map d/touch)))

(defn retry
  [tries f & args]
  (let [res (try {:value (apply f args)}
                 (catch Exception e
                   (if (= 0 tries)
                     (throw e)
                     {:exception e})))]
    (if (:exception res)
      (do
        (Thread/sleep 5000)
        (recur (dec tries) f args))
      (:value res))))

(defn connect
  []
  (println "Attempting to connect to" uri)
  (datomic/create-connection {:settings {:mesos-datomic-uri uri}}))

(try
  (let [conn (retry 10 connect)]
    (println "Connected to Datomic:" conn)
    (create-pool conn "alpha" :pool.state/active)
    (create-pool conn "beta" :pool.state/inactive)
    (create-pool conn "gamma" :pool.state/active)
    (create-pool conn "delta" :pool.state/inactive)
    (quota/set-quota! conn "default" "alpha" "For quota-related testing." :cpus 8 :mem 1024)
    (quota/set-quota! conn "default" "gamma" "For quota-related testing." :cpus 9 :mem 2048)
    (println "Pools & Quotas:")
    (run! (fn [{:keys [pool/name] :as p}]
            (clojure.pprint/pprint p)
            (clojure.pprint/pprint (quota/get-quota (d/db conn) "default" name)))
          (pools (d/db conn)))
    (System/exit 0))
  (catch Throwable t
    (println "Failed to seed pools:" t)
    (System/exit 1)))
