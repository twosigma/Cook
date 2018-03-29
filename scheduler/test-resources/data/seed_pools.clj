(ns data.seed-pools
  (:require [datomic.api :as d]))

(def uri (second *command-line-args*))
(println "Datomic URI is" uri)

(defn create-pool
  [conn name]
  (println "Creating pool" name)
  @(d/transact conn [{:db/id (d/tempid :db.part/user)
                      :pool/name name
                      :pool/purpose "This is a pool for testing purposes"
                      :pool/state :pool.state/active}]))

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
  (d/connect uri))

(try
  (let [conn (retry 10 connect)]
    (println "Connected to Datomic:" conn)
    (create-pool conn "alpha")
    (create-pool conn "beta")
    (create-pool conn "gamma")
    (create-pool conn "delta")
    (println "Pools:")
    (-> conn d/db pools clojure.pprint/pprint)
    (System/exit 0))
  (catch Throwable t
    (println "Failed to seed pools:" t)
    (System/exit 1)))
