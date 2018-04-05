(ns cook.mesos.pool
  (:require [datomic.api :as d]))

(defn all-pools
  "Returns a list of Datomic entities corresponding
  to all of the currently defined pools."
  [db]
  (map (partial d/entity db)
       (d/q '[:find [?e ...]
              :in $
              :where
              [?e :pool/name]]
            db)))

(defn pool-by-name
  "Returns the pool with the given name, or nil"
  [db pool-name]
  (->> (d/q '[:find ?e .
              :in $ ?pool-name
              :where
              [?e :pool/name ?pool-name]]
            db pool-name)
       (d/entity db)))

(defn accepts-submissions?
  "Returns true if the given pool can accept job submissions"
  [pool]
  (= :pool.state/active (:pool/state pool)))
