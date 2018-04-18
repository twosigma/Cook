(ns cook.mesos.pool
  (:require [cook.config :as config]
            [datomic.api :as d])
  (:import [java.util UUID]))

(defn check-pool
  "Returns true if requesting-default-pool? and the entity does not have a pool
   or the entity has a pool named pool-name"
  [source eid entity->pool pool-name requesting-default-pool?]
  (let [ent (d/entity source eid)
        pool (entity->pool ent)]
    (or (and (nil? pool)
             requesting-default-pool?)
        (= pool-name (:pool/name pool)))))

(defn pool-name-or-default
  "Returns:
   - The given pool name if not-nil
   - The default pool name, if configured
   - A random UUID"
  [pool-name]
  (or pool-name (config/default-pool) (str (UUID/randomUUID))))

(defn requesting-default-pool?
  "Returns true if pool-name is nil or equal to the default pool name"
  [pool-name]
  (true? (or (nil? pool-name) (= pool-name (config/default-pool)))))

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

(defn accepts-submissions?
  "Returns true if the given pool can accept job submissions"
  [pool]
  (= :pool.state/active (:pool/state pool)))
