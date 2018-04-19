(ns cook.mesos.pool
  (:require [clojure.tools.logging :as log]
            [cook.config :as config]
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

(defn guard-invalid-default-pool
  "Throws if either of the following is true:
   - there are pools in the database, but no default pool is configured
   - there is no pool in the database matching the configured default"
  [db]
  (let [pools (all-pools db)
        default-pool-name (config/default-pool)]
    (log/info "Pools in the database:" pools ", default pool:" default-pool-name)
    (if default-pool-name
      (when-not (some #(= default-pool-name (:pool/name %)) pools)
        (throw (ex-info "There is no pool in the database matching the configured default pool"
                        {:pools pools :default-pool-name default-pool-name})))
      (when (-> pools count pos?)
        (throw (ex-info "There are pools in the database, but no default pool is configured"
                        {:pools pools}))))))
