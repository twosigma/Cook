(ns cook.pool
  (:require [clojure.tools.logging :as log]
            [cook.config :as config]
            [datomic.api :as d]))

; this is used in share and quota applications that look at per-pool resources
(defn check-pool
  "Returns true if requesting-default-pool? and the entity does not have a pool
   or the entity has a pool named pool-name"
  ([ent entity->pool pool-name requesting-default-pool?]
   (let [pool (entity->pool ent)]
     (or (and (nil? pool)
              requesting-default-pool?)
         (= pool-name (:pool/name pool)))))
  ([source eid entity->pool pool-name requesting-default-pool?]
   (check-pool (d/entity source eid) entity->pool pool-name requesting-default-pool?)))

(def nil-pool "nil-pool")

; this is used in job submission applications where users are primarily concerned
; with submit-pool
(defn check-pool-and-submit-pool
  "Returns true if requesting-default-pool? and the entity does not have a pool
   or the entity has either a pool or a submit-pool named pool-name"
  ([ent entity->pool pool-name requesting-default-pool?]
   (let [pool (entity->pool ent)
         submit-pool (:job/submit-pool-name ent)]
     (or (and (nil? pool)
              requesting-default-pool?)
         (= pool-name (:pool/name pool))
         (= pool-name submit-pool)))))

(defn check-pool-for-listing
  "Returns true if either the provided pool-name is the 'nil' pool, or if
  pool/check-pool returns true. This allows us to return jobs in all pools when the user
  does not specify a pool."
  ([entity entity->pool pool-name default-pool?]
   (or
     (= nil-pool pool-name)
     (cook.pool/check-pool-and-submit-pool entity entity->pool pool-name default-pool?)))
  ([source eid entity->pool pool-name default-pool?]
   (check-pool-for-listing (d/entity source eid) entity->pool pool-name default-pool?)))

(defn pool-name-or-default
  "Returns:
   - The given pool name if not-nil
   - The default pool name, if configured
   - `nil-pool`
   Returning `nil-pool` instead of `nil` allows this to be used as an argument to a datomic function
   or comparison operator"
  [pool-name]
  (or pool-name (config/default-pool) nil-pool))

(defn default-pool?
  "Returns true if pool-name is equal to the default pool name"
  [pool-name]
  (= pool-name (config/default-pool)))

(defn requesting-default-pool?
  "Returns true if pool-name is nil or equal to the default pool name"
  [pool-name]
  (true? (or (nil? pool-name) (default-pool? pool-name))))

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

(defn schedules-jobs?
  "Returns true if the given pool should schedule jobs"
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