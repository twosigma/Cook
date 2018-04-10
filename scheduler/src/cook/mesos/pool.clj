(ns cook.mesos.pool
  (:require [datomic.api :as d]))

(defn check-pool
  "Returns true if requesting-default-pool? and the entity does not have a pool
   or the entity has a pool named pool-name"
  [source eid relationship pool-name requesting-default-pool?]
  (let [ent (d/entity source eid)
        pool (relationship ent)]
    (or (and (nil? pool)
             requesting-default-pool?)
        (= pool-name (:pool/name pool)))))

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
