(ns cook.sim.util
  (:require [datomic.api :as d]))

(defn transaction-times
  "Given a Datomic db snapshot and an entity id, returns the times associated with
  all transactions affecting the entity."
  [db eid]
  (->> (d/q '[:find ?instant
              :in $ ?e
              :where
              [?e _ _ ?tx]
              [?tx :db/txInstant ?instant]]
            (d/history db) eid)
       (map first)
       (sort)))

(defn created-at
  "Given a Datomic db snapshot and an entity id, returns the time when the entity
  was first created (first transaction)."
  [db eid]
  (first (transaction-times db eid)))

(defn updated-at
  "Given a Datomic db snapshot and an entity id, returns the time when the entity
  was last updated (last transaction)."
  [db eid]
  (last (transaction-times db eid)))
