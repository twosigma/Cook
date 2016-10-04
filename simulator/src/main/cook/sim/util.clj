(ns cook.sim.util
  (:require [datomic.api :as d]))

(defn transaction-times
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
  [db eid]
  (first (transaction-times db eid)))

(defn updated-at
  [db eid]
    (last (transaction-times db eid)))
