;;
;; Copyright (c) Two Sigma Open Source, LLC
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;  http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(ns cook.config-incremental
  (:require [clojure.tools.logging :as log]
            [cook.datomic :as datomic]
            [datomic.api :as d]
            [plumbing.core :refer [for-map map-from-vals]])
  (:import (java.util UUID)))

(defn get-conn
  "Get datomic database connection"
  []
  datomic/conn)

(defn incremental-values->incremental-value-ents
  "Convert plain map incremental values to datomic entities"
  [incremental-values]
  (map-indexed
    (fn [index {:keys [value portion comment]}]
      (cond->
        {:incremental-value/ordinal index
         :incremental-value/value value
         :incremental-value/portion portion}
        comment
        (assoc :incremental-value/comment comment)))
    incremental-values))

(defn write-configs
  "Write incremental configurations to the database."
  [incremental-configurations]
  @(d/transact
     (get-conn)
     (mapcat
       (fn [{:keys [key values]}]
         [; We need to retract existing incremental values; otherwise,
          ; values simply get added to the existing list
          [:db.fn/resetAttribute
           [:incremental-configuration/key key]
           :incremental-configuration/incremental-values
           nil]
          {:db/id (d/tempid :db.part/user)
           :incremental-configuration/key key
           :incremental-configuration/incremental-values (incremental-values->incremental-value-ents values)}])
       incremental-configurations)))

(defn incremental-value-ents->incremental-values
  "Convert datomic incremental value entities to plain maps"
  [incremental-value-ents]
  (map (fn [{:keys [:incremental-value/value :incremental-value/portion :incremental-value/comment]}]
         (cond->
           {:value value :portion portion}
           comment
           (assoc :comment comment)))
       (sort-by :incremental-value/ordinal incremental-value-ents)))

(defn read-config
  "Read an incremental configuration from the database."
  [key]
  (->> (d/entity (d/db (get-conn)) [:incremental-configuration/key key])
       :incremental-configuration/incremental-values
       incremental-value-ents->incremental-values))

(defn read-all-configs
  "Read all incremental configurations from the database."
  []
  (let [db (d/db (get-conn))
        configs (map #(d/entity db %)
                     (d/q '[:find [?incremental-configuration ...]
                            :where
                            [?incremental-configuration :incremental-configuration/key ?key]]
                          db))]
    (for-map [{:keys [:incremental-configuration/key :incremental-configuration/incremental-values]} configs]
      key
      (incremental-value-ents->incremental-values incremental-values))))

(defn select-config-from-values
  "Select one value for a uuid given incremental values."
  [uuid incremental-values]
  (try
    (when (not-empty incremental-values)
      (let [pct (-> uuid .hashCode Math/abs (/ Integer/MAX_VALUE) double)]
        (reduce (fn [portion-acc {:keys [value portion]}]
                  (let [total-portion (+ portion-acc portion)]
                    (if (>= total-portion pct) (reduced value) total-portion)))
                0.0
                incremental-values)))
    (catch Exception _
      (log/warn "Failed to select a value from incremental values."
                {:uuid uuid
                 :incremental-values incremental-values}))))

(defn select-config-from-key
  "Select one value for a uuid given a key to look up incremental values."
  [uuid config-key]
  (select-config-from-values uuid (read-config config-key)))

(defn resolve-incremental-config
  "Resolve an incremental config to the appropriate value.
  With an overload that takes a fallback config to use in case the incremental config cannot be resolved.
  If the incremental config cannot be resolved and there is no fallback then nil is returned and it is up to the caller
  to handle appropriately.

  An incremental configuration can either be a collection of incremental values or a keyword.
  If it is a keyword, then it is used as a key to look up a collection of incremental values in the database.
  An collection of incremental values is resolved by picking one of the values using the job uuid hash. Each incremental
  value has an associated portion and the portions must add up to 1.0"
  ([uuid incremental-config]
   (cond
     (coll? incremental-config)
     (select-config-from-values uuid incremental-config)
     (keyword? incremental-config)
     (select-config-from-key uuid incremental-config)))
  ([uuid incremental-config fallback-config]
   (let [resolved-incremental-config (resolve-incremental-config uuid incremental-config)]
     (if resolved-incremental-config
       [resolved-incremental-config :resolved-incremental-config]
       ; use a fallback config in case there is a problem resolving an incremental config
       [fallback-config :used-fallback-config]))))

