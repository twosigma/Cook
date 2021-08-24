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
    (fn [index {:keys [value portion]}]
      {:incremental-value/ordinal index
       :incremental-value/value value
       :incremental-value/portion portion})
    incremental-values))

(defn write-config
  "Write an incremental configuration to the database."
  [key incremental-values]
  @(d/transact
     (get-conn)
     [; We need to retract existing incremental values; otherwise,
      ; values simply get added to the existing list
      [:db.fn/resetAttribute
       [:incremental-configuration/key key]
       :incremental-configuration/incremental-values
       nil]
      {:db/id (d/tempid :db.part/user)
       :incremental-configuration/key key
       :incremental-configuration/incremental-values (incremental-values->incremental-value-ents incremental-values)}]))

(defn incremental-value-ents->incremental-values
  "Convert datomic incremental value entities to plain maps"
  [incremental-value-ents]
  (map (fn [{:keys [:incremental-value/value :incremental-value/portion]}]
         {:value value :portion portion})
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
  [^UUID uuid incremental-values]
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
  [^UUID uuid config-key]
  (select-config-from-values uuid (read-config config-key)))

(defn resolve-incremental-config
  "Resolve an incremental config to the appropriate value."
  [^UUID uuid incremental-config]
  (cond
    (coll? incremental-config)
    (select-config-from-values uuid incremental-config)
    (keyword? incremental-config)
    (select-config-from-key uuid incremental-config)))