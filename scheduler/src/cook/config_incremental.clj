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

(defn flat-values-array->weighted-values
  "Convert an incremental configuration specified by a flat array to a list of weighted values.

  e.g. convert [0.1 a 0.9 b] to
  [{:weighted-value/ordinal 0
  :weighted-value/weight 0.1
  :weighted-value/value a}
  {:weighted-value/ordinal 1
  :weighted-value/weight 0.9
  :weighted-value/value b}]"
  [flat-values-array]
  (map-indexed
    (fn [index [weight value]]
      {:weighted-value/ordinal index
       :weighted-value/weight weight
       :weighted-value/value value})
    (partition 2 flat-values-array)))

(defn weighted-values->flat-values-array
  "Reverse of flat-values-array->weighted-values"
  [weighted-values]
  (->> weighted-values
       (map (fn [{:keys [:weighted-value/weight :weighted-value/value]}] [weight value]))
       flatten))

(defn write-config
  "Write an incremental configuration to the database."
  [key flat-values-array]
  @(d/transact
     (get-conn)
     [; We need to retract existing weighted values; otherwise,
      ; values simply get added to the existing list
      [:db.fn/resetAttribute
       [:incremental-configuration/key key]
       :incremental-configuration/weighted-values
       nil]
      {:db/id (d/tempid :db.part/user)
       :incremental-configuration/key key
       :incremental-configuration/weighted-values (flat-values-array->weighted-values flat-values-array)}]))

(defn read-config
  "Read an incremental configuration from the database."
  [key]
  (->> (d/entity (d/db (get-conn)) [:incremental-configuration/key key])
       :incremental-configuration/weighted-values
       (sort-by :weighted-value/ordinal)))

(defn read-all-configs
  "Read all incremental configurations from the database."
  []
  (let [db (d/db (get-conn))
        configs (map #(d/entity db %)
                     (d/q '[:find [?incremental-configuration ...]
                            :where
                            [?incremental-configuration :incremental-configuration/key ?key]]
                          db))]
    (for-map [{:keys [:incremental-configuration/key :incremental-configuration/weighted-values]} configs]
      key
      (weighted-values->flat-values-array (sort-by :weighted-value/ordinal weighted-values)))))

(defn select-config-from-values
  "Select one value for a uuid given weighted values."
  [^UUID uuid weighted-values]
  (try
    (when (not-empty weighted-values)
      (let [pct (double (/ (Math/abs (.hashCode ^UUID uuid)) Integer/MAX_VALUE))]
        (reduce (fn [weight-acc {:keys [weighted-value/weight weighted-value/value]}]
                  (let [total-weight (+ weight-acc weight)]
                    (if (>= total-weight pct) (reduced value) total-weight)))
                0.0
                weighted-values)))
    (catch Exception _
      (log/warn "Failed to select incremental config from weighted values."
                {:uuid uuid
                 :weighted-values weighted-values}))))

(defn select-config-from-key
  "Select one value for a uuid given a key to loop up weighted values."
  [^UUID uuid config-key]
  (select-config-from-values uuid (read-config config-key)))

(defn resolve-incremental-config
  "Resolve an incremental config to the appropriate value."
  [^UUID uuid incremental-config]
  (cond
    (coll? incremental-config)
    (select-config-from-values uuid (flat-values-array->weighted-values incremental-config))
    (keyword? incremental-config)
    (select-config-from-key uuid incremental-config)))