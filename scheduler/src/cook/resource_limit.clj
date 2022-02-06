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
(ns cook.resource-limit
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [cook.cache :as ccache]
            [cook.caches :as caches]
            [cook.config :as config]
            [cook.datomic]
            [cook.pool :as pool]
            [cook.postgres :as pg]
            [cook.rate-limit]
            [cook.rate-limit.generic :as rtg]
            [cook.queries :as queries]
            [datomic.api :as d]
            [mount.core :as mount]
            [next.jdbc :as sql]
            [plumbing.core :as pc]))

(def default-user "default")

(defn- net-map
  "A helper function for turning a list of tuples into a set of layered maps.
  Given a list of maps, a function for extracting a key from the maps, group the map
  by that key, then call another function on the underlying map."
  [amap key-fn sub-fn]
  (->> amap
       (group-by key-fn)
       (pc/map-vals sub-fn)))

(defn- sql-result->split-quotshares
  "Returns a map from 'quotssharetype -> pool -> user -> resource -> amount.
  Convenience debugging method to show an entire entire resource limit map"
  [sql-result]
  ;; BROKEN: NEEDS KEYWORDS HERE.
  (let [split-by-resource-name (fn [key] (net-map key :resource_name #(-> % first :amount)))
        split-by-user-name (fn [key] (net-map key :user_name split-by-resource-name))
        split-by-pool-name (fn [key] (net-map key :pool_name split-by-user-name))
        split-by-resource-limit-type (fn [key] (net-map key :resource_limit_type split-by-pool-name))]
    (split-by-resource-limit-type sql-result)))

(let [miss-fn
      (fn [_]
        (sql/execute! (pg/pg-db) ["SELECT resource_limit_type, pool_name, user_name, resource_name, amount from resource_limits"]))]
  (defn get-all-resource-limits
    "Get everything in resource_limits. Values might be cached."
    []
    ;(ccache/lookup-cache! caches/resource-limits identity miss-fn :all-resource-limits)
    ))

(defn query-quota-pool-user
  "Do a query for the quota for a specific user and pool in resource_limits"
  [pool user]
  (sql/execute! (pg/pg-db) ["SELECT resource_limit_type, pool_name, user_name, resource_name, amount from resource_limits WHERE resource_limit_type = 'quota' AND pool_name = ? and user_name = ?" pool user]))


; Some defaults to be effectively infinity if you don't configure quotas explicitly.
; 10M jobs and 10k/sec sustained seems to have a lot of headroom. Don't want to go into the billions
; because of integer wraparound risks.
; These numbers are not round numbers so they're very greppable.
(def default-launch-rate-saved 10000097.)
(def default-launch-rate-per-minute 600013.)

; Set of all mesos resource types (and default values)
(def all-mesos-resource-types {:cpus Double/MAX_VALUE :gpus Double/MAX_VALUE :mem Double/MAX_VALUE})
; Set of all quota-relevant resource types (and default values)
(def all-quota-resource-types (merge {:count Integer/MAX_VALUE :launch-rate-saved default-launch-rate-saved :launch-rate-per-minute default-launch-rate-per-minute} all-mesos-resource-types))

(defn make-quotadict-from-val
  "Given a :resource_name and :amount keys in a sql result, map them into the quota keywords (:count, :gpus, etc.) and assoc onto the result."
  [result {:keys [:resource_limits/resource_name :resource_limits/amount] :as tuple}]
  (assoc result
    (case resource_name
      "count" :count
      "cpus" :cpus
      "mem" :mem
      "gpus" :gpus
      "launch-rate-saved" :launch-rate-saved
      "launch-rate-per-minute" :launch-rate-per-minute)
    (if (= resource_name "count")
      (int amount)
      amount)))

(defn split-one-resource-type
  "Take either a sql result of quota or share and turn into a map
    pool -> user -> {:cpu ... :mem ... :count ... ...}"
  [quota-subset-sql-result]
  "Returns a map from 'pool -> user -> quota-map"
  (let [split-by-resource-name (fn [key] (reduce make-quotadict-from-val {} key))
        split-by-user-name (fn [key] (net-map key :resource_limits/user_name split-by-resource-name))
        split-by-pool-name (fn [key] (net-map key :resource_limits/pool_name split-by-user-name))]
    (split-by-pool-name quota-subset-sql-result)))

(defn sql-result->quotamap
  "Given a sql result, extract just the 'quota' fields and turn into a map:
     pool -> user -> {:cpu ... :mem ... :count ... ...}"
  [sql-result]
  (let [split-by-type (group-by :resource_limits/resource_limit_type sql-result)
        sqlresult-quota (get split-by-type "quota")]
    ; TODO: This should just cache all of the quota maps and refresh every 30 seconds into a global var for quota and share.
    (split-one-resource-type sqlresult-quota)))


; TODO: This shouldn't exist. We should just cache all of the quota maps and refresh every 30 seconds. This then just delecates to the cache case with the global cache.
(defn get-quota-dict-pool-user
  [pool-name user]
  (-> (query-quota-pool-user pool-name user)
    sql-result->quotamap
    (get-in [pool-name user])))

; TODO: This shouldn't exist. We should just cache all of the quota maps and refresh every 30 seconds. This then just delecates to the cache case with the global cache.
(defn get-quota-pool-user
  [pool-name user]
  (assert pool-name)
  (merge
    all-quota-resource-types
    (or (get-quota-dict-pool-user pool-name default-user) {})
    (or (get-quota-dict-pool-user pool-name user) {})))

(defn get-quota-dict-pool-user-from-cache
  [cache pool-name user]
  (get-in cache [pool-name user]))

(defn get-quota-pool-user-from-cache
  "Cache is a quota map, as returned from get-all-resource-limits and fed through sql-result->quotamap"
  [cache pool-name user]
  (assert pool-name)
  (merge
    all-quota-resource-types
    (or (get-quota-dict-pool-user-from-cache cache pool-name default-user) {})
    (or (get-quota-dict-pool-user-from-cache cache pool-name user) {})))

(defn retract-quota!
  "Retract quota."
  [pool user]
  (sql/execute! (pg/pg-db) ["DELETE FROM resource_limits WHERE resource_limit_type = 'quota' AND pool_name = ? and user_name = ?;" pool user]))

(defn quota-key-to-sql-key
  "Convert from quota keyword notation to sql resource_type"
  [keyword]
  (case keyword
    :count "count"
    :cpus "cpus"
    :mem "mem"
    :gpus "gpus"
    :launch-rate-saved "launch-rate-saved"
    :launch-rate-per-minute "launch-rate-per-minute" ))

(defn set-quota!
  [pool user kvs reason]
  (doseq [[key val] kvs]
    ; This is a bit overcomplicated to handle upsert logic, Insert or upate.
    (sql/execute! (pg/pg-db) ["insert into resource_limits as r (resource_limit_type,pool_name,user_name,resource_name,amount,reason) VALUES (?,?,?,?,?,?) ON CONFLICT (resource_limit_type,pool_name,user_name,resource_name) DO UPDATE set amount=excluded.amount, reason=excluded.reason where r.resource_limit_type = excluded.resource_limit_type AND r.pool_name = excluded.pool_name and r.user_name=excluded.user_name and r.resource_name = excluded.resource_name;" "quota" pool user (quota-key-to-sql-key key) val reason]))
  (sql/execute! (pg/pg-db) ["COMMIT;"]))

(defn truncate!
  "Reset the quota table between unit tests"
  []
  (sql/execute! (pg/pg-db) ["delete from resource_limits where true;"]))