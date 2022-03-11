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
            [plumbing.core :as pc])
  (:import (com.google.common.cache CacheLoader CacheBuilder LoadingCache)
           (java.util.concurrent TimeUnit)))

(def default-user "default")

(defn- net-map
  "A helper function for turning a list of tuples into a set of layered maps.
  Given a list of maps, a function for extracting a key from the maps, group the map
  by that key, then call another function on the underlying map."
  [maps key-fn sub-fn]
  (->> maps
       (group-by key-fn)
       (pc/map-vals sub-fn)))

(defn make-resource-limit-map-from-sql-row
  "Given a :resource_name and :amount keys in a sql result, map them into the resource limit keywords (:count, :gpus, etc.) and assoc onto the result."
  [result {:keys [:resource_limits/resource_name :resource_limits/amount]}]
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

(defn sql-result->resource-limits-map
  "Take a sql result of querying resource_limits and turn into a map
    resource-limit-type -> pool -> user -> {:cpu ... :mem ... :count ... ...}"
  [sql-rows]
  "Returns a map from 'resource-limit-type -> pool -> user -> resource-limit-map"
  (let [split-by-resource-name (fn [sql-rows] (reduce make-resource-limit-map-from-sql-row {} sql-rows))
        split-by-user-name (fn [sql-rows] (net-map sql-rows :resource_limits/user_name split-by-resource-name))
        split-by-pool-name (fn [sql-rows] (net-map sql-rows :resource_limits/pool_name split-by-user-name))
        split-by-resource-limit-type (fn [sql-rows] (net-map sql-rows :resource_limits/resource_limit_type split-by-pool-name))]
    (split-by-resource-limit-type sql-rows)))

(defn query-all-resource-limits
  "Get everything in resource_limits"
  []
  (sql/execute! (pg/pg-db) ["SELECT resource_limit_type, pool_name, user_name, resource_name, amount from resource_limits"]))

(defn make-full-resource-limits-map
  "Query the database to make a full map of all resource limits - including for all resource limit types, pools, and users."
  []
  (sql-result->resource-limits-map (query-all-resource-limits)))

(defn make-resource-limits-cache []
  "Build a new cache for resource limits"
  (-> (CacheBuilder/newBuilder)
    (.maximumSize (or (get-in config/config [:settings :pg-config :resource-limit-cache-size]) 10000))
    (.expireAfterAccess (or (get-in config/config [:settings :pg-config :resource-limit-cache-expiry-time-seconds]) 30) TimeUnit/SECONDS)
    (.refreshAfterWrite (or (get-in config/config [:settings :pg-config :resource-limit-cache-refresh-time-seconds]) 10) TimeUnit/SECONDS)
    (.build (proxy [CacheLoader] []
              (load [_]
                (log/info "Loading resource-limits using CacheLoader")
                (make-full-resource-limits-map))))))

(def resource-limits-cache-atom (atom nil))
(def initialization-promise-atom (atom nil))

(defn initialize-resource-limits-cache!
  "Initialize the resource-limits-cache exactly once"
  []
  (let [p (promise)]
    (if (compare-and-set! initialization-promise-atom nil p)
      (let [^LoadingCache resource-limits-cache (make-resource-limits-cache)]
        (reset! resource-limits-cache-atom resource-limits-cache)
        (deliver p resource-limits-cache)
        resource-limits-cache)
      (deref @initialization-promise-atom 5000 nil))))

(defn ^LoadingCache get-resource-limits-cache
  "Get the resource-limits-cache. This method ensures it exists."
  []
  (or @resource-limits-cache-atom (initialize-resource-limits-cache!)))

(defn invalidate-resource-limits-cache!
  "Invalidate the resource-limits-cache"
  []
  (.invalidateAll (get-resource-limits-cache)))

(defn get-resource-limits-map
  "Get everything in resource_limits. Values might be cached."
  []
  (ccache/lookup-cache! (get-resource-limits-cache) identity
                        (fn [_]
                          (log/info "Loading resource-limits because of a cache miss")
                          (make-full-resource-limits-map))
                        :all-resource-limits))

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

(defn get-quota-pool-user
  [pool-name user]
  (assert pool-name)
  (let [user->quota (get-in (get-resource-limits-map) ["quota" pool-name])]
    (merge
      all-quota-resource-types
      (or (get user->quota default-user) {})
      (or (get user->quota user) {}))))

(defn get-share-pool-user
  [pool-name user]
  (assert pool-name)
  (let [user->quota (get-in (get-resource-limits-map) ["share" pool-name])]
    (merge
      all-mesos-resource-types
      (or (get user->quota default-user) {})
      (or (get user->quota user) {}))))

(defn retract-resource-limit!
  "Retract a resource limit."
  [resource-limit-type pool user reason]
  ; TODO: mark reason when we are not just deleting
  ; FIXME: why don't we need COMMIT?
  (sql/execute! (pg/pg-db) ["DELETE FROM resource_limits WHERE resource_limit_type = ? AND pool_name = ? and user_name = ?;" resource-limit-type pool user]))

(defn retract-quota!
  "Retract quota."
  [pool user reason]
  (retract-resource-limit! "quota" pool user reason))

(defn retract-share!
  "Retract share."
  [pool user reason]
  (retract-resource-limit! "share" pool user reason))

(defn resource-key-to-sql-key
  "Convert from resource keyword notation to sql resource_type"
  [keyword]
  (case keyword
    :count "count"
    :cpus "cpus"
    :mem "mem"
    :gpus "gpus"
    :launch-rate-saved "launch-rate-saved"
    :launch-rate-per-minute "launch-rate-per-minute"))

(defn set-resource-limit!
  [resource-type pool user kvs reason]
  (doseq [[key val] kvs]
    ; This is a bit overcomplicated to handle upsert logic, Insert or upate.
    (sql/execute! (pg/pg-db) ["insert into resource_limits as r (resource_limit_type,pool_name,user_name,resource_name,amount,reason) VALUES (?,?,?,?,?,?) ON CONFLICT (resource_limit_type,pool_name,user_name,resource_name) DO UPDATE set amount=excluded.amount, reason=excluded.reason where r.resource_limit_type = excluded.resource_limit_type AND r.pool_name = excluded.pool_name and r.user_name=excluded.user_name and r.resource_name = excluded.resource_name;" resource-type pool user (resource-key-to-sql-key key) val reason]))
  ; FIXME: does COMMIT do anything?
  (sql/execute! (pg/pg-db) ["COMMIT;"]))

(defn set-quota!
  [pool user kvs reason]
  (set-resource-limit! "quota" pool user kvs reason))

(defn set-share!
  [pool user kvs reason]
  (set-resource-limit! "share" pool user kvs reason))

(defn truncate!
  "Reset the resource_limits table between unit tests"
  []
  (sql/execute! (pg/pg-db) ["delete from resource_limits where true;"]))
