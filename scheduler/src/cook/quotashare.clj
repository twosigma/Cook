(ns cook.quotashare
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [clojure.java.jdbc :as sql]
            [cook.config :as config]
            [cook.datomic]
            [cook.pool :as pool]
            [cook.rate-limit]
            [cook.rate-limit.generic :as rtg]
            [cook.queries :as queries]
            [datomic.api :as d]
            [mount.core :as mount]
            [plumbing.core :as pc]))

(def default-user "default")

(def pg-db {:dbtype "postgresql"
            :dbname "cook_dev"
            :host "localhost"
            :user "twosigma"
            :password "fsadms8x7dsnmd7"
            :ssl true
            :sslfactory "org.postgresql.ssl.NonValidatingFactory"})

'(sql/query pg-db ["SELECT * from resource_limits;"])

(defn- net-map
  "A helper function for turning a list of tuples into a set of layered maps.
  Given a map, a function for extracting a key from the values, group the map by that key, then call another function on teh underlying map."
  [amap key-fn sub-fn]
  (->> amap
      (group-by key-fn)
       (pc/map-vals sub-fn)))

(defn- sql-result->split-quotshares
  [sql-result]
  "Returns a map from 'quotssharetype -> pool -> user -> resource -> amount (containing (EVERYTHING)"
  (let [split-by-resource-name (fn [key] (net-map key :resource_name #(-> % first :amount)))
        split-by-user-name (fn [key] (net-map key :user_name split-by-resource-name))
        split-by-pool-name (fn [key] (net-map key :pool_name split-by-user-name))
        split-by-resource-limit-type (fn [key] (net-map key :resource_limit_type split-by-pool-name))]
    (split-by-resource-limit-type sql-result)))


(defn query-quotashares-all
  []
  (sql/query pg-db ["SELECT resource_limit_type, pool_name, user_name, resource_name, amount from resource_limits"]))

(defn query-quota-pool-user
  [pool user]
  (sql/query pg-db ["SELECT resource_limit_type, pool_name, user_name, resource_name, amount from resource_limits WHERE resource_limit_type = 'quota' AND pool_name = ? and user_name = ?" pool user]))


; Some defaults to be effectively infinity if you don't configure quotas explicitly.
; 10M jobs and 10k/sec sustained seems to have a lot of headroom. Don't want to go into the billions
; because of integer wraparound risks.
; These numbers are not round numbers so they're very greppable.
(def default-launch-rate-saved 10000097.)
(def default-launch-rate-per-minute 600013.)

(def all-mesos-resource-types {:cpus 1000000000 :gpus 1000000 :mem 1000000000000000})
(def all-quota-resource-types (merge {:count 1000000000 :launch-rate-saved default-launch-rate-saved :launch-rate-per-minute default-launch-rate-per-minute} all-mesos-resource-types))

(defn make-quotadict-from-val
  [result {:keys [resource_name amount]}]
  (assoc result
    (case resource_name
      "count" :count
      "cpus" :cpus
      "mem" :mem
      "gpus" :gpus
      "launch-rate-saved" :launch-rate-saved
      "launch-rate-per-minute" :launch-rate-per-minute)
    amount))

;; A hint to show it all. Dead today. Should be dead tomorrow too.
(defn split-one-resource-type
  [quota-subset-sql-result]
  "Returns a map from 'pool -> user -> quota-map"
  (let [split-by-resource-name (fn [key] (reduce make-quotadict-from-val all-quota-resource-types key))
        split-by-user-name (fn [key] (net-map key :user_name split-by-resource-name))
        split-by-pool-name (fn [key] (net-map key :pool_name split-by-user-name))]
    (split-by-pool-name quota-subset-sql-result)))

(defn sql-result->quotamap
  [sql-result]
  (let [split-by-type (group-by "resource_limit_type" sql-result)
        sqlresult-quota (get "quota" split-by-type)]
    ; TODO: This just cache all of the quota maps and refresh every 30 seconds into a global var for quota and share.
    (split-one-resource-type sqlresult-quota)))

; TODO: This shouldn't exist. We should just cache all of the quota maps and refresh every 30 seconds.
(defn load-quota-pool-user
  [pool user]
  (println "Bar:" (query-quota-pool-user pool user))
  (sql-result->quotamap (query-quota-pool-user pool user)))

; TODO: This shouldn't exist. We should just cache all of the quota maps and refresh every 30 seconds. This then just delecates to the cache case with the global cache.
(defn get-quota-dict-pool-user
  [pool-name user]
  (println "FOO:" (load-quota-pool-user pool-name user))
  (get-in (sql-result->quotamap (load-quota-pool-user pool-name user)) [pool-name user]))

; TODO: This shouldn't exist. We should just cache all of the quota maps and refresh every 30 seconds. This then just delecates to the cache case with the global cache.
(defn get-quota-pool-user
  [pool-name user]
  (assert pool-name)
  (if-let [dict (get-quota-dict-pool-user pool-name user)]
    (merge all-quota-resource-types dict)
    (merge all-quota-resource-types (get-quota-dict-pool-user pool-name default-user))))

(defn get-quota-dict-pool-user-from-cache
  [cache pool-name user]
  (get-in cache [pool-name user]))

(defn get-quota-pool-user-from-cache
  [cache pool-name user]
  (assert pool-name)
  (if-let [dict (get-quota-dict-pool-user-from-cache cache pool-name user)]
    (merge all-quota-resource-types dict)
    (merge all-quota-resource-types (get-quota-dict-pool-user-from-cache cache pool-name default-user))))

(defn retract-quota!
  [pool user]
  (sql/execute! pg-db ["DELETE FROM person WHERE resource_limit_type = 'quota' AND pool_name = ? and user_name = ?; COMMIT" pool user]))

(defn quota-key-to-sql-key
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
  (let [tostore (merge all-quota-resource-types kvs)]
    (doseq [[key val] tostore]
      (sql/execute! pg-db ["insert into resource_limits as r (resource_limit_type,pool_name,user_name,resource_name,amount,reason) VALUES (?,?,?,?,?,?) ON CONFLICT (resource_limit_type,pool_name,user_name,resource_name) DO UPDATE set amount=excluded.amount, reason=excluded.reason where r.resource_limit_type = excluded.resource_limit_type AND r.pool_name = excluded.pool_name and r.user_name=excluded.pool_name and r.resource_name = excluded.resource_name;" "quota" pool user (quota-key-to-sql-key key) val reason]))
    (sql/execute! pg-db ["COMMIT;"])))

