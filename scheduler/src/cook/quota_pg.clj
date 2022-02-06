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
(ns cook.quota-pg
  (:require [clojure.tools.logging :as log]
            [cook.config :as config]
            [cook.datomic]
            [cook.pool :as pool]
            [cook.rate-limit]
            [cook.rate-limit.generic :as rtg]
            [cook.resource-limit :as resource-limit]
            [datomic.api :as d]
            [mount.core :as mount]))
;; This namespace is dangerously similar to cook.share (it was copied..)
;; it isn't obvious what the abstraction is, but there must be one.

;; As part of the pool migration, count was migrated from a field on the share entity
;; to a resource. This was because the share and quota entities had an identity uniqueness
;; for the username attribute, and it was decided that it was cleaner to maintain that
;; constraint and add the pool to the resource entities (as opposed to adding pool to the
;; quote or share and making username non-unique.) Since users can have different count
;; quotas in different pools, we moved count to a resource. There was some precedent for
;; non-mesos resource types (uri) already so it seemed like a reasonable compromise.

(def default-user "default")

; Some defaults to be effectively infinity if you don't configure quotas explicitly.
; 10M jobs and 10k/sec sustained seems to have a lot of headroom. Don't want to go into the billions
; because of integer wraparound risks.
; These numbers are not round numbers so they're very greppable.
(def default-launch-rate-saved 10000097.)
(def default-launch-rate-per-minute 600013.)

; FIXME why this and not pool/pool-name-or-default
(defn defaultify-pool
  [pool-name]
  (or pool-name (config/default-pool) "default-pool"))

(defn get-quota
  "Query a user's pre-defined quota.

   If a user's pre-defined quota is NOT defined, return the quota for the
   `default-user`. If there is NO `default-user` value for a specific type,
   return Double.MAX_VALUE."
  [db user pool-name]
  (resource-limit/get-quota-pool-user (defaultify-pool pool-name) user))

(defn pool+user->token-key
  "Given a pool name and a user, create a key suitable for the per-user-per-pool ratelimit code"
  [pool-name user]
  {:pool-name (pool/pool-name-or-default pool-name) :user user})

(defn create-per-user-per-pool-launch-rate-limiter
  "From the configuration map, extract the keys that setup the per user launch rate limit config."
  [conn config]
  (log/info "Creating per-user-per-pool-launch-rate-limiter")
  (let [ratelimit-config (some-> config :settings :rate-limit :per-user-per-pool-job-launch)]
    (if (seq ratelimit-config)
      (do
        (log/info "Making per-user-launch rate limit config with" ratelimit-config)
        (rtg/make-generic-tbf-rate-limiter
          ratelimit-config
          (fn [{:keys [user pool-name] :as key}]
            (let [db (d/db conn)
                  {:keys [launch-rate-saved launch-rate-per-minute] :as quota}
                  (get-quota db user pool-name)]
              (log/info "For token-key" key "got quota" quota)
              (rtg/config->token-bucket-filter {:tokens-replenished-per-minute launch-rate-per-minute :bucket-size launch-rate-saved})))))
      (do
        (log/info "Not configuring per-user-launch rate because no configuration set")
        rtg/AllowAllRateLimiter))))

(mount/defstate per-user-per-pool-launch-rate-limiter
  :start (create-per-user-per-pool-launch-rate-limiter cook.datomic/conn config/config))

(def ratelimit-quota-fields #{:resource.type/launch-rate-saved
                              :resource.type/launch-rate-per-minute})


(defn maybe-flush-ratelimit
  "Look at the quota being updated (or retracted) and flush the rate limit
  if it's a rate-limit related quota"
  [pool-name user type]
  (when (contains? ratelimit-quota-fields type)
    (let [token-key (if (= user default-user)
                      nil ; Flush all users
                      (pool+user->token-key pool-name user))]
      (log/info "Flushing rate limit quota for" type "for" token-key)
      (cook.rate-limit/flush! per-user-per-pool-launch-rate-limiter token-key))))

(defn retract-quota!
  [conn user pool-name reason]
  (resource-limit/retract-quota! (defaultify-pool pool-name) user))

(defn set-quota!
  "Set the quota for a user. Note that the type of resource must be in the
   list of (get-all-resource-types)

   Usage:
   (set-quota! conn \"u1\" \"pool-a\" \"updating quota\" :cpus 20.0 :mem 10.0 :count 50)
   or
   (set-quota! conn \"u1\" \"pool-a\" \"updating quota\" :cpus 20.0)
   etc."
  [conn user pool-name reason & {:as args}]
  (resource-limit/set-quota! (defaultify-pool pool-name) user args reason))

(defn create-user->quota-fn
  "Returns a function which will return the quota same as `(get-quota db user)`
   snapshotted to the db passed in. However, it queries for all users with quota
   and returns the `default-user` value if a user is not returned.
   This is usefully if the application will go over ALL users during processing"
  [db pool-name]
  ; TODO: Cache should be a global cache we refresh every minute and use. See text in cook.quotsshare.
  (let [cache (resource-limit/sql-result->quotamap (resource-limit/get-all-resource-limits))]
    (fn [user] (resource-limit/get-quota-pool-user-from-cache cache (defaultify-pool pool-name) user))))

(defn create-pool->user->quota-fn
  "Creates a function that takes a pool name, and returns an equivalent of user->quota-fn for each pool"
  [db]
  ; TODO: Cache should be a global cache we refresh every minute and use. See text in cook.quotsshare.
  (let [cache (resource-limit/sql-result->quotamap (resource-limit/get-all-resource-limits))]
    (fn [pool-name]
      (fn [user] (resource-limit/get-quota-pool-user-from-cache cache (defaultify-pool pool-name) user)))))
