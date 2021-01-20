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
(ns cook.quota
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [cook.config :as config]
            [cook.datomic]
            [cook.pool :as pool]
            [cook.rate-limit]
            [cook.rate-limit.generic :as rtg]
            [cook.queries :as queries]
            [datomic.api :as d]
            [mount.core :as mount]
            [plumbing.core :as pc]))
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

(defn- resource-type->datomic-resource-type
  [type]
  (keyword "resource.type" (name type)))


(defn- get-quota-by-type
  [db type user pool-name]
  (let [type (resource-type->datomic-resource-type type)
        pool-name' (pool/pool-name-or-default pool-name)
        requesting-default-pool (pool/requesting-default-pool? pool-name)
        query '[:find ?a
                :in $ ?u ?t ?pool-name ?requesting-default-pool
                :where
                [?e :quota/user ?u]
                [?e :quota/resource ?r]
                [?r :resource/type ?t]
                [?r :resource/amount ?a]
                [(cook.pool/check-pool $ ?r :resource/pool ?pool-name
                                             ?requesting-default-pool)]]]
    (ffirst (d/q query db user type pool-name' requesting-default-pool))))

(defn- get-quota-by-type-or-default
  [db type user pool-name]
  (or (get-quota-by-type db type user pool-name)
      (get-quota-by-type db type default-user pool-name)
      Double/MAX_VALUE))

(defn get-quota-extra
  "Get quota for additional resource types"
  [db resource-type user pool-name default-value]
  (or (get-quota-by-type db resource-type user pool-name)
      (get-quota-by-type db resource-type default-user pool-name)
      default-value))

; Some defaults to be effectively infinity if you don't configure quotas explicitly.
; 10M jobs and 10k/sec sustained seems to have a lot of headroom. Don't want to go into the billions
; because of integer wraparound risks.
; These numbers are not round numbers so they're very greppable.
(def default-launch-rate-saved 10000097.)
(def default-launch-rate-per-minute 600013.)

(defn get-quota
  "Query a user's pre-defined quota.

   If a user's pre-defined quota is NOT defined, return the quota for the
   `default-user`. If there is NO `default-user` value for a specific type,
   return Double.MAX_VALUE."
  [db user pool-name]
  (let [mesos-resource-quota (pc/map-from-keys (fn [type] (get-quota-by-type-or-default db type user pool-name))
                                               (queries/get-all-resource-types db))
        ;; As part of the pool migration, there might be a mix of quotas that have the count as an attribute or a resource.
        ;; We prefer to read the resource if available and fall back to the attribute.
        count-quota (or (get-quota-by-type db :resource.type/count user pool-name) ; prefer resource
                        (:quota/count (d/entity db [:quota/user user])) ; then the field on the user
                        (get-quota-by-type db :resource.type/count default-user pool-name) ; then the resource from the default user
                        (:quota/count (d/entity db [:quota/user default-user])) ; then the field on the default user
                        Integer/MAX_VALUE)
        launch-rate-saved (get-quota-extra db
                                                     :resource.type/launch-rate-saved
                                                     user
                                                     pool-name
                                                     default-launch-rate-saved)
        launch-rate-per-minute (get-quota-extra
                                           db
                                           :resource.type/launch-rate-per-minute
                                           user
                                           pool-name
                                           default-launch-rate-per-minute)]
    (assoc mesos-resource-quota :count (int count-quota)
                                :launch-rate-saved launch-rate-saved
                                :launch-rate-per-minute launch-rate-per-minute)))

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
  (let [db (d/db conn)
        pool-name' (pool/pool-name-or-default pool-name)
        requesting-default-pool? (pool/requesting-default-pool? pool-name)
        resources (d/q '[:find [?r ...]
                         :in $ ?u ?pool-name ?requesting-default-pool
                         :where
                         [?s :quota/user ?u]
                         [?s :quota/resource ?r]
                         [(cook.pool/check-pool $ ?r :resource/pool ?pool-name
                                                      ?requesting-default-pool)]]
                       db user pool-name' requesting-default-pool?)
        _ (doseq [r resources]
            (maybe-flush-ratelimit pool-name user (:resource/type (d/entity db r))))
        resource-txns (mapv #(conj [:db.fn/retractEntity] %) resources)
        txns (conj resource-txns [:db/add [:quota/user user] :quota/reason reason])
        count-field (:quota/count (d/entity db [:quota/user user]))]

    @(d/transact conn (cond-> txns
                        ; If the user has a count field, remove that as well.
                        count-field (conj [:db/retract [:quota/user user] :quota/count count-field])))))

(defn set-quota!
  "Set the quota for a user. Note that the type of resource must be in the
   list of (get-all-resource-types)

   Usage:
   (set-quota! conn \"u1\" \"pool-a\" \"updating quota\" :cpus 20.0 :mem 10.0 :count 50)
   or
   (set-quota! conn \"u1\" \"pool-a\" \"updating quota\" :cpus 20.0)
   etc."
  [conn user pool-name reason & kvs]
  (let [pool-name' (pool/pool-name-or-default pool-name)
        requesting-default-pool? (pool/requesting-default-pool? pool-name)
        db (d/db conn)]
    (loop [[type amount & kvs] kvs
           txns []]
      (if (nil? amount)
        @(d/transact conn txns)
        (let [type (resource-type->datomic-resource-type type)
              resource (-> (d/q '[:find ?r
                                  :in $ ?user ?type ?pool-name ?requesting-default-pool
                                  :where
                                  [?e :quota/user ?user]
                                  [?e :quota/resource ?r]
                                  [?r :resource/type ?type]
                                  [(cook.pool/check-pool $ ?r :resource/pool ?pool-name
                                                               ?requesting-default-pool)]]
                                db user type pool-name' requesting-default-pool?)
                           ffirst)
              txn (if resource
                    [[:db/add resource :resource/amount (double amount)]]
                    (let [resource (cond-> {:resource/type type
                                            :resource/amount (double amount)}
                                     pool-name (assoc :resource/pool [:pool/name pool-name]))]
                      [{:db/id (d/tempid :db.part/user)
                        :quota/user user
                        :quota/resource [resource]}]))]
          (maybe-flush-ratelimit pool-name user type)
          (recur kvs (into txn txns)))))
    (let [count-field (:quota/count (d/entity db [:quota/user user]))]
      @(d/transact conn (cond-> [[:db/add [:quota/user user] :quota/reason reason]]
                          ; If the user had a legacy count attribute, retract that as part of this update.
                          count-field
                          (conj [:db/retract [:quota/user user] :quota/count count-field]))))))

(defn- retrieve-user->resource-quota-amount
  "Returns the user->amount map for all quota resources specified for the given pool and resource type.
   This function only supports the mechanism of storing quota counts as resources."
  [db pool-name resource-type]
  (let [type (resource-type->datomic-resource-type resource-type)
        pool-name' (pool/pool-name-or-default pool-name)
        requesting-default-pool (pool/requesting-default-pool? pool-name)
        query '[:find ?u ?a
                :in $ ?t ?pool-name ?requesting-default-pool
                :where
                [?e :quota/user ?u]
                [?e :quota/resource ?r]
                [?r :resource/type ?t]
                [?r :resource/amount ?a]
                [(cook.pool/check-pool $ ?r :resource/pool ?pool-name ?requesting-default-pool)]]]
    (->> (d/q query db type pool-name' requesting-default-pool)
      (into {}))))

(defn- retrieve-user->quota-attribute-count
  "Returns the user->count-quota for all count quota attributes specified on the user entity.
   This function only supports the legacy mechanism of storing quota counts as attributes."
  [db users]
  (let [query '[:find ?u ?c
                :in $ [?u ...]
                :where
                [?e :quota/user ?u]
                [?e :quota/count ?c]]]
    (->> (d/q query db users)
      (into {}))))

(defn- retrieve-user->count-quota
  "Returns the map for user to count-quota for all the provided users.
   It accounts for both the resource-based and attribute-based lookup of the count quota."
  [db pool-name all-users default-quota]
  (let [user->quota-resource-count (retrieve-user->resource-quota-amount db pool-name :count)
        remaining-users (vec (set/difference (set all-users) (set (keys user->quota-resource-count))))
        user->quota-attribute-count (when (seq remaining-users)
                                      (retrieve-user->quota-attribute-count db remaining-users))]
    (pc/map-from-keys
      (fn [user]
        (int
          ; As part of the pool migration, there might be a mix of quotas that have the count as an attribute or a resource.
          ; Hence, we prefer resource over the field on the user for count quota.
          ; Refer to the implementation of `get-quota` for further details.
          (or (get user->quota-resource-count user)
              (get user->quota-attribute-count user)
              default-quota)))
      all-users)))

(defn create-user->quota-fn
  "Returns a function which will return the quota same as `(get-quota db user)`
   snapshotted to the db passed in. However, it queries for all users with quota
   and returns the `default-user` value if a user is not returned.
   This is usefully if the application will go over ALL users during processing"
  [db pool-name]
  (let [default-type->quota (get-quota db default-user pool-name)
        default-count-quota (get default-type->quota :count)
        all-resource-types (queries/get-all-resource-types db)
        type->user->quota (pc/map-from-keys #(retrieve-user->resource-quota-amount db pool-name %) all-resource-types)
        all-quota-users (d/q '[:find [?user ...] :where [?q :quota/user ?user]] db) ;; returns a sequence without duplicates
        user->count-quota (retrieve-user->count-quota db pool-name all-quota-users default-count-quota)
        user->quota-cache (-> (pc/map-from-keys
                                (fn [user]
                                  (-> (pc/map-from-keys
                                        #(get-in type->user->quota [% user] (get default-type->quota %))
                                        all-resource-types)
                                    (assoc :count (user->count-quota user))))
                                all-quota-users)
                            (assoc default-user default-type->quota))]
    (fn user->quota
      [user]
      (or (get user->quota-cache user)
          (get user->quota-cache default-user)))))

(defn create-pool->user->quota-fn
  "Creates a function that takes a pool name, and returns an equivalent of user->quota-fn for each pool"
  [db]
  (let [all-pools (map :pool/name (pool/all-pools db))
        using-pools? (seq all-pools)]
    (if using-pools?
      (let [pool->quota-cache (pc/map-from-keys (fn [pool-name] (create-user->quota-fn db pool-name))
                                                all-pools)]
        (fn pool->user->quota
          [pool]
          (pool->quota-cache pool)))
      (let [quota-cache (create-user->quota-fn db nil)]
        (fn pool->user->quota
          [pool]
          quota-cache)))))
