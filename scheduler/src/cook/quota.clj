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
            [cook.pool :as pool]
            [cook.schema]
            [cook.tools :as util]
            [datomic.api :as d]
            [metatransaction.core :refer [db]]
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

(defn get-quota
  "Query a user's pre-defined quota.

   If a user's pre-defined quota is NOT defined, return the quota for the
   `default-user`. If there is NO `default-user` value for a specific type,
   return Double.MAX_VALUE."
  [db user pool-name]
  (let [mesos-resource-quota (pc/map-from-keys (fn [type] (get-quota-by-type-or-default db type user pool-name))
                                               (util/get-all-resource-types db))
        ;; As part of the pool migration, there might be a mix of quotas that have the count as an attribute or a resource.
        ;; We prefer to read the resource if available and fall back to the attribute.
        count-quota (or (get-quota-by-type db :resource.type/count user pool-name) ; prefer resource
                        (:quota/count (d/entity db [:quota/user user])) ; then the field on the user
                        (get-quota-by-type db :resource.type/count default-user pool-name) ; then the resource from the default user
                        (:quota/count (d/entity db [:quota/user default-user])) ; then the field on the default user
                        Integer/MAX_VALUE)]
    (assoc mesos-resource-quota :count (int count-quota))))

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
        all-resource-types (util/get-all-resource-types db)
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
