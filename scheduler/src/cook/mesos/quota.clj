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
(ns cook.mesos.quota
  (:require [cook.mesos.pool :as pool]
            [cook.mesos.schema]
            [cook.mesos.util :as util]
            [datomic.api :as d]
            [plumbing.core :as pc]))
;; This namespace is dangerously similar to cook.mesos.share (it was copied..)
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
                [(cook.mesos.pool/check-pool $ ?r :resource/pool ?pool-name
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
                         [(cook.mesos.pool/check-pool $ ?r :resource/pool ?pool-name
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
                                  [(cook.mesos.pool/check-pool $ ?r :resource/pool ?pool-name
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

(defn create-user->quota-fn
  "Returns a function which will return the quota same as `(get-quota db user)`
   snapshotted to the db passed in. However, it queries for all users with quota
   and returns the `default-user` value if a user is not returned.
   This is usefully if the application will go over ALL users during processing"
  [db pool-name]
  (let [all-quota-users (d/q '[:find [?user ...]
                               :where
                               [?q :quota/user ?user]]
                             db)
        user->quota-cache (->> all-quota-users
                               (map (fn [user]
                                      [user (get-quota db user pool-name)]))
                               ;; In case default-user doesn't have an explicit quota
                               (cons [default-user (get-quota db default-user pool-name)])
                               (into {}))]
    (fn user->quota
      [user]
      (or (get user->quota-cache user)
          (get user->quota-cache default-user)))))
