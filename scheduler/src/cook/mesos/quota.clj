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
  (:require [cook.mesos.schema]
            [cook.mesos.util :as util]
            [datomic.api :as d :refer (q)]))
;; This namespace is dangerously similar to cook.mesos.share (it was copied..)
;; it isn't obvious what the abstraction is, but there must be one.

(def default-user "default")

(defn- resource-type->datomic-resource-type
  [type]
  (keyword "resource.type" (name type)))

(defn- get-quota-by-type
  [db type user]
  (let [type (resource-type->datomic-resource-type type)
        query '[:find ?a
                :in $ ?u ?t
                :where
                [?e :quota/user ?u]
                [?e :quota/resource ?r]
                [?r :resource/type ?t]
                [?r :resource/amount ?a]]]
    (or (-> (q query db user type) ffirst)
        (-> (q query db default-user type) ffirst)
        (Double/MAX_VALUE))))

(defn- get-max-jobs-quota
  [db user]
  [:count
   (or (:quota/count (d/entity db [:quota/user user]))
       (:quota/count (d/entity db [:quota/user default-user]))
       (Double/MAX_VALUE))])

(defn get-quota
  "Query a user's pre-defined quota.

   If a user's pre-defined quota is NOT defined, return the quota for the
   `default-user`. If there is NO `default-user` value for a specific type,
   return Double.MAX_VALUE."
  [db user]
  (->> (util/get-all-resource-types db)
       (map (fn [type] [type (get-quota-by-type db type user)]))
       (cons (get-max-jobs-quota db user))
       (into {})))

(defn quota-history
  "Return changes to a user's own quota, in the form
  [{:time (inst when change occurred)
    :reason (stated reason for change)
    :quota (quota as would have been returned by get-quota after change)}]"
  [db user]
  (->> (d/q '[:find ?e ?a ?v ?added ?tx
              :in $ ?a ?e
              :where
              [?e ?a ?v ?tx ?added]]
            (d/history db)
            :quota/reason
            [:quota/user user])
       (sort-by last)
       (map (fn [[e attr v added tx]]
              (let [tx-db (d/as-of db tx)
                    tx-e (d/entity tx-db e)]
                {:time (:db/txInstant (d/entity db tx))
                 :reason (:quota/reason tx-e)
                 :quota (get-quota tx-db user)})))
       distinct))

(defn retract-quota!
  [conn user reason]
  @(d/transact conn [[:db/add [:quota/user user] :quota/reason reason]])
  @(d/transact conn [[:db.fn/retractEntity [:quota/user user]]]))

(defn set-quota!
  "Set the quota for a user. Note that the type of resource must be in the
   list of (get-all-resource-types)

   Usage:
   (set-quota! conn \"u1\" :cpus 20.0 :mem 10.0 :count 50)
   or
   (set-quota! conn \"u1\" :cpus 20.0)
   etc."
  [conn user reason & kvs]
  (loop [[type amount & kvs] kvs
         txns []]
    (if (nil? amount)
      @(d/transact conn txns)
      (if (= type :count)
        (recur kvs (into [{:db/id (d/tempid :db.part/user)
                                  :quota/user user
                                  :quota/count amount}]
                         txns))
        (let [type (resource-type->datomic-resource-type type)
              resource (-> (q '[:find ?r
                                :in $ ?user ?type
                                :where
                                [?e :quota/user ?user]
                                [?e :quota/resource ?r]
                                [?r :resource/type ?type]]
                              (d/db conn) user type)
                           ffirst)
              txn (if resource
                    [[:db/add resource :resource/amount amount]]
                    [{:db/id (d/tempid :db.part/user)
                      :quota/user user
                      :quota/resource [{:resource/type type
                                        :resource/amount amount}]}])]
          (recur kvs (into txn txns))))))
  @(d/transact conn [[:db/add [:quota/user user] :quota/reason reason]]))

(defn create-user->quota-fn
  "Returns a function which will return the quota same as `(get-quota db user)`
   snapshotted to the db passed in. However, it queries for all users with quota
   and returns the `default-user` value if a user is not returned.
   This is usefully if the application will go over ALL users during processing"
  [db]
  (let [all-quota-users (d/q '[:find [?user ...]
                               :where
                               [?q :quota/user ?user]]
                             db)
        user->quota-cache (->> all-quota-users
                               (map (fn [user]
                                      [user (get-quota db user)]))
                               ;; In case default-user doesn't have an explicit quota
                               (cons [default-user (get-quota db default-user)]) 
                               (into {}))]
    (fn user->quota
      [user]
      (or (get user->quota-cache user)
          (get user->quota-cache default-user)))))
