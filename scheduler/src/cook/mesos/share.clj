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
(ns cook.mesos.share
  (:require [clojure.tools.logging :as log]
            [cook.mesos.util :as util]
            [datomic.api :as d :refer (q)]
            [metatransaction.core :refer (db)]
            [metrics.timers :as timers]
            [plumbing.core :as pc]))

(def default-user "default")

(defn- resource-type->datomic-resource-type
  "Converts the resource type to a datomic resource type keyword, e.g. :cpus to :resource.type/cpus"
  [type]
  (keyword "resource.type" (name type)))

(defn- datomic-resource-type->resource-type
  "Converts the datomic resource type to a resource type keyword, e.g. :resource.type/cpus to :cpus"
  [type]
  (keyword (name type)))

(defn- retract-share-by-type!
  [conn type user]
  (let [db (db conn)
        type (resource-type->datomic-resource-type type)
        resource (ffirst (q '[:find ?r
                              :in $ ?u ?t
                              :where
                              [?e :share/user ?u]
                              [?e :share/resource ?r]
                              [?r :resource/type ?t]
                              [?r :resource/amount ?a]]
                            db user type))]
    (if resource
      @(d/transact conn
                   [[:db.fn/retractEntity resource]])
      (log/warn "Resource" type "for user" user "does not exist, could not retract"))))

(defn get-share-by-types
  "Query a user's pre-defined share by types (e.g. [:cpus, :mem, :gpus]).
   Returns a map from resource type to amount (double value).

   If a user's pre-defined share is NOT defined for a specific resource type, return either:
   1. the explicitly provided default value in type->share, or
   2. the share for the \"default\" user. If there is NO \"default\"
      value for a specific type, return Double.MAX_VALUE."
  [db user types type->share]
  (let [query '[:find ?t ?a
                :in $ ?u [?t ...]
                :where
                [?e :share/user ?u]
                [?e :share/resource ?r]
                [?r :resource/type ?t]
                [?r :resource/amount ?a]]
        datomic-resource-types (map resource-type->datomic-resource-type types)
        default-type->share (pc/map-from-keys
                              (fn [type] (get type->share type Double/MAX_VALUE))
                              types)]
    (->> (q query db user datomic-resource-types)
         (into {})
         (pc/map-keys datomic-resource-type->resource-type)
         (merge default-type->share))))

(defn get-share
  "Query a user's pre-defined share.

   If a user's pre-defined share is NOT defined, return the share for the
   \"default\" user. If there is NO \"default\" value for a specific type,
   return Double.MAX_VALUE."
  ([db user]
   (get-share db user (util/get-all-resource-types db)))
  ([db user resource-types]
   (let [type->default (get-share-by-types db default-user resource-types {})]
     (get-share db user resource-types type->default)))
  ([db user resource-types type->default]
   (get-share-by-types db user resource-types type->default)))

(timers/deftimer [cook-mesos share get-shares-duration])

(defn get-shares
  "The result returned is equivalent to: (pc/map-from-keys #(get-share db %1) users).

   This function minimize db calls by locally caching the results for the default-user
   share and all the available resource-types."
  ([db users]
   (get-shares db users (util/get-all-resource-types db)))
  ([db users resource-types]
   (timers/time!
     get-shares-duration
     (let [type->default (get-share db default-user resource-types {})]
       (-> (fn [user] (get-share db user resource-types type->default))
           (pc/map-from-keys users))))))

(defn share-history
  "Return changes to a user's own share, in the form
  [{:time (inst when change occurred)
    :reason (stated reason for change)
    :share (share as would have been returned by get-share after change)}]"
  [db user]
  (->> (d/q '[:find ?e ?a ?v ?added ?tx
              :in $ ?a ?e
              :where
              [?e ?a ?v ?tx ?added]]
            (d/history db)
            :share/reason
            [:share/user user])
       (sort-by last)
       (map (fn [[e attr v added tx]]
              (let [tx-db (d/as-of db tx)
                    tx-e (d/entity tx-db e)]
                {:time (:db/txInstant (d/entity db tx))
                 :reason (:share/reason tx-e)
                 :share (get-share tx-db user)})))
       distinct))

(defn retract-share!
  [conn user reason]
  (let [db (d/db conn)]
    (->> (util/get-all-resource-types db)
         (map (fn [type]
                [type (retract-share-by-type! conn type user)]))
         (into {})))
  @(d/transact conn [[:db/add [:share/user user] :share/reason reason]]))

(defn set-share!
  "Set the share for a user. Note that the type of resource must be in the
   list of (get-all-resource-types)

   Usage:
   (set-share! conn \"u1\" :cpus 20.0 :mem 10.0)
   or
   (set-share! conn \"u1\" :cpus 20.0)
   etc."
  [conn user reason & kvs]
  (loop [[type amount & kvs] kvs
         txns []]
    (if (and amount (pos? amount))
      (let [type (resource-type->datomic-resource-type type)
            resource (-> (q '[:find ?r
                              :in $ ?user ?type
                              :where
                              [?e :share/user ?user]
                              [?e :share/resource ?r]
                              [?r :resource/type ?type]]
                            (d/db conn) user type)
                         ffirst)
            txn (if resource
                  [[:db/add resource :resource/amount amount]]
                  [{:db/id (d/tempid :db.part/user)
                    :share/user user
                    :share/resource [{:resource/type type
                                      :resource/amount amount}]}])]
        (recur kvs (into txn txns)))
      @(d/transact conn txns)))
  @(d/transact conn [[:db/add [:share/user user] :share/reason reason]]))

(timers/deftimer [cook-mesos share create-user->share-fn-duration])

(defn create-user->share-fn
  "Returns a function which will return the share same as `(get-share db user)`
   snapshotted to the db passed in. However, it queries for all users with share
   and returns the `default-user` value if a user is not returned.
   This is useful if the application will go over ALL users during processing"
  [db]
  (timers/time!
    create-user->share-fn-duration
    (let [all-share-users (d/q '[:find [?user ...]
                                 :where
                                 [?q :share/user ?user]]
                               db)
          default-user-share (get-share db default-user)
          user->share-cache (-> (get-shares db all-share-users)
                                ;; In case default-user doesn't have an explicit share
                                (assoc default-user default-user-share))]
      (fn user->share
        [user]
        (or (get user->share-cache user)
            default-user-share)))))
