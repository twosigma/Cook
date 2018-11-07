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
            [cook.config :as config]
            [cook.mesos.pool :as pool]
            [cook.mesos.util :as util]
            [datomic.api :as d]
            [metrics.timers :as timers]
            [plumbing.core :as pc])
  (:import [java.util UUID]))

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
  [conn type user pool]
  (let [db (d/db conn)
        type (resource-type->datomic-resource-type type)
        resource (first (d/q '[:find [?r ...]
                               :in $ ?u ?t ?pool-name ?requesting-default-pool
                               :where
                               [?e :share/user ?u]
                               [?e :share/resource ?r]
                               [?r :resource/type ?t]
                               [?r :resource/amount ?a]
                               [(cook.mesos.pool/check-pool $ ?r :resource/pool ?pool-name
                                                            ?requesting-default-pool)]]
                             db user type (pool/pool-name-or-default pool)
                             (pool/requesting-default-pool? pool)))]
    (if resource
      @(d/transact conn
                   [[:db.fn/retractEntity resource]])
      (log/warn "Resource" type "for user" user "does not exist, could not retract"))))

(defn- filter-resource-entities
  "Takes a list of resource entities and returns a map from type to amount.
   If there are multiple resources for the same type, choose the one which exactly
   matches the default pool name."
  [resources]
  (let [default-pool (config/default-pool)
        reduce-vals (fn [resources]
                      (if (= 1 (count resources))
                        (first resources)
                        (first (filter #(= default-pool (get-in % [:resource/pool :pool/name]))
                                       resources))))]
    (->> resources
         (group-by :resource/type)
         (pc/map-vals reduce-vals)
         (pc/map-vals :resource/amount))))

(defn get-share-by-types
  "Query a user's pre-defined share by types (e.g. [:cpus, :mem, :gpus]).
   Returns a map from resource type to amount (double value).

   If a user's pre-defined share is NOT defined for a specific resource type, return either:
   1. the explicitly provided default value in type->share, or
   2. the share for the \"default\" user. If there is NO \"default\"
      value for a specific type, return Double.MAX_VALUE."
  [db user pool-name types type->share]
  (let [query '[:find [?r ...]
                :in $ ?u ?pool-name ?requesting-default-pool [?t ...]
                :where
                [?e :share/user ?u]
                [?e :share/resource ?r]
                [?r :resource/type ?t]
                [?r :resource/amount ?a]
                [(cook.mesos.pool/check-pool $ ?r :resource/pool ?pool-name
                                             ?requesting-default-pool)]]
        datomic-resource-types (map resource-type->datomic-resource-type types)
        default-type->share (pc/map-from-keys
                              (fn [type] (get type->share type Double/MAX_VALUE))
                              types)]

    (->> (d/q query db user (pool/pool-name-or-default pool-name)
              (pool/requesting-default-pool? pool-name) datomic-resource-types)
         (map (partial d/entity db))
         filter-resource-entities
         (pc/map-keys datomic-resource-type->resource-type)
         (merge default-type->share))))

(defn get-share
  "Query a user's pre-defined share.

   If a user's pre-defined share is NOT defined, return the share for the
   \"default\" user. If there is NO \"default\" value for a specific type,
   return Double.MAX_VALUE."
  ([db user]
   (get-share db user nil))
  ([db user pool]
   (get-share db user pool (util/get-all-resource-types db)))
  ([db user pool resource-types]
   (let [type->default (get-share-by-types db default-user pool resource-types {})]
     (get-share db user pool resource-types type->default)))
  ([db user pool resource-types type->default]
   (get-share-by-types db user pool resource-types type->default)))

(timers/deftimer [cook-mesos share get-shares-duration])

(defn get-shares
  "The result returned is equivalent to: (pc/map-from-keys #(get-share db %1) users).

   This function minimize db calls by locally caching the results for the default-user
   share and all the available resource-types."
  ([db users]
   (get-shares db users nil))
  ([db users pool-name]
   (get-shares db users pool-name (util/get-all-resource-types db)))
  ([db users pool-name resource-types]
   (timers/time!
     get-shares-duration
     (let [type->default (get-share db default-user pool-name resource-types {})]
       (-> (fn [user] (get-share db user pool-name resource-types type->default))
           (pc/map-from-keys users))))))

(defn retract-share!
  [conn user pool reason]
  (let [db (d/db conn)]
    (->> (util/get-all-resource-types db)
         (map (fn [type]
                [type (retract-share-by-type! conn type user pool)]))
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
  [conn user pool-name reason & kvs]
  (loop [[type amount & kvs] kvs
         txns []]
    (if (and amount (pos? amount))
      (let [type (resource-type->datomic-resource-type type)
            resource (-> (d/q '[:find ?r
                                :in $ ?user ?type ?pool-name ?requesting-default-pool
                                :where
                                [?e :share/user ?user]
                                [?e :share/resource ?r]
                                [?r :resource/type ?type]
                                [(cook.mesos.pool/check-pool $ ?r :resource/pool ?pool-name
                                                             ?requesting-default-pool)]]
                              (d/db conn) user type (pool/pool-name-or-default pool-name)
                              (pool/requesting-default-pool? pool-name))
                         ffirst)
            txn (if resource
                  [[:db/add resource :resource/amount amount]]
                  (let [resource (cond-> {:resource/type type
                                          :resource/amount amount}
                                   pool-name (assoc :resource/pool [:pool/name pool-name]))]
                    [{:db/id (d/tempid :db.part/user)
                      :share/user user
                      :share/resource [resource]}]))]
        (recur kvs (into txn txns)))
      @(d/transact conn txns)))
  @(d/transact conn [[:db/add [:share/user user] :share/reason reason]]))

(timers/deftimer [cook-mesos share create-user->share-fn-duration])

(defn create-user->share-fn
  "Returns a function which will return the share same as `(get-share db user)`
   snapshotted to the db passed in. However, it queries for all users with share
   and returns the `default-user` value if a user is not returned.
   This is useful if the application will go over ALL users during processing"
  [db pool-name]
  (timers/time!
    create-user->share-fn-duration
    (let [all-share-users (d/q '[:find [?user ...]
                                 :where
                                 [?q :share/user ?user]]
                               db)
          default-user-share (get-share db default-user pool-name)
          user->share-cache (-> (get-shares db all-share-users pool-name)
                                ;; In case default-user doesn't have an explicit share
                                (assoc default-user default-user-share))]
      (fn user->share
        [user]
        (or (get user->share-cache user)
            default-user-share)))))
