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
(ns cook.scheduler.share-pg
  (:require [clojure.tools.logging :as log]
            [cook.config :as config]
            [cook.pool :as pool]
            [cook.queries :as queries]
            [cook.resource-limit :as resource-limit]
            [datomic.api :as d]
            [metatransaction.core :refer [db]]
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

(defn defaultify-pool
  [pool-name]
  (or pool-name (config/default-pool) "default-pool"))

(defn get-share
  "Query a user's pre-defined share.

   If a user's pre-defined share is NOT defined, return the share for the
   \"default\" user. If there is NO \"default\" value for a specific type,
   return Double.MAX_VALUE."
  ([db user]
   (get-share db user nil))
  ([db user pool]
   (resource-limit/get-share-pool-user (defaultify-pool pool) user)))

(timers/deftimer [cook-mesos share get-shares-duration])

(defn get-shares
  "The result returned is equivalent to: (pc/map-from-keys #(get-share db %1) users).

   This function minimize db calls by locally caching the results for the default-user
   share and all the available resource-types."
  ([db users]
   (get-shares db users nil))
  ([db users pool-name]
   ;(timers/time!
   ;  get-shares-duration
   ;  (pc/map-from-keys (fn [user] (get-share db user pool-name)) users))
   (pc/map-from-keys (fn [user] (get-share db user pool-name)) users)
   ))

(defn retract-share!
  [conn user pool reason]
  (resource-limit/retract-share! (defaultify-pool pool) user reason))

(defn set-share!
  "Set the share for a user. Note that the type of resource must be in the
   list of (get-all-resource-types)

   Usage:
   (set-share! conn \"u1\" :cpus 20.0 :mem 10.0)
   or
   (set-share! conn \"u1\" :cpus 20.0)
   etc."
  [conn user pool-name reason & {:as args}]
  (resource-limit/set-share! (defaultify-pool pool-name) user args reason))

(timers/deftimer [cook-mesos share create-user->share-fn-duration])

(defn create-user->share-fn
  "Returns a function which will return the share same as `(get-share db user)`
   snapshotted to the db passed in. However, it queries for all users with share
   and returns the `default-user` value if a user is not returned.
   This is useful if the application will go over ALL users during processing"
  [db pool-name]
  ;(timers/time!
  ;  create-user->share-fn-duration
  ;  (let [defaulted-pool-name (defaultify-pool pool-name)]
  ;    (fn [user] (resource-limit/get-share-pool-user defaulted-pool-name user))))
  (let [defaulted-pool-name (defaultify-pool pool-name)]
    (fn [user] (resource-limit/get-share-pool-user defaulted-pool-name user)))
  )
