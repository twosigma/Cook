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
            cook.mesos.schema))

(def default-user "default")

(defn- resource-type->datomic-resource-type
  [type]
  (keyword "resource.type" (name type)))

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

(defn- get-share-by-type
  [db type user]
  (let [type (resource-type->datomic-resource-type type)
        query '[:find ?a
                :in $ ?u ?t
                :where
                [?e :share/user ?u]
                [?e :share/resource ?r]
                [?r :resource/type ?t]
                [?r :resource/amount ?a]]]
    (or (-> (q query db user type) ffirst)
        (-> (q query db default-user type) ffirst)
        (Double/MAX_VALUE))))

(defn get-share
  "Query a user's pre-defined share.

   If a user's pre-defined share is NOT defined, return the share for the
   \"default\" user. If there is NO \"default\" value for a specific type,
   return Double.MAX_VALUE."
  [db user]
  (->> (util/get-all-resource-types db)
       (map (fn [type] [type (get-share-by-type db type user)]))
       (into {})))

(defn retract-share!
  [conn user]
  (let [db (d/db conn)]
    (->> (util/get-all-resource-types db)
         (map (fn [type]
                [type (retract-share-by-type! conn type user)]))
         (into {}))))

(defn set-share!
  "Set the share for a user. Note that the type of resource must be in the
   list of (get-all-resource-types)

   Usage:
   (set-share! conn \"u1\" :cpus 20.0 :mem 10.0)
   or
   (set-share! conn \"u1\" :cpus 20.0)
   etc."
  [conn user & kvs]
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
      @(d/transact conn txns))))

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
          user->share-cache (->> all-share-users
                                 (map (fn [user]
                                        [user (get-share db user)]))
                                 ;; In case default-user doesn't have an explicit share
                                 (cons [default-user (get-share db default-user)])
                                 (into {}))]
      (fn user->share
        [user]
        (or (get user->share-cache user)
            (get user->share-cache default-user))))))



(comment
  ;; Adjust the share.
  (let [conn (d/connect "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/mesos-jobs2?interface=http")]
    (set-share! conn "promised" :cpus 3000.0 :mem 2500000.0))

  (let [conn (d/connect "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/mesos-jobs2?interface=http")]
    (set-share! conn "default" :cpus 3000.0 :mem 2500000.0))

  ;; Retract share.
  (let [conn (d/connect "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/mesos-jobs2?interface=http")]
    (retract-share! conn "wyegelwe"))

  ;; Check the share.
  (let [conn (d/connect "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/mesos-jobs2?interface=http")]
    (println "default" (get-share (db conn) "default")))

  ;; List users who has non default share.
  (let [conn (d/connect "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/mesos-jobs2?interface=http")
        db (db conn)
        users (q '[:find ?u
                   :in $
                   :where
                   [?e :share/user ?u]
                   [?e :share/resource ?r]]
                 db)]
    (println users))

  ; Print table of users with explicit share set and what that share is
  (let [conn (d/connect "datomic:mem://test-db")
        db (d/db conn)
        all-share-users (d/q '[:find [?user ...]
                               :where
                               [?q :share/user ?user]]
                             db)]
    (->> all-share-users
         (map (fn [user]
                (assoc (get-share db user) :user user)))
         ;; In case default-user doesn't have an explicit share
         (cons (assoc (get-share db default-user) :user default-user))
         (sort-by (juxt :mem :cpus :user))
         reverse
         (clojure.pprint/print-table [:user :mem :cpus])

         )))
