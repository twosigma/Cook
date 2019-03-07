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
(ns metatransaction.core
  (:require [datomic.api :as d])
  (:import (datomic Datom)))

(def metatransaction-schema
  [{:db/id (d/tempid :db.part/db)
    :db/ident :metatransaction/status
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc "Status for meta transaction, either :metatransaction.status/commited or :metatransaction.status/uncommited"
    :db/index true
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/user)
    :db/ident :metatransaction.status/committed}
   {:db/id (d/tempid :db.part/user)
    :db/ident :metatransaction.status/uncommitted}
   {:db/id (d/tempid :db.part/db)
    :db/ident :metatransaction/uuid
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/doc "Client-based unique ID for metatransactions"
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :metatransaction/tx
    :db/valueType :db.type/ref
    :db/index true
    :db/cardinality :db.cardinality/many
    :db/doc "Reference all transactions of a metatransaction"
    :db.install/_attribute :db.part/db}])

(def enforce-uncommitted
  "Throws an exception if the meta transaction has already been committed"
  #db/fn
  {:lang :clojure
   :params [db mt-id]
   :code (if-not mt-id
           (throw (ex-info (str "No metatransaction: " mt-id) {:mt-id mt-id}))
           (let [metatransaction (d/entity db mt-id)]
             (when-not (= (:metatransaction/status metatransaction)
                          :metatransaction.status/uncommitted)
               (let [[tx-id tx-time]
                     (first
                       (d/q '[:find ?tx ?time
                              :where
                              [?mt-id :metatransaction/status :metatransaction.status/committed ?tx]
                              [?tx :db/txInstant ?time]] db))]
                 (throw (ex-info (str "metatransaction "
                                      (:metatransaction/uuid metatransaction)
                                      " has already been committed as of "
                                      (- (.getTime (java.util.Date.))
                                         (.getTime tx-time))
                                      " ms ago.")
                                 {:metatransaction metatransaction
                                  :time-of-commit tx-time
                                  :tx-id tx-id }))))))})

(def include-in
  "db fn: [:metatransaction/include-in <uuid>]
     Includes the current transaction in the metatransaction specified by uuid.
     If a metatransaction doesn't yet exist, it will be created"
  #db/fn
  {:lang :clojure
   :params [db mt-uuid]
   :code (let [mt (d/entity db [:metatransaction/uuid mt-uuid])
                mt-id (or (:db/id mt) (d/tempid :db.part/user))
                tx-id (d/tempid :db.part/tx)]
            (when mt
              (d/invoke db :metatransaction/enforce-uncommitted db (:db/id mt)))
            [(merge
               {:db/id mt-id
                :metatransaction/tx tx-id}
               (when-not mt
                 {:metatransaction/uuid mt-uuid
                  :metatransaction/status :metatransaction.status/uncommitted}))])})

(def commit
  "db fn: [:metatransaction/commit <uuid>]
   Marks the metatransaction as committed. If the metatransaction
   doesn't exist an exception is thrown. This operation is idempotent."
  #db/fn
  {:lang :clojure
   :params [db mt-uuid]
   :code (let [mt (d/entity db [:metatransaction/uuid mt-uuid])
                mt-id (:db/id mt)]
            (when-not mt-id
              (throw (ex-info (str "There is no metatransaction for uuid: " mt-uuid) {:mt-uuid mt-uuid})))
            [{:db/id mt-id :metatransaction/status :metatransaction.status/committed}])})

(def db-fns
  [{:db/id #db/id [:db.part/user]
    :db/ident :metatransaction/include-in
    :db/doc (:doc (meta #'include-in))
    :db/fn include-in}
   {:db/id #db/id [:db.part/user]
    :db/ident :metatransaction/enforce-uncommitted
    :db/doc (:doc (meta #'commit))
    :db/fn enforce-uncommitted}
   {:db/id #db/id [:db.part/user]
    :db/ident :metatransaction/commit
    :db/doc (:doc (meta #'enforce-uncommitted))
    :db/fn commit}])

(defn install-metatransaction-support
  "Installs the metatransaction schema additions to conn."
  [conn]
  @(d/transact conn (concat metatransaction-schema db-fns)))

(defn create-committed-filter
  "Creates a db filter function that removes transactions that are marked as
   :metatransaction.status/uncommitted by their corresponding metatransaction.
   Transactions without a metatransaction will go unfiltered. "
  [db]
  (let [committed (d/entid db :metatransaction.status/committed)]
    (fn [db ^Datom datom]
      (if-let [mt (-> (d/datoms db :vaet (.tx datom) :metatransaction/tx) first :e)]
        (= committed (-> (d/datoms db :eavt mt :metatransaction/status) first :v))
        true))))

(defn filter-committed
  "Applies the filter metatransaction.core/create-committed-filter to db"
  [db]
  (d/filter db (create-committed-filter db)))

(def db
  "Creates a db from the connection passed in and then
   applies metatransaction.core/filter-committed to it."
  (comp filter-committed d/db))
