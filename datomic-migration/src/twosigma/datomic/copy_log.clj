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
(ns twosigma.datomic.copy-log
  (:require
    [clj-leveldb :as leveldb]
    [clojure.core.async :as async]
    [clojure.set :as set]
    [clojure.tools.logging :as log]
    [datomic.api :as d]
    [riemann.client :as r])
  (:import
    (java.io Closeable)
    (java.nio ByteBuffer)
    (java.text SimpleDateFormat)
    (java.util Date Iterator))
  (:gen-class))

(set! *warn-on-reflection* true)

(defmacro trace [& args]
  `(log/trace (pr-str ~@args)))

(defprotocol DbIdMap
  (get-ids [this ids]
    "Returns a map from ids to new entity IDs")
  (put-ids [this ids-map]
    "Adds a map of entity ID=ID to this store.")
  (put-last-t [this t]
    "Stores the last t value successfully processed from this
    database.")
  (get-last-t [this]
    "Returns the last t value stored, or nil if none stored."))

(defrecord AtomDbIdMap [amap]
  DbIdMap
  (get-ids [this id]
    (select-keys @amap id))
  (put-ids [this ids-map]
    (swap! amap merge ids-map)
    this)
  (put-last-t [this t]
    (swap! amap assoc :last-t t))
  (get-last-t [this]
    (:last-t @amap)))

(defn atom-db-id-map
  "Returns a DbIdMap which stores its state in local memory in an
  Atom."
  []
  (->AtomDbIdMap (atom {})))

(defn long->bytes [n]
  (let [buf (ByteBuffer/allocate 8)]
    (.putLong buf n)
    (.array buf)))

(defn bytes->long [bytes]
  (.getLong (ByteBuffer/wrap bytes)))

(def leveldb-last-t-key (.getBytes "filter-log-last-key" "UTF-8"))

(defrecord LevelDbIdMap [leveldb]
  Closeable
  (close [this] (.close ^Closeable leveldb))
  DbIdMap
  (get-ids [this ids]
    (persistent!
     (reduce (fn [m k]
               (if-let [v (leveldb/get leveldb (long->bytes k))]
                 (assoc! m k (bytes->long v))
                 m))
             (transient {})
             ids)))
  (put-ids [this ids-map]
    (apply leveldb/put
           leveldb
           (persistent!
            (reduce-kv
             (fn [out k v]
               (-> out
                   (conj! (long->bytes k))
                   (conj! (long->bytes v))))
             (transient [])
             ids-map))))
  (put-last-t [this t]
    (leveldb/put leveldb leveldb-last-t-key (long->bytes t)))
  (get-last-t [this]
    (when-let [bytes (leveldb/get leveldb leveldb-last-t-key)]
      (bytes->long bytes))))

(defn leveldb-id-map [file]
  (->LevelDbIdMap
   ;; Don't use key/val encoders/decoders because we will use this
   ;; same LevelDB instance to encode different types.
   (leveldb/create-db file {})))

(defn make-tempids
  "Returns a map from ids to new tempIDs with the correct partitions."
  [ids from-db]
  (reduce (fn [m id]
            (assoc m id (d/tempid (d/ident from-db (d/part id)))))
          {}
          ids))

(defn ^Iterator txrange-iterator
  "Returns an Iterator of the transaction log."
  [conn start end]
  (-> conn
      d/log
      ^Iterable (d/tx-range start end)
      .iterator))

(defn find-eids
  "Returns all entity IDs in a transaction returned by tx-range."
  [logtx
   refattrs]  ; set of attribute IDs in old DB with ref value type
  (reduce (fn [eids [e a v]]
            (if (contains? refattrs a)
              (conj eids e a v)
              (conj eids e a)))
          #{}
          (:data logtx)))

(defn keyset [map]
  (set (keys map)))

(defn reresolve
  "Resolves tempids created in this transaction into a map of entity
  IDs from the old database to entity IDs in the new database."
  [creating-idmap {:keys [db-after tempids]}]
  (persistent!
   (reduce-kv
    (fn [m old-eid tempid]
      ;; Orphan tempids may have been filtered out
      (if-let [new-eid (d/resolve-tempid db-after tempids tempid)]
        (assoc! m old-eid new-eid)
        m))
    (transient {})
    creating-idmap)))

(defn find-ref-attrs
  "Returns a set of entity IDs for all attributes in the database with
  valueType ref."
  [db]
  ;; can't use [?attr ...] syntax on this version of Datomic
  (set (map first
            (d/q '[:find ?attr
                   :where
                   [?attr :db/ident ?a]
                   [?attr :db/valueType :db.type/ref]
                   ]
                 db))))

(defn put-known-idents
  "Prepopulates dbidmap with entity IDs for idents which already exist
  in to-db. Ignores idents which do not exist in from-db."
  [dbidmap from-db to-db]
  (let [idents (map first
                    (d/q '[:find ?ident
                           :where [_ :db/ident ?ident]]
                         to-db))
        ident-idmap (reduce
                     (fn [m ident]
                       (if-let [from-id (d/entid from-db ident)]
                         (assoc m from-id (d/entid to-db ident))
                         m))
                     {}
                     idents)]
    (put-ids dbidmap ident-idmap)))

(defn datom->assertion
  [[e a v _ add?]  ; datom in old DB
   idmap  ; map of old Entity ID => new Entity ID or tempid
   refattrs]  ; set of attribute IDs in old DB with ref value type
  (vector (if add? :db/add :db/retract)
          (get idmap e)
          (get idmap a)
          (if (contains? refattrs a)
            (get idmap v)
            v)))

(defn tempid? [x]
  (and (map? x)
       (contains? x :part)
       (contains? x :idx)
       (integer? (:idx x))
       (neg? (:idx x))))

(defn remove-orphan-tempids [data]
  (let [e-tempids (->> data
                       (map #(nth % 1))
                       (filter tempid?)
                       set)
        v-tempids (->> data
                       (map #(nth % 3))
                       (filter tempid?)
                       set)
        orphans (set/difference v-tempids e-tempids)]
    (log/trace :orphans orphans)
    (remove (fn [[op e a v]] (contains? orphans v)) data)))


(def counter-interval
  "Interval of time, in milliseconds, at which to log updated counts."
  10000)

(defn per-second [n]
  #_(long (/ n (/ counter-interval 1000.0)))
  n)

(defn start-log-counter
  [{:keys [start end from-conn from-db to-conn counters-ch riemann-host]}]
  (let [date-format (SimpleDateFormat. "yyyy-MM-dd HH:mm")
        t->date (fn ^java.util.Date [t]
                  (:db/txInstant (d/entity from-db (d/t->tx t))))
        t->date-str (fn [t]
                      (.format
                        date-format
                        (t->date t)))
        rc (r/tcp-client {:host riemann-host})]
   (async/go
     (try
       (loop [timer (async/timeout counter-interval)
              prev-t -1
              t -1
              read-tx 0
              read-datoms 0
              write-tx 0
              write-datoms 0]
         (async/alt!
           timer
           ([_]
            (log/infof "At t=%d (%s); read/write %d/%d tx/sec; %d/%d datoms/sec"
                       t
                       (t->date-str t)
                       (per-second read-tx)
                       (per-second write-tx)
                       (per-second read-datoms)
                       (per-second write-datoms))
            (r/send-event rc {:service "datomic migration t"
                              :state "ok"
                              :metric t
                              :date (t->date-str t)})
            (r/send-event rc {:service "datomic migration read-tx"
                              :state "ok"
                              :metric read-tx})
            (r/send-event rc {:service "datomic migration write-tx"
                              :state "ok"
                              :metric write-tx})
            (r/send-event rc {:service "datomic migration read-datoms"
                              :state "ok"
                              :metric read-datoms})
            (r/send-event rc {:service "datomic migration write-datoms"
                              :state "ok"
                              :metric write-datoms})
            (let [cur-date (t->date t)
                  prev-date (or (t->date prev-t) cur-date)
                  delta-time (- (.getTime cur-date) (.getTime prev-date))]
              (r/send-event rc {:service "datomic migration delta time milliseconds"
                                :state "ok"
                                :metric delta-time})
              (r/send-event rc {:service "datomic migration delta time seconds"
                                :state "ok"
                                :metric (/ delta-time 1000.0)})

              (r/send-event rc {:service "datomic migration delta time minutes"
                                :state "ok"
                                :metric (/ delta-time 1000.0 60.0)}))

            (recur (async/timeout counter-interval)
                   t t 0 0 0 0))

           counters-ch
           ([result]
            (if (some? result)
              (recur timer
                     prev-t
                     (:t result t)
                     (+ read-tx (:read-tx result 0))
                     (+ read-datoms (:read-datoms result 0))
                     (+ write-tx (:write-tx result 0))
                     (+ write-datoms (:write-datoms result 0)))
              (log/infof "Done at t=%d (%s)" t (t->date t))))))
       (catch Throwable t
         (log/error t 'log-counter))))))

(defn inc-when [t]
  (when t (inc t)))

(defn non-empty-tx?
  "Returns true if the filtered transaction data contains more than
  just the txInstant datom."
  [txinput]
  (< 1 (count txinput)))

(def transaction-timeout
  "Max time to wait for a transaction, in milliseconds."
  (* 10 60 1000)) ; ten minutes

(defn copy-tx
  "Writes filtered datoms to to-conn, resolving entity IDs and
  removing 'orphan' references. Returns the deref'd result of
  d/transact with :txinput (the data being transacted) assoc'd in."
  [logtx  ; single value from tx-range of from-conn
   filtered  ; new transaction data to write to to-conn
   {:keys [dbidmap refattrs from-conn from-db to-conn]}]
  (let [logtx-eids (find-eids logtx refattrs)
        known-idmap (get-ids dbidmap logtx-eids)
        unknown-ids (set/difference logtx-eids (keyset known-idmap))
        creating-idmap (make-tempids unknown-ids from-db)
        current-idmap (merge known-idmap creating-idmap)
        txinput (->> filtered
                     (map #(datom->assertion % current-idmap refattrs))
                     remove-orphan-tempids
                     vec)]
    (trace :txinput txinput)
    (when (non-empty-tx? txinput)
      (let [txresult (deref (d/transact-async to-conn txinput)
                            transaction-timeout
                            ::timeout)]
        (when (= ::timeout txresult)
          (throw (ex-info "Transaction timed out" {:txinput txinput})))
        (trace :txresult txresult)
        (let [created-idmap (reresolve creating-idmap txresult)]
          (trace :created-idmap created-idmap)
          (put-ids dbidmap created-idmap))
        (assoc txresult :txinput txinput)))))

(defn assert-initialized [context]
  (assert (::initialized? context)
          (str "Context not initialized; you must call " `init)))

;;; Public entry points

(defn init
  "Initializes the DB ID Map in context, idempotently. Returns context
  updated with :refattrs."
  [{:keys [dbidmap from-conn to-conn] :as context}]
  (assert dbidmap "Context is missing :dbidmap")
  (assert from-conn "Context is missing :from-conn")
  (assert to-conn "Context is missing :to-conn")
  (let [from-db (d/db from-conn)
        to-db (d/db to-conn)
        refattrs (find-ref-attrs from-db)]
    (put-known-idents dbidmap from-db to-db)
    (assoc context
           ::initialized? true
           :from-db from-db
           :to-db to-db
           :refattrs refattrs)))

(defn copy-ts
  "Copies a collection of T's from-conn to to-conn, in order,
  applying a transducer xform to the :data of each transaction in the
  log."
  [ts {:keys [xform dbidmap from-conn] :as context}]
  (assert-initialized context)
  (let [log (d/log from-conn)
        ts (sort (distinct (seq ts)))]
    (doseq [t ts]
      (let [{:keys [t data] :as logtx} (first (d/tx-range log t (inc t)))
            filtered (try (into [] xform data)
                          (catch Throwable ex
                            (log/errorf ex "Transform error at t=%d" t)
                            (throw ex)))]
        (trace :logtx logtx)
        (when (non-empty-tx? filtered)
          (try
            (copy-tx logtx filtered context)
            (put-last-t dbidmap t)
            (log/infof "Copy/filtered t=%d" t)
            (catch Throwable ex
              (log/errorf ex "Error at t=%d" t)
              (throw ex))))))
    {:last-t (last ts)}))

(defn copy-log
  "Copies database from-conn to-conn by walking the transaction log
  from start to end, applying a transducer xform to the :data of each
  transaction.

  If the transformed transaction is empty or contains only
  the :db/txInstant datom, it will not be copied.

  If start is nil and dbidmap is non-empty, copying will resume from
  the last T stored in dbidmap.

  Preserves txInstant but does not preserve entity IDs.

  Returns a map of
    :last-t - last from-conn T completely processed"
  [{:keys [xform dbidmap refattrs start end from-conn]
    :as context}]
  (assert-initialized context)
  (let [start (or start (inc-when (get-last-t dbidmap)))
        range (txrange-iterator from-conn start end)
        counters-ch (async/chan 100)
        context (assoc context :counters-ch counters-ch)]
    (start-log-counter context)
    (loop [last-t start]
      (if (.hasNext range)
        (let [{:keys [t data] :as logtx} (.next range)
              filtered (try (into [] xform data)
                            (catch Throwable ex
                              (log/errorf ex "Transform error at t=%d" t)
                              nil))]
          (trace :logtx logtx)
          (async/>!! counters-ch {:t t :read-tx 1 :read-datoms (count data)})
          (when (non-empty-tx? filtered)
            (try
              (let [{:keys [tx-data]} (copy-tx logtx filtered context)]
                (async/>!! counters-ch {:write-tx 1
                                        :write-datoms (count tx-data)}))
              (catch Throwable ex
                (log/errorf ex "Error at t=%d; last-t=%d" t last-t)
                (async/close! counters-ch)
                (throw ex))))
          (put-last-t dbidmap t)
          (recur t))
        (do ;; finished
          (async/close! counters-ch)
          {:last-t last-t})))))

