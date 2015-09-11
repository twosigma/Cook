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
(ns twosigma.datomic.filter-log
  (:require
   [clj-leveldb :as leveldb]
   [clojure.tools.logging :as log]
   [datomic.api :as d]
   [twosigma.datomic.copy-log :as copy-log])
  (:import
   (java.io Closeable)
   (java.nio ByteBuffer)
   (java.util Date Iterator))
  (:gen-class))

(set! *warn-on-reflection* true)

(defn filter-ts
  "Copies a collection of T's from-conn to to-conn, in order,
  filtering out datoms that do not match pred."
  [ts {:keys [pred] :as context}]
  (copy-log/copy-ts ts (assoc context :xform (filter pred))))

(defn filter-log
  "Copies database from-conn to to-conn by walking the transaction log
  from start to end, filtering out datoms that do not match pred.
  Other arguments like copy-log."
  [{:keys [pred dbidmap start end from-conn]
    :as context}]
  (copy-log/copy-log (assoc context :xform (filter pred))))

;;; Filter predicates

(defn remove-attr
  "Returns a filter predicate function which removes all datoms with
  the given attribute."
  [db attr]
  (let [attr-eid (d/entid db attr)]
    (fn [[e a v]]
      (not= a attr-eid))))

(defn remove-before
  "Returns a filter predicate function which removes all datoms older
  than t-or-date."
  [db t-or-date]
  (let [start-tx (d/entid-at db :db.part/tx t-or-date)]
    (fn [[e a v tx]]
      (not (< tx start-tx)))))

(defn remove-attr-before [db attr t-or-date]
  (let [attr-eid (d/entid db attr)
        start-tx (d/entid-at db :db.part/tx t-or-date)]
    (fn [[e a v tx]]
      (not (and (= a attr-eid)
                (< tx start-tx))))))

(defn autoexcision-cutoffs
  "Given a connection, finds attributes with the
  :autoexcision/duration attribute. Returns a map of attribute IDs to
  Tx IDs. Datoms with that attribute and a Tx less than the Tx ID
  should be removed."
  [conn]
  (let [log (d/log conn)
        basis (d/basis-t (d/db conn))
        now (System/currentTimeMillis)]
    (->> (d/q '[:find ?a ?d
                :where [?a :autoexcision/duration ?d]]
              (d/db conn))
         (reduce
          (fn [m [a d]]
            (let [start (Date. (long (- now d)))
                  t (or (:t (first (d/tx-range log start nil)))
                        basis)]
              (assoc m a (d/t->tx t))))
          {}))))

(defn remove-autoexcision [conn]
  (let [cutoffs (autoexcision-cutoffs conn)]
    (fn [[e a v tx]]
      (<= (get cutoffs a Long/MIN_VALUE) tx))))


;; filter-ts helpers
(defn get-all-attr-txns
  [db attr]
  (map (comp d/tx->t first)
       (d/q '[:find ?tx
              :in $ ?attr
              :where
              [_ ?attr _ ?tx]]
            db attr)))

(defn get-schema-txns
  [db]
  (map (comp d/tx->t first)
       (d/q '[:find ?tx
              :where
              [_ :db/ident _ ?tx]
              ]
            db)))

(defn get-schema-ents
  [db]
  (d/q '[:find ?a ?tx
         :where
         [?e :db/ident ?a ?tx]]
       db))

(defn get-static-datoms-before
  [from-conn time-before]
  (let [schema-txns (filter #(>= % 1000) ; Only take user schema elements
                            (get-schema-txns (d/history (d/db from-conn))))
        schema-ents (get-schema-ents (d/db from-conn))
        our-schema-ents (->> schema-ents
                             (filter #(>= (-> % second d/tx->t) 100))
                             (map (comp #(d/entity (d/db from-conn) [:db/ident %]) first)))
        non-expiring-schema-ents (->> our-schema-ents
                                      (remove :autoexcision/duration)
                                      (map d/touch))
        static-txns (concat schema-txns
                            (mapcat (comp
                                      (partial get-all-attr-txns (d/db from-conn))
                                      :db/ident)
                                    non-expiring-schema-ents))
        t-before (:t (first (d/tx-range (d/log from-conn) time-before nil)))]
    (->> static-txns
         (filter #(< % t-before))
         distinct
         sort)))

;;; Command-line invocation
(defn check-transact-static-args
  [args]
  (when (not= (count args) 5)
    (println "transact-static must have arguments: time-to-include from-url to-url leveldb-file riemann-host")
    (System/exit 64)))

(defn transact-static-main
  [[time-to-include from-url to-url leveldb-file riemann-host]]
  (let [time-to-include (clojure.instant/read-instant-date time-to-include)
        _ (d/create-database to-url)
        from-conn (d/connect from-url)
        to-conn (d/connect to-url)
        pred (remove-autoexcision from-conn)
        static-txns (get-static-datoms-before from-conn time-to-include)]
    (with-open [^Closeable dbidmap (copy-log/leveldb-id-map leveldb-file)]
      (prn (filter-ts static-txns (copy-log/init {:from-conn from-conn
                                                  :to-conn to-conn
                                                  :pred pred
                                                  :dbidmap dbidmap
                                                  :riemann-host riemann-host
                                                  }))))
    (System/exit 0)))

(defn check-copy-full-log-args
  [args]
  (when (not= 4 (count args))
      (println "copy-log must have arguments: from-url to-url leveldb-file riemann-host")
      (System/exit 64)))

(defn copy-full-log-main
  [[from-url to-url leveldb-file riemann-host]]
  (let [_ (d/create-database to-url)
          from-conn (d/connect from-url)
          to-conn (d/connect to-url)
          pred identity]
      (with-open [^Closeable dbidmap (copy-log/leveldb-id-map leveldb-file)]
        (prn (filter-log (copy-log/init {:from-conn from-conn
                                         :to-conn to-conn
                                         :pred pred
                                         :dbidmap dbidmap
                                         :riemann-host riemann-host
                                         }))))
    (System/exit 0)))


(defn check-copy-log-args
  [args]
  (when (not= 4 (count args))
      (println "copy-log must have arguments: from-url to-url leveldb-file riemann-host")
      (System/exit 64)))

(defn copy-log-main
  [[from-url to-url leveldb-file riemann-host]]
  (let [_ (d/create-database to-url)
          from-conn (d/connect from-url)
          to-conn (d/connect to-url)
          pred (remove-autoexcision from-conn)]
      (with-open [^Closeable dbidmap (copy-log/leveldb-id-map leveldb-file)]
        (prn (filter-log (copy-log/init {:from-conn from-conn
                                         :to-conn to-conn
                                         :pred pred
                                         :dbidmap dbidmap
                                         :riemann-host riemann-host
                                         }))))
    (System/exit 0)))



(defn check-static-and-copy-args
  [args]
  (when (not= (count args) 5)
    (println "both must have arguments: time-to-include from-url to-url leveldb-file riemann-host")
    (System/exit 64)))

(defn transact-static-and-copy-log-main
  [[time-to-include from-url to-url leveldb-file riemann-host]]
  (let [time-to-include (clojure.instant/read-instant-date time-to-include)
        _ (d/create-database to-url)
        from-conn (d/connect from-url)
        to-conn (d/connect to-url)
        pred (remove-autoexcision from-conn)
        static-txns (get-static-datoms-before from-conn time-to-include)]
    (with-open [^Closeable dbidmap (copy-log/leveldb-id-map leveldb-file)]
      (let [ctx (copy-log/init {:from-conn from-conn
                                :to-conn to-conn
                                :pred pred
                                :dbidmap dbidmap
                                :riemann-host riemann-host})
            last-t-in-dbidmap (copy-log/get-last-t dbidmap)
            static-txns (if last-t-in-dbidmap
                          (filter #(> % last-t-in-dbidmap) static-txns)
                          static-txns)]
        (log/info "Picking up at t: " last-t-in-dbidmap)
        (when (seq static-txns)
          (prn (filter-ts static-txns ctx)))
        (prn (filter-log ctx))))
    (System/exit 0)))

(defn -main [& args]
  (try
    (when-not (seq args)
      (println "Must specify method, either transact-static or copy-log or both")
      (System/exit 64))
    (let [method (first args)]
      (condp = method
        "transact-static" (do
                            (check-transact-static-args (rest args))
                            (transact-static-main (rest args)))
        "copy-log" (do
                     (check-copy-log-args (rest args))
                     (copy-log-main (rest args)))
        "copy-full-log" (do
                          (println "This will copy ALL the data. Use with care. You almost certainly don't want this.")
                          (check-copy-full-log-args (rest args))
                          (copy-full-log-main (rest args)))
        "static-and-copy" (do
                            (check-static-and-copy-args (rest args))
                            (transact-static-and-copy-log-main (rest args)))))
    (catch Throwable ex
      (log/error ex "Error")
      (.printStackTrace ex)
      (System/exit 70))))

;;; Sample usage

(comment
  (def ctx (copy-log/init {:end 4000  ; for demonstration
                           :pred (constantly true)
                           :dbidmap dbidmap
                           :from-conn from-conn
                           :to-conn to-conn}))

  (filter-log ctx)
  ;;=> {:last-t 3994, :transacted-count 450}

  ;; Must keep same dbidmap for next run

  ;; Next run starts at last-t + 1
  (filter-log (assoc ctx :start (inc (:last-t *1))))

  ;; With LevelDB storing entity ID mappings:
  (with-open [dbidmap (copy-log/leveldb-id-map "/tmp/dbidmap.leveldb")]
    (filter-log (copy-log/init {:pred (constantly true)
                                :dbidmap dbidmap
                                :from-conn from-conn
                                :to-conn to-conn})))

  )
