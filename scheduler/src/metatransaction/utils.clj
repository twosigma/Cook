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
(ns metatransaction.utils
  (:require [clojure.core.async :as async]
            [datomic.api :as d]))

(def assert-db
  "db-fn: [:utils/assert-db test-type query-structure & inputs]
     Throws an exception if the result of (q query-structure) doesn't agree
     with the test type. The current supported test types are: [:none, :exist]

     This can be used to assure the database is in the same state it
     was when transaction data was created."
  #db/fn {:lang :clojure
          :params [db test-type query-structure & ins]
          :code (let [resp (apply d/q query-structure db ins)
                      test-fn (condp = test-type
                                :none (comp not seq)
                                :exists seq)]
                  (when-not (test-fn resp)
                    (throw (ex-info "Failed assertion" {:time (java.util.Date. )}))))})

(def idempotent-transaction
  "db-fn: [:utils/idempotent-transaction uuid]
   Throws an exception if uuid for attribute :tx/idempotent-uuid already exists.

   This can be used to ensure idempotent transactions when transacting with retries."
  #db/fn {:lang :clojure
          :params [db uuid]
          :code (let [ent (d/entity db [:tx/idempotent-uuid uuid])]
                   (if ent
                     (throw (ex-info "idempotency check failed." {:idempotent-ex? ent}))
                     [[:db/add (d/tempid :db.part/tx) :tx/idempotent-uuid uuid]]))})

(defn make-idempotent
  "Outputs a fn that will ensure the txn outputted from f will be idempotent
   when transacted with datomic. Uses db-fn :utils/idempotent-transaction"
  [f]
  (let [uuid (d/squuid)]
    (fn [db & args]
      (d/invoke db :utils/idempotent-transaction db uuid)
      (into [[:utils/idempotent-transaction uuid]] (apply f db args)))))

(defn idempotent-exception?
  "Checks if the exception thrown was due to the data
   already being transacted into datomic"
  [ex]
  (:idempotent-ex? (ex-data ex)))


(defmacro suppress-idempotent-exceptions
  "Evaluates body and if an idempotent exception (see idempotent-exception?)
   is thrown, just return the exception. All other exceptions are thrown."
  ([& body]
  `(try
     ~@body
     (catch Exception e#
       (if-not (idempotent-exception? e#)
         (throw e#)
         e#)))))

(defn <??
  "Like clojure.core.async/<!! except that if the value pulled from the channel
   is an instance of Throwable, it will throw the exception. Otherwise, return
   the value."
  [c]
  (let [out (async/<!! c)]
    (if (instance? Throwable out)
      (throw out)
      out)))

(defmacro <?
  "Like clojure.core.async/<!! except that if the value pulled from the channel
   is an instance of Throwable, it will throw the exception. Otherwise, return
   the value."
  [ch]
  `(let [out# (async/<! ~ch)]
     (if (instance? Throwable out#)
       (throw out#)
       out#)))

(defn update-async
  "Attempts to transact with conn using the output of transaction-fn as transaction data.
   You will likely find that update! or update!! meet your needs and provide a better api.

   The output will either be an exception (not thrown!) if the transaction
   couldn't be completed or a map with two keys if the transaction succeeded.
   The keys are:
       :transaction which contains the output of the transaction and
       :errors the list of errors that occurred before the transaction succeeded

   If an exception occurs during transaction, update-async will try again according to
   retry-schedule (:retry-schedule as a key in the opts param). retry-schedule is expected
   to be a sequence of time in millis that update-async should wait between retries. When the
   seq is exhausted, update-async will return.

   transaction-timeout (:transaction-timeout as a key in the opts param) is the amount of time
   in millis that one try of transacting will wait before assuming failure. Note that even after
   the timeout, the transaction can complete."
  [conn {:keys [retry-schedule transaction-timeout listener]
          :or {retry-schedule [] transaction-timeout 60000 listener identity}
          :as opts}
   transaction-fn & args]
  (async/go
    (println "getting started")
    (try
      (loop [[sleep & sched] retry-schedule exceptions []]
        ;; Get transaction data
        (let [tx (<? (async/thread  ; Throw an exception immeditately if thrown in tx fn
                                   (try
                                     (apply transaction-fn (d/db conn) args)
                                     (catch Exception e
                                       (ex-info "Exception in transaction-fn."
                                                (merge (ex-data e) {:tx-fn-ex e :errors (seq exceptions)})
                                                e)))))
              _ (println "ran txn fn")
              ;; Attempt transaction
              transaction (try
                            (let [fut (d/transact-async conn tx)
                                  c (async/chan)]
                              (d/add-listener fut #(async/close! c) clojure.lang.Agent/pooledExecutor)  ;Should use a different executor!
                              (async/alt!
                                (async/timeout transaction-timeout) ([_] (ex-info
                                                                           "Timeout waiting for response from datomic server"
                                                                           {:conn conn
                                                                            :transaction-future fut
                                                                            :timeout transaction-timeout}))
                                c ([_] @fut)))
                            (catch Exception e
                              e))
              ;; Update exception state
              exception-occurred (isa? (type transaction) Exception)
              exceptions (into exceptions (when exception-occurred [transaction]))]
          (println "finished txns")
          (if-not (and sleep exception-occurred)
            (let [resp (if exception-occurred
                         (ex-info (str "Failed to complete transact on "  (d/db conn)
                                       " after " (inc (count retry-schedule)) " attempt(s)")
                                  (merge
                                    {:errors (seq exceptions)
                                     :conn conn}
                                    (ex-data (last exceptions)))
                                  (last exceptions))
                         {:errors (seq exceptions)
                          :transaction transaction})]
              (listener resp)
              (println "got a result" resp)
              resp)
            (do
              (async/<! (async/timeout sleep))
              (recur sched (conj exceptions transaction))))))
        ;; If any exception occurs uncaught, still want to see it
        (catch Exception e
          e))))

(defn update!!
  "Attempts to transact with conn using the output of transaction-fn as transaction data.

   This is a blocking call, which will throw an exception if the transaction couldn't
   be completed. If the transaction succeeds it will return a map with two keys,
   :transaction and :errors.
       :transaction  -- which contains the output of the transaction and
       :errors --  the list of errors that occurred before the transaction succeeded.
                   It is included for debugging information

   If an exception occurs during transaction, update!! will try again according to
   retry-schedule (:retry-schedule as a key in the opts param). retry-schedule is expected
   to be a sequence of time in millis that update-async should wait between retries. When the
   seq is exhausted, update-async will return.

   transaction-timeout (:transaction-timeout as a key in the opts param) is the amount of time
   in millis that one try of transacting will wait before assuming failure. Note that even after
   the timeout, the transaction can complete."
  [conn opts f & args]
  (<?? (apply update-async conn opts f args)))

(defmacro update!
  "Attempts to transact with conn using the output of transaction-fn as transaction data.

   This is a parking call, which will throw an exception if the transaction couldn't
   be completed. If the transaction succeeds it will return a map with two keys,
   :transaction and :errors.
       :transaction  -- which contains the output of the transaction and
       :errors --  the list of errors that occurred before the transaction succeeded.
                   It is included for debugging information

   If an exception occurs during transaction, update! will try again according to
   retry-schedule (:retry-schedule as a key in the opts param). retry-schedule is expected
   to be a sequence of time in millis that update-async should wait between retries. When the
   seq is exhausted, update-async will return.

   transaction-timeout (:transaction-timeout as a key in the opts param) is the amount of time
   in millis that one try of transacting will wait before assuming failure. Note that even after
   the timeout, the transaction can complete."
  [conn opts f & args]
  `(<? (update-async ~conn ~opts ~f ~@args)))

(defn transact-async-with-retries
  "Attempts to transact tx multiple times in the event of failure.
   You will likely find that transact-with-retries! or transact-with-reries!!
   meet your needs and provide a better api.

   The output will either be an exception (not thrown!) if the transaction
   couldn't be completed or a map with two keys if the transaction succeeded.
   The keys are:
       :transaction which contains the output of the transaction and
       :errors the list of errors that occurred before the transaction succeeded

   If an exception occurs during transaction, transact-async-with-retries will try again according to
   retry-schedule (:retry-schedule as a key in the opts param). retry-schedule is expected
   to be a sequence of time in millis that update-async should wait between retries. When the
   seq is exhausted, update-async will return.

   transaction-timeout (:transaction-timeout as a key in the opts param) is the amount of time
   in millis that one try of transacting will wait before assuming failure. Note that even after
   the timeout, the transaction can complete."
  [conn opts tx]
  (update-async conn opts (fn [_] tx)))

(defn transact-with-retries!!
  "Attempts to transact tx multiple times in the event of failure.

   This is a blocking call, which will throw an exception if the transaction couldn't
   be completed. If the transaction succeeds it will return a map with two keys,
   :transaction and :errors.
       :transaction  -- which contains the output of the transaction and
       :errors --  the list of errors that occurred before the transaction succeeded.
                   It is included for debugging information


   If an exception occurs during transaction, transact-with-retries!! will try again according to
   retry-schedule (:retry-schedule as a key in the opts param). retry-schedule is expected
   to be a sequence of time in millis that update-async should wait between retries. When the
   seq is exhausted, update-async will return.

   transaction-timeout (:transaction-timeout as a key in the opts param) is the amount of time
   in millis that one try of transacting will wait before assuming failure. Note that even after
   the timeout, the transaction can complete."
  [conn opts tx]
  (<?? (transact-async-with-retries conn opts tx)))

(defmacro transact-with-retries!
  "Attempts to transact tx multiple times in the event of failure.

   This is a parking call, which will throw an exception if the transaction couldn't
   be completed. If the transaction succeeds it will return a map with two keys,
   :transaction and :errors.
       :transaction  -- which contains the output of the transaction and
       :errors --  the list of errors that occurred before the transaction succeeded.
                   It is included for debugging information


   If an exception occurs during transaction, transact-with-retries! will try again according to
   retry-schedule (:retry-schedule as a key in the opts param). retry-schedule is expected
   to be a sequence of time in millis that update-async should wait between retries. When the
   seq is exhausted, update-async will return.

   transaction-timeout (:transaction-timeout as a key in the opts param) is the amount of time
   in millis that one try of transacting will wait before assuming failure. Note that even after
   the timeout, the transaction can complete."
  [conn opts tx]
  `(<? (transact-async-with-retries ~conn ~opts ~tx)))

(def utils-schema
  [{:db/id (d/tempid :db.part/db)
    :db/ident :tx/idempotent-uuid
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/doc "id used to check whether a specific transaction has succeed"
    :db/unique :db.unique/identity
    :db/index true
    :db.install/_attribute :db.part/db}])

(def db-fns
  [{:db/id (d/tempid :db.part/user)
    :db/ident :utils/assert-db
    :db/doc (:doc (meta #'assert-db))
    :db/fn assert-db}
   {:db/id (d/tempid :db.part/user)
    :db/ident :utils/idempotent-transaction
    :db/doc (:doc (meta #'idempotent-transaction))
    :db/fn idempotent-transaction}])

(def utils-txns
  (concat db-fns utils-schema))

(defn install-utils-support
  [conn]
  @(d/transact conn utils-txns))
