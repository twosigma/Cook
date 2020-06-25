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
(ns cook.datomic
  (:require [clojure.core.async :as async]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cook.config :refer [config]]
            [cook.util :as util]
            [datomic.api :as d :refer [q]]
            [mount.core :as mount])
  (:import (clojure.lang Agent)
           (datomic Connection)
           (java.util.concurrent BlockingQueue TimeUnit)))

(defn create-connection
  "Creates and returns a connection to the given Datomic URI"
  [{{:keys [mesos-datomic-uri]} :settings}]
  ((util/lazy-load-var 'datomic.api/create-database) mesos-datomic-uri)
  (let [conn ((util/lazy-load-var 'datomic.api/connect) mesos-datomic-uri)]
    (doseq [txn (deref (util/lazy-load-var 'cook.schema/work-item-schema))]
      (deref ((util/lazy-load-var 'datomic.api/transact) conn txn))
      ((util/lazy-load-var 'metatransaction.core/install-metatransaction-support) conn)
      ((util/lazy-load-var 'metatransaction.utils/install-utils-support) conn))
    conn))

(defn disconnect
  "Releases the given Datomic connection"
  [^Connection conn]
  (.release conn))

(mount/defstate conn
                :start (create-connection config)
                :stop (disconnect conn))

(defn create-tx-report-mult
  "Takes a datomic connection, and returns a core.async mult that can
  be tapped to get a feed and a function to kill the background
  thread."
  [conn]
  (let [inner-chan (async/chan (async/buffer 4096))
        mult (async/mult inner-chan)
        report-q (d/tx-report-queue conn)
        running (atom true)
        join (async/thread
               (try
                 (while @running
                   (try
                     (when-let [e (.poll ^BlockingQueue report-q
                                         1 TimeUnit/SECONDS)]
                       (case (async/alt!!
                               [[inner-chan e]] :wrote
                               ;; The value for :default is eagerly evaluated, unlike for other clauses
                               :default :dropped
                               :priority true)
                         :dropped (log/info "dropped item from tx-report-mult--maybe there's a slow consumer?")
                         nil))
                     (catch InterruptedException _ nil)))
                 (catch Exception e
                   (log/error e "tx report queue tap got borked. That's bad."))))]
    [mult
     (fn []
       (reset! running false)
       (async/<!! join))]))

(defn transact-with-retries
  "Takes a connection and a transaction and retries the transaction until it succeeds.

   Also takes a retry schedule (timeouts in millis between retries)

   NB: this is super hard to debug crappy transaction logic. First, harden your transaction
   using the normal transact function. Later, you can wrap with this, and suffer the impossibility
   of debugging."
  ([conn txn]
   (transact-with-retries conn txn (cook.util/rand-exponential-seq 10 1000)))
  ([conn txn retry-schedule]
   (async/go
     (loop [[sleep & sched] retry-schedule]
       (when (and (not= :success
                        (try
                          (let [fut (d/transact-async conn txn)
                                c (async/chan)]
                            (d/add-listener fut #(async/close! c) Agent/pooledExecutor)
                            (async/<! c)
                            @fut
                            :success)
                          (catch Exception e
                            (if (.contains (.getMessage e) "db.error/cas-failed")
                              :success ;; it's not really success, but we can't continue anyway
                              e))))
                  sleep)
         (log/warn "Retrying txn" txn "attempt" (- (count retry-schedule) (count sched)) "of" (count retry-schedule))
         (async/<! (async/timeout sleep))
         (recur sched))))))

(defn transaction-timeout?
  "Returns true if the given exception is due to a transaction timeout"
  [exception]
  (str/includes? (str (.getMessage exception)) "Transaction timed out."))

(defn transact
  "Like datomic.api/transact, except the caller provides
  a handler function for transaction timeout exceptions"
  [conn tx-data handle-timeout-fn]
  (try
    @(d/transact conn tx-data)
    (catch Exception e
      (if (transaction-timeout? e)
        (do
          (log/warn e "Datomic transaction timed out")
          (handle-timeout-fn e))
        (do
          (log/error e "Datomic transaction caused exception")
          (throw e))))))
