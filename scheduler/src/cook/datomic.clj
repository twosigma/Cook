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
            [clojure.pprint :refer (pprint)]
            [clojure.tools.logging :as log]
            [cook.util :refer (deftraced)]
            [datomic.api :as d :refer (q)]))

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
                     (when-let [e (.poll ^java.util.concurrent.BlockingQueue report-q
                                         1 java.util.concurrent.TimeUnit/SECONDS)]
                       (case (async/alt!!
                               [[inner-chan e]] :wrote
                               ;; The value for :default is eagerly evaluated, unlike for other clauses
                               :default :dropped
                               :priority true)
                         :dropped (log/error "dropped item from tx-report-mult--maybe there's a slow consumer?")
                         nil))
                     (catch InterruptedException e nil)))
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
                            (d/add-listener fut #(async/close! c) clojure.lang.Agent/pooledExecutor)
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

(deftraced pq
  "Executes a datomic query under miniprofiler"
  {:custom-timing ["datomic" "query"
                   (fn [query & _] (with-out-str (pprint query)))]}
  ([query] (q query))
  ([query a] (q query a))
  ([query a b] (q query a b))
  ([query a b c] (q query a b c))
  ([query a b c d] (q query a b c d))
  ([query a b c d e] (q query a b c d e))
  ([query a b c d e f] (q query a b c d e f))
  ([query a b c d e f & rest] (apply q query a b c d e f rest)))
