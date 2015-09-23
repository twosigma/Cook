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
(ns clj-http-async-pool.test-utils
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [clj-http-async-pool.core :refer :all]
            [clj-http-async-pool.circuit-breaker :as circuit-breaker]
            [clj-http-async-pool.pool :as pool]
            [clj-http-async-pool.router :as router]))

(comment
  (do
    (require 'clj-logging-config.log4j)
    (clj-logging-config.log4j/set-loggers!
     (org.apache.log4j.Logger/getRootLogger)
     {:pattern "%d{ISO8601} [%15.15t] %-5p %30.30c %x - %m%n" ;org.apache.log4j.EnhancedPatternLayout/TTCC_CONVERSION_PATTERN
                          :level :debug}))
)

(defn multiply-duration
  [base]
  (* base (Long. (or (System/getenv "CLJ_HTTP_ASYNC_POOL_TEST_DURATION_MULTIPLIER")
                     1))))

(defmacro timed
  [& body]
  `(let [ch# (async/chan 1)
         elapsed-str# (with-out-str
                       (time
                        (let [res# (do ~@body)]
                          (async/>!! ch# res#))))
         elapsed# (Integer/parseInt (re-find #"\d+" elapsed-str#))]
     {:elapsed elapsed#
      :result (async/<!! ch#)}))

(defmacro with-lifecycle
  [binding & body]
  (assert (vector? binding))
  (assert (= 2 (count binding)))
  `(let [~(first binding) (start ~(second binding))]
     (try
       ~@body
       (finally (stop ~(first binding))))))

(defn test-http-opts
  [{:keys [connection-timeout-ms socket-timeout-ms]
    :or {connection-timeout-ms 100
         socket-timeout-ms 100}}]
  (pool/map->HTTPOptions
   {:connection-timeout-ms connection-timeout-ms
    :socket-timeout-ms socket-timeout-ms}))

(defn test-persistent-connection-opts
  [{:keys [connection-ttl-secs default-per-route]
    :or {connection-ttl-secs 60
         default-per-route 2}}]
  (pool/map->PersistentConnectionOptions
   {:connection-ttl-secs connection-ttl-secs
    :default-per-route default-per-route}))

(defn test-pool-opts
  [{:keys [nthreads]
    :or {nthreads 1}
    :as user-opts}]
  (pool/map->ConnectionPoolOptions
   {:nthreads nthreads
    :http-opts (test-http-opts user-opts)
    :persistent-connection-opts (test-persistent-connection-opts user-opts)}))

(defn test-circuit-breaker-opts
  [{:keys [response-timeout-ms lifetime-ms failure-threshold reset-timeout-ms failure-logger-size]
    :or {response-timeout-ms 1000
         lifetime-ms 1000
         failure-threshold 0.8
         reset-timeout-ms 1000
         failure-logger-size 100}}]
  (circuit-breaker/map->CircuitBreakerOptions
   {:response-timeout-ms response-timeout-ms
    :lifetime-ms lifetime-ms
    :failure-threshold failure-threshold
    :reset-timeout-ms reset-timeout-ms
    :failure-logger-size failure-logger-size}))

(defn test-router-opts
  [{:keys [hosts]
    :or {hosts #{}}
    :as user-opts}]
  (router/map->ConnectionPoolRouterOptions
   {:hosts hosts
    :pool-opts (test-pool-opts user-opts)
    :circuit-breaker-opts (test-circuit-breaker-opts user-opts)}))
