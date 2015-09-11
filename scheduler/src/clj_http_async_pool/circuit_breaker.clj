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
(ns clj-http-async-pool.circuit-breaker
  "Implements the Circuit Breaker pattern using core.async.

  We assume you have a service modeled as a core.async channel.  To
  make requests, we push a map into that channel, which has
  a :response-chan element which should be a channel onto which we
  expect the result will be pushed.  This allows us to model the
  circuit breaker as middleware by wrapping the service's channel and
  supplying our own.

  The circuit breaker is composed of an event loop and a state
  machine.  The event loop handles request and timing events, and
  updates the state machine.  For each request, we spawn a goroutine
  to handle the timing out of that request, and that goroutine
  notifies the event loop of success or failure, which gets brokered
  back through to the state machine.

  The currently implemented state machine tracks an EMA where
  successes are interpreted as a +1 signal and failures are a -1
  signal.  We parameterize the mean lifetime in milliseconds of the
  EMA, and the \"failure threshold\".  The output of the EMA is a
  value between -1 and +1, and when the EMA value drops below the
  failure threshold, we break the circuit.

  The other parameters are the response timeout, which is the timeout
  for each request before it is considered a failure, and the reset
  timeout, which is the time the circuit stays open before we try some
  requests again.

  Example:

    ;;; Original code, posts a request to service and waits for a
    ;;; response.
    (let [service (make-service ...)]
      ...
      (let [ch (async/chan)]
        (async/go
          (async/>! service
                    {:request ...
                     :response-chan ch})
          (async/<! ch))))

    ;;; Code with circuit breaker, we wrap the raw service and use
    ;;; (:request-chan circuit-breaker) instead.
    (let [raw-service (make-raw-service ...)
          circuit-breaker (make-circuit-breaker opts raw-service)]
      ...
      (let [ch (async/chan)]
        (async/go
          (async/>! (:request-chan circuit-breaker)
                    {:request ...
                     :response-chan ch}))))
  "
  (:require [amalloy.ring-buffer :refer (ring-buffer)]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [clj-http-async-pool.utils :refer :all]))

(defrecord CircuitBreakerOptions
    [lifetime-ms                ; Mean lifetime parameter for the EMA.
     failure-threshold          ; When the EMA value drops below this,
                                ; we break the circuit
     response-timeout-ms        ; Time before a request is considered
                                ; failed
     reset-timeout-ms           ; Time the circuit will stay open
                                ; before it returns to half-open
     failure-logger-size        ; Number of past failed requests to
                                ; keep in the log.
     ])

(defprotocol CircuitState
  "The state machine that implements the guts of the circuit breaker."
  (open? [this]
    "Returns true when the circuit is open, i.e. we should drop
    requests on the floor.")
  (update [this success? req duration-ms]
    "Updates the state after a request.  Includes the request itself
    and how long it took, if the state would like to do something
    smart with that.")
  (timeout-for [this req]
    "Returns the timeout we should use for the given request.  Naively
    this could be a constant, but if we build a model of the request
    duration maybe we can predict something by looking at the request
    object.")
  (reset [this]
    "If the state is open, this will reset it to half-open."))

(defrecord EMACircuitState [lifetime-ms failure-threshold timeout-ms
                            mode avg last-observation-ns]
  CircuitState
  (open? [_] (= mode :open))
  (update [this success? _ _]
    (let [current-ns (System/nanoTime)
          minus-delta-t (- last-observation-ns current-ns)
          ;; \alpha = 1-e^{-\Delta t / T}
          alpha (- 1 (java.lang.Math/exp (/ minus-delta-t (* 1000 1000 lifetime-ms))))
          new-avg (+ (* alpha (if success? +1 -1))
                     (* (- 1 alpha) avg))
          new-mode (condp = mode
                     :closed (if (< new-avg failure-threshold) :open :closed)
                     :open :open
                     :half-open (if success? :closed :open))]
      (assoc this
             :mode new-mode
             :avg new-avg
             :last-observation-ns current-ns)))
  (timeout-for [_ _] timeout-ms)
  (reset [this]
    (if (= mode :open)
      (assoc this :mode :half-open)
      this)))

(defn- make-ema-circuit-state
  ([circuit-breaker-opts]
   (make-ema-circuit-state (:lifetime-ms circuit-breaker-opts)
                           (:failure-threshold circuit-breaker-opts)
                           (:response-timeout-ms circuit-breaker-opts)))
  ([lifetime-ms failure-threshold timeout-ms]
   (->EMACircuitState lifetime-ms failure-threshold timeout-ms
                      :closed 1 (System/nanoTime))))

(defprotocol FailureLogger
  "Logs failed requests."
  (log-failure [this request response duration-ms]
    "Logs the request and response that took duration-ms to receive,
    even if we didn't get a chance to forward on the response.")
  (get-failures [this]
    "Returns a seq of failed request details."))

(defrecord FixedSizeFailureLogger [buffer]
  FailureLogger
  (log-failure [this request response duration-ms]
    (update-in this [:buffer]
               #(conj % {:request request
                         :response response
                         :duration-ms duration-ms
                         :log-timestamp (System/currentTimeMillis)})))
  (get-failures [_] (seq buffer)))

(defn- make-fixed-size-failure-logger [num-entries]
  (->FixedSizeFailureLogger (ring-buffer num-entries)))

(defn- post-request-and-wait
  "Passes through request to inner-request-chan."
  [request response-timeout-ms inner-request-chan result-chan]
  (async/go
    (let [outer-response-chan (:response-chan request)
          inner-response-chan (async/chan)]
      (try
        (let [begin-time-ms (System/currentTimeMillis)
              request' (assoc request :response-chan inner-response-chan)
              timeout (async/timeout response-timeout-ms)
              _ (log/debug "Breaker >>" (dissoc request :response-chan))
              posted? (async/alt!
                        [[inner-request-chan request']] ([v _] true)
                        timeout ([_] false)
                        :priority true)
              response (when posted?
                         (async/alt!
                           inner-response-chan ([response] response)
                           timeout ([_] nil)
                           :priority true))
              success? (when response
                         (log/debug "Breaker <<" (dissoc request :response-chan) ":" response)
                         (async/>! outer-response-chan response))]
          (async/>! result-chan
                    {:success? success?
                     :request request
                     :response response
                     :duration-ms (- (System/currentTimeMillis) begin-time-ms)}))
        (finally
          (async/close! inner-response-chan)
          (async/close! outer-response-chan))))))

(defn- circuit-breaker-loop
  "Main circuit breaker loop."
  [outer-request-chan circuit-breaker-opts inner-request-chan failure-logger stats-chan]
  (async/go-loop [state (make-ema-circuit-state circuit-breaker-opts)
                  stats {:passed 0
                         :failed 0
                         :dropped 0
                         :pending 0}
                  failure-logger failure-logger
                  reset-timeout-chan (async/chan)
                  result-chan (async/chan)]
    (async/alt!
      outer-request-chan
      ([request] (if request
                   (if (open? state)
                     (do
                       (async/close! (:response-chan request))
                       (recur state (update-in stats [:dropped] inc)
                              failure-logger reset-timeout-chan result-chan))
                     (do
                       (post-request-and-wait request
                                              (timeout-for state request)
                                              inner-request-chan
                                              result-chan)
                       (recur state (update-in stats [:pending] inc)
                              failure-logger reset-timeout-chan result-chan)))
                   (log-stop "CircuitBreaker"
                     (async/close! inner-request-chan)
                     (async/close! stats-chan)
                     stats)))
      result-chan
      ([result] (let [status-keyword (if (:success? result) :passed :failed)
                      new-state (update state (:success? result) (:request result) (:duration-ms result))
                      just-tripped? (and (not (:success? result))
                                         (open? new-state)
                                         (not (open? state)))
                      new-reset-timeout-chan (if just-tripped?
                                               (async/timeout (:reset-timeout-ms circuit-breaker-opts))
                                               reset-timeout-chan)]
                  (when-not (:success? result)
                    (log-failure failure-logger (:request result) (:response result) (:duration-ms result)))
                  (recur new-state (-> stats
                                       (update-in [:pending] dec)
                                       (update-in [status-keyword] inc))
                         failure-logger new-reset-timeout-chan result-chan)))
      reset-timeout-chan
      ([_] (recur (reset state) stats failure-logger (async/chan) result-chan))
      [[stats-chan (assoc stats :mode (:mode state))]]
      ([_ _] (recur state stats failure-logger reset-timeout-chan result-chan)))))

(defn make-circuit-breaker
  "Wraps inner-request-chan in a circuit breaker.  Returns a new
  request-chan that can be used instead, along with a chan you can
  block on to wait for the loop to join.  Also returns stats-chan
  which you can poll for the current status of the circuit breaker.
  "
  [circuit-breaker-opts inner-request-chan]
  (log-start "CircuitBreaker"
    (let [outer-request-chan (async/chan)
          failure-logger (make-fixed-size-failure-logger (:failure-logger-size circuit-breaker-opts))
          stats-chan (async/chan)
          join-chan (circuit-breaker-loop outer-request-chan
                                          circuit-breaker-opts
                                          inner-request-chan
                                          failure-logger
                                          stats-chan)]
      {:request-chan outer-request-chan
       :join-chan join-chan
       :failure-logger failure-logger
       :stats-chan stats-chan})))
