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
(ns clj-http-async-pool.router
  "Implements a router on top of per-destination threadpools, with
  circuit breaking to prevent rolling failures and excessive
  user-facing timeouts during network partitions.

  Each host or regex-matched group of hosts gets a separate threadpool
  so that one bad-acting host can't block the threads servicing other
  hosts.

  We also use the circuit breaker to allow for faster timeouts while
  there are network/host problems.

  Like clj-http-async-pool.pool, requests return a channel onto which
  the response will eventually be placed.  For a prettier interface,
  use clj-http-async-pool.client.  If the request times out, the
  response channel will be closed without receiving anything, so takes
  will return nil.

  For configuration info, check the \"Options\" records in router.clj,
  circuit_breaker.clj, and pool.clj.

  Example:

    ;; With clj-http.client:
    (use '[clj-http.client :as http])
    (http/get \"https://www.random.org/sequences/?min=1&max=42&col=1&format=plain\")

    ;; With clj-http-async-pool.router
    (use '[clj-http-async-pool.router :as http-router])
    (def router (http-router/make-router {:hosts #{\"www.random.org:80\"}}))
    (use '[clj-http-async-pool.client :as http])
    (http/get router \"https://www.random.org/sequences/?min=1&max=42&col=1&format=plain\")
  "
  (:require [clj-http.client :as client]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [clj-http-async-pool.core :refer :all]
            [clj-http-async-pool.circuit-breaker :refer (make-circuit-breaker)]
            [clj-http-async-pool.pool :refer (make-connection-pool)]
            [clj-http-async-pool.utils :refer :all]))

(defn host-key
  "Constructs a topic-fn suitable for async/pub based on a collection
  of hosts, either flat strings or regexes.
  "
  [hosts]
  (fn [w]
    (let [req (:req w)
          {:keys [server-name server-port]} (or (some-> req
                                                        :url
                                                        client/parse-url)
                                                (select-keys req #{:server-name :server-port}))
          host (str server-name ":" (or server-port 80))]
      (or (some #(if (instance? java.util.regex.Pattern %)
                   (when (re-find % host)
                     (str %))
                   (when (= % host)
                     %))
                hosts)
          ::default))))

(defrecord ConnectionPoolRouterOptions
    [hosts                      ; Set of strings or regexes grouping
                                ; hosts behind distinct connection
                                ; pools
     pool-opts                  ; Connection pool options
     circuit-breaker-opts       ; Circuit breaker options
     ])

;;; Routes requests to per-host ConnectionPools or a default pool used
;;; for all other hosts.  Must specify up-front which hosts should get
;;; their own pools.
(defrecord ConnectionPoolRouter [router-opts
                                 work-chan conn-pools circuit-breakers]
  Lifecycle
  (start [this]
    (if work-chan
      this
      (log-start "ConnectionPoolRouter"
        (let [work-chan (async/chan)
              hosts (:hosts router-opts)
              work-pub (async/pub work-chan (host-key hosts))
              parts (for [host (conj (map str hosts) ::default)]
                      (let [conn-pool (start (make-connection-pool (:pool-opts router-opts)))
                            circuit-breaker (make-circuit-breaker (:circuit-breaker-opts router-opts)
                                                                  (:work-chan conn-pool))]
                        (async/sub work-pub host (:request-chan circuit-breaker))
                        {:host host
                         :conn-pool conn-pool
                         :circuit-breaker circuit-breaker}))
              conn-pools (into {} (map (juxt :host :conn-pool) parts))
              circuit-breakers (into {} (map (juxt :host :circuit-breaker) parts))]
          ;; Uncomment this to periodically dump the circuit breaker
          ;; stats to the log.
          #_(doall
             (for [{:keys [host circuit-breaker]} circuit-breakers]
               (async/go-loop []
                 (async/<! (async/timeout 1000))
                 (when-let [state (async/<! (:state-chan circuit-breaker))]
                   (log/debug "Host:" host "Breaker state:" state)
                   (recur)))))
          (assoc this
                 :work-chan work-chan
                 :conn-pools conn-pools
                 :circuit-breakers circuit-breakers)))))
  (stop [this]
    (if work-chan
      (log-stop "ConnectionPoolRouter"
        (async/close! work-chan)
        (doseq [[host circuit-breaker] circuit-breakers]
          (async/<!! (:join-chan circuit-breaker)))
        (doseq [[host conn-pool] conn-pools]
          (stop conn-pool))
        (assoc this
               :work-chan nil
               :conn-pools nil
               :circuit-breakers nil))
      this))
  AsyncHttpClient
  (request [this req]
    (when work-chan
      (let [response-chan (async/chan)
            work {:req req
                  :response-chan response-chan}]
        (async/go
          (log/debug "Router >>" req)
          (async/>! work-chan work)
          (let [response (async/<! response-chan)]
            (log/debug "Router <<" req ":" response)
            response))))))

(defn make-router [router-opts]
  (map->ConnectionPoolRouter {:router-opts router-opts}))
