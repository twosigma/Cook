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
(ns clj-http-async-pool.pool
  "Implements a basic HTTP request connection pool.

  We use a preforking threadpool to wait on blocking HTTP requests,
  and provide a core.async compatible interface.  Requests return a
  channel onto which the response will eventually be placed.  For a
  prettier interface, use clj-http-async-pool.client.
  "
  (:require [clj-http.client :as client]
            [clj-http.conn-mgr :as conn-mgr]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [clj-http-async-pool.core :refer :all]
            [clj-http-async-pool.utils :refer :all]))

(defrecord HTTPOptions
    [connection-timeout-ms      ; Timeout for establishing the
                                ; connection
     socket-timeout-ms          ; Intra-packet timeout for underlying
                                ; TCP connection
     ])

(defrecord PersistentConnectionOptions
    [connection-ttl-secs        ; Time the persistent connection tries
                                ; to stay alive
     default-per-route          ; Default number of concurrent
                                ; requests to each given route
     ])

(defrecord ConnectionPoolOptions
    [nthreads                   ; Number of worker threads
     http-opts                  ; HTTP options
     persistent-connection-opts ; Persistent connection options
     ])

(defn- pool-worker
  "Creates a function that can accept requests and serve them, merging
  in the defaults from the http options and the connection manager.
  "
  [http-opts conn-mgr]
  (fn [{:keys [req response-chan]}]
    (let [req' (merge (->> {:socket-timeout (:socket-timeout-ms http-opts)
                            :conn-timeout (:connection-timeout-ms http-opts)
                            :connection-manager conn-mgr
                            :throw-exceptions false}
                           (remove (comp nil? second))
                           (into {}))
                      req)]
      (try
        (log/debug "Pool >>" req)
        (let [response (client/request req')]
          (log/debug "Pool <<" req ":" response)
          (async/>!! response-chan response))
        (catch Throwable e
          (log/error "Pool worker: processing request " (select-keys req' #{:server-name :server-port :uri :url}) ":" e)
          (some-> e ex-data :object))
        (finally
          (async/close! response-chan)))
      nil)))

;;; A pool of threads for making HTTP requests.  Manages a clj-http
;;; connection manager and a threadpool.
;;;
;;; Can be used by pushing objects with keys [req response-chan] onto
;;; work-chan and waiting for a response on response-chan.
(defrecord ConnectionPool [pool-opts
                           conn-mgr work-chan out-chan]
  Lifecycle
  (start [this]
    (if conn-mgr
      this
      (log-start "ConnectionPool"
        (let [nthreads (:nthreads pool-opts)
              conn-mgr (conn-mgr/make-reusable-conn-manager
                        (->>  {:timeout (get-in pool-opts [:persistent-connection-opts :connection-ttl-secs])
                               :threads nthreads
                               :default-per-route (get-in pool-opts [:persistent-connection-opts :default-per-route])}
                              (remove (comp nil? second))
                              (into {})))
              work-chan (async/chan)
              out-chan (async/chan)]
          (async/pipeline-blocking nthreads out-chan
                                   (comp
                                    (map-xf (pool-worker (:http-opts pool-opts) conn-mgr))
                                    (filter-xf (comp not nil?)))
                                   work-chan)
          (assoc this
                 :conn-mgr conn-mgr
                 :work-chan work-chan
                 :out-chan out-chan)))))
  (stop [this]
    (if conn-mgr
      (log-stop "ConnectionPool"
        (async/close! work-chan)
        (async/<!! out-chan)
        (conn-mgr/shutdown-manager conn-mgr)
        (assoc this
               :conn-mgr nil
               :work-chan nil
               :out-chan nil))
      this))
  AsyncHttpClient
  (request [this req]
    (when work-chan
      (let [response-chan (async/chan)
            work {:req req
                  :response-chan response-chan}]
        (async/go
          (async/>! work-chan work)
          (async/<! response-chan))))))

(defn make-connection-pool [pool-opts]
  (map->ConnectionPool {:pool-opts pool-opts}))
