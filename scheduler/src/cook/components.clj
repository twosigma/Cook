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
(ns cook.components
  (:gen-class)
  (:require [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :as log]
            [compojure.core :refer [context GET POST routes]]
            [compojure.route :as route]
            [congestion.middleware :refer [ip-rate-limit wrap-rate-limit]]
            [congestion.storage :as storage]
            ; This explicit require is needed so that mount can see the defstate defined in the cook.caches namespace.
            [cook.caches]
            [cook.compute-cluster :as cc]
            [cook.config :refer [config]]
            [cook.datomic :as datomic]
            ; This explicit require is needed so that mount can see the defstate defined in the cook.plugins.adjustment namespace.
            [cook.plugins.adjustment]
            ; This explicit require is needed so that mount can see the defstate defined in the cook.plugins.completion namespace.
            [cook.plugins.completion]
            ; This explicit require is needed so that mount can see the defstate defined in the cook.plugins.file namespace.
            [cook.plugins.file]
            ; This explicit require is needed so that mount can see the defstate defined in the cook.plugins.job-submission-modifier namespace.
            [cook.plugins.job-submission-modifier]
            ; This explicit require is needed so that mount can see the defstate defined in the cook.plugins.launch namespace.
            [cook.plugins.launch]
            ; This explicit require is needed so that mount can see the defstate defined in the cook.plugins.pool namespace.
            [cook.plugins.pool]
            ; This explicit require is needed so that mount can see the defstate defined in the cook.plugins.submission namespace.
            [cook.plugins.submission]
            [cook.pool :as pool]
            ; This explicit require is needed so that mount can see the defstate defined in the cook.prometheus-metrics namespace.
            [cook.prometheus-metrics]
            [cook.queue-limit :as queue-limit]
            ; This explicit require is needed so that mount can see the defstate defined in the cook.quota namespace.
            [cook.quota :as quota]
            [cook.rate-limit]
            [cook.rest.cors :as cors]
            [cook.rest.impersonation :refer [impersonation-authorized-wrapper]]
            [cook.util :as util]
            [datomic.api :as d]
            [fork.metrics-clojure.metrics.jvm.core :as metrics-jvm]
            [metrics.ring.instrument :refer [instrument]]
            [mount.core :as mount]
            [plumbing.core :refer [fnk]]
            [plumbing.graph :as graph]
            [ring.middleware.cookies :refer [wrap-cookies]]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.stacktrace :refer [wrap-stacktrace]]
            [ring.util.mime-type]
            [ring.util.response :refer [response]])
  (:import (clojure.core.async.impl.channels ManyToManyChannel)
           (java.io IOException)
           (java.security Principal)
           (javax.security.auth Subject)
           (javax.servlet ServletInputStream)
           (org.apache.curator.framework CuratorFrameworkFactory)
           (org.apache.curator.framework.state ConnectionStateListener)
           (org.apache.curator.retry BoundedExponentialBackoffRetry)
           (org.eclipse.jetty.security DefaultUserIdentity UserAuthentication)
           (org.eclipse.jetty.server.handler HandlerCollection RequestLogHandler)
           (org.eclipse.jetty.server NCSARequestLog Request)))

(defn wrap-no-cache
  [handler]
  (fn [req]
    (let [resp (handler req)]
      (assoc-in resp
                [:headers "Cache-control"]
                "max-age=0"))))

(def raw-scheduler-routes
  {:scheduler (fnk [mesos leadership-atom pool-name->pending-jobs-atom progress-update-chans settings]
                ((util/lazy-load-var 'cook.rest.api/main-handler)
                  datomic/conn
                  (fn [] @pool-name->pending-jobs-atom)
                  settings
                  (get-in mesos [:mesos-scheduler :leader-selector])
                  leadership-atom
                  progress-update-chans))
   :view (fnk [scheduler]
           scheduler)})

(def full-routes
  {:raw-scheduler raw-scheduler-routes
   :view (fnk [raw-scheduler]
           (routes (:view raw-scheduler)
                   (route/not-found "<h1>Not a valid route</h1>")))})

(def mesos-scheduler
  {:mesos-scheduler (fnk [[:settings hostname mea-culpa-failure-limit mesos-leader-path mesos-run-as-user
                           offer-incubate-time-ms optimizer rebalancer server-port task-constraints]
                          compute-clusters curator-framework mesos-datomic-mult leadership-atom
                          pool-name->pending-jobs-atom mesos-heartbeat-chan
                          trigger-chans]

                      ; We track queue limits on all nodes, not just the leader, because
                      ; we need to check them when job submission requests come in
                      (queue-limit/start-updating-queue-lengths)
                      (if curator-framework
                        (do
                          (log/info "Initializing mesos scheduler")
                          (try
                            (Class/forName "org.apache.mesos.Scheduler")
                            ((util/lazy-load-var 'cook.mesos/start-leader-selector)
                              {:curator-framework curator-framework
                               :mea-culpa-failure-limit mea-culpa-failure-limit
                               :mesos-datomic-conn datomic/conn
                               :mesos-datomic-mult mesos-datomic-mult
                               :mesos-heartbeat-chan mesos-heartbeat-chan
                               :leadership-atom leadership-atom
                               :pool-name->pending-jobs-atom pool-name->pending-jobs-atom
                               :mesos-run-as-user mesos-run-as-user
                               :offer-incubate-time-ms offer-incubate-time-ms
                               :optimizer-config optimizer
                               :rebalancer-config rebalancer
                               :server-config {:hostname hostname
                                               :server-port server-port}
                               :task-constraints task-constraints
                               :trigger-chans trigger-chans
                               :zk-prefix mesos-leader-path
                               :api-only? (cook.config/api-only-mode?)})
                            (catch ClassNotFoundException e
                              (log/warn e "Not loading mesos support...")
                              nil)))
                        (throw (ex-info "This node does not have a curator configured" {}))))})

(defn health-check-middleware
  "This adds /debug to return 200 OK"
  [h leadership-atom leader-reports-unhealthy]
  (fn healthcheck [req]
    (if (and (= (:uri req) "/debug")
             (= (:request-method req) :get))
      {:status (if (and leader-reports-unhealthy @leadership-atom)
                 503
                 200)
       :headers {}
       :body (str "Server is running version: " @util/version " (commit " @util/commit ")")}
      (h req))))

(def curator-framework
  (fnk [[:settings zookeeper]]
    (when zookeeper
      (log/info "Using zookeeper connection string:" zookeeper)
      (let [retry-policy (BoundedExponentialBackoffRetry. 100 120000 10)
            curator-framework (CuratorFrameworkFactory/newClient zookeeper 180000 30000 retry-policy)]
        (.. curator-framework
            getConnectionStateListenable
            (addListener (reify ConnectionStateListener
                           (stateChanged [_ _ newState]
                             (log/info "Curator state changed:"
                                       (str newState))))))
        (.start curator-framework)
        curator-framework))))

(defn tell-jetty-about-usename [h]
  "Our auth in cook.rest.spnego doesn't hook in to Jetty - this handler
  does so to make sure it's available for Jetty to log"
  (fn [req]
    (do
      (.setAuthentication ^Request (:servlet-request req)
                          (UserAuthentication.
                            "kerberos"
                            (DefaultUserIdentity.
                              (Subject.)
                              (reify Principal ; Shim principal to pass username along
                                (equals [this another]
                                  (= this another))
                                (getName [_]
                                  (:authorization/user req))
                                (toString [_]
                                  (str "Shim principal for user: " (:authorization/user req))))
                              (into-array String []))))
      (h req))))

(defn configure-jet-logging
  "Set up access logs for Jet"
  [server]
  (doto server
    (.setHandler (doto (HandlerCollection.)
                   (.addHandler (.getHandler server))
                   (.addHandler (doto (RequestLogHandler.)
                                  (.setRequestLog (doto (NCSARequestLog.)
                                                    (.setFilename "log/access_log-yyyy_mm_dd.log")
                                                    (.setAppend true)
                                                    (.setLogLatency true)
                                                    (.setExtended true)
                                                    (.setPreferProxiedForAddress true)))
                                  (.setServer server)))))))

(defn conditional-auth-bypass
  "Skip authentication on some hard-coded endpoints."
  [h auth-middleware]
  (let [auth-fn (auth-middleware h)
        no-auth-pattern #"/(?:info|metrics|progress/[-\w]+)"]
    (fn filtered-auth [{:keys [uri request-method] :as req}]
      (if (re-matches no-auth-pattern uri)
        (h req)
        (auth-fn req)))))

(defn conditional-rate-limit
  "Set rate limits, with support for endpoints with custom limits."
  [h rate-limit-storage default-user-limit user-limit-for-metrics]
  (let [metrics-rate-limit-pattern #"/metrics"]
    (fn filtered-rate-limit [{:keys [uri] :as req}]
      (if (re-matches metrics-rate-limit-pattern uri)
        ((wrap-rate-limit h {:storage rate-limit-storage :limit user-limit-for-metrics}) req)
        ((wrap-rate-limit h {:storage rate-limit-storage :limit default-user-limit}) req)))))

(defn prometheus-path-fn
  "By default, the path will be the entire URI, but since we have UUID-based URI paths, we need to truncate them."
  [{:keys [uri]}]
  (clojure.string/replace
            uri
            #"[0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12}"
            "<UUID>"))

(defn- consume-request-stream [handler]
  (fn [{:keys [body] :as request}]
    (let [resp (handler request)]
      (if (and (instance? ServletInputStream body)
               (not (.isFinished ^ServletInputStream body))
               (not (instance? ManyToManyChannel resp)))
        (try
          (slurp body)
          (catch IOException e
            (log/error e "Unable to consume request stream"))))
      resp)))

(defn- wrap-exception-logging
  [handler]
  (fn wrap-exception-logging [request]
    (try
      (handler request)
      (catch Exception e
        (let [{:keys [params remote-addr :authorization/user uri request-method]} request]
          (log/error e "Unhandled exception in ring handler" {:params params
                                                              :remote-addr remote-addr
                                                              :user user
                                                              :uri uri
                                                              :request-method request-method}))
        (throw e)))))

(def scheduler-server
  (graph/eager-compile
    {:route full-routes
     :http-server (fnk [[:settings cors-origins server-port authorization-middleware impersonation-middleware
                         leader-reports-unhealthy server-https-port server-keystore-path server-keystore-type
                         server-keystore-pass [:rate-limit user-limit user-limit-for-metrics]]
                        [:route view] leadership-atom]
                    (log/info "Launching http server")
                    (let [rate-limit-storage (storage/local-storage)
                          jetty ((util/lazy-load-var 'qbits.jet.server/run-jetty)
                                 (cond-> {:ring-handler (routes
                                                          (route/resources "/resource")
                                                          (-> view
                                                            (cook.prometheus-metrics/wrap-ring-instrumentation {:path-fn prometheus-path-fn})
                                                            (conditional-rate-limit rate-limit-storage user-limit
                                                                                    user-limit-for-metrics)
                                                            tell-jetty-about-usename
                                                            impersonation-middleware
                                                            (conditional-auth-bypass authorization-middleware)
                                                            wrap-exception-logging
                                                            wrap-stacktrace
                                                            wrap-no-cache
                                                            wrap-cookies
                                                            wrap-params
                                                            (cors/cors-middleware cors-origins)
                                                            (health-check-middleware leadership-atom leader-reports-unhealthy)
                                                            instrument
                                                            consume-request-stream))
                                          :join? false
                                          :configurator configure-jet-logging
                                          :max-threads 200
                                          :request-header-size 32768}
                                   server-port (assoc :port server-port)
                                   server-https-port (assoc :ssl-port server-https-port)
                                   server-keystore-pass (assoc :key-password server-keystore-pass)
                                   server-keystore-path (assoc :keystore server-keystore-path)
                                   server-keystore-type (assoc :keystore-type server-keystore-type)))]
                      (fn [] (.stop jetty))))
     :compute-clusters (fnk [exit-code-syncer-state
                             mesos-agent-query-cache
                             mesos-heartbeat-chan
                             settings
                             progress-update-chans
                             trigger-chans]
                         (reset! cc/exit-code-syncer-state-atom exit-code-syncer-state)
                         (doall (map (fn [{:keys [factory-fn config]}]
                                       (let [resolved (util/lazy-load-var factory-fn)]
                                         (log/info "Calling compute cluster factory fn" factory-fn "with config" config)
                                         (resolved config {:exit-code-syncer-state exit-code-syncer-state
                                                           :mesos-agent-query-cache mesos-agent-query-cache
                                                           :mesos-heartbeat-chan mesos-heartbeat-chan
                                                           :sandbox-syncer-config (:sandbox-syncer settings)
                                                           :progress-update-chans progress-update-chans
                                                           :trigger-chans trigger-chans})))
                                     (:compute-clusters settings))))
     :progress-update-chans (fnk [[:settings [:progress :as progress-config]] trigger-chans]
                              (let [{:keys [progress-updater-trigger-chan]} trigger-chans]
                                ;; XXX - We should be able to :require cook.progress rather than using lazy-load-var here,
                                ;; but there's currently a compile-time bug that prevents that: https://github.com/twosigma/Cook/issues/1370
                                ((util/lazy-load-var 'cook.progress/make-progress-update-channels)
                                 progress-updater-trigger-chan progress-config datomic/conn)))
     :mesos-datomic-mult (fnk []
                           (first ((util/lazy-load-var 'cook.datomic/create-tx-report-mult) datomic/conn)))
     ; TODO(pschorf): Remove hearbeat support
     :mesos-heartbeat-chan (fnk []
                             (async/chan (async/buffer 4096)))
     :local-zookeeper (fnk [[:settings zookeeper-server]]
                        (when zookeeper-server
                          (log/info "Starting local ZK server")
                          (.start zookeeper-server)))
     :mesos mesos-scheduler
     :mesos-agent-query-cache (fnk [[:settings [:agent-query-cache max-size ttl-ms]]]
                                (-> {}
                                  (cache/lru-cache-factory :threshold max-size)
                                  (cache/ttl-cache-factory :ttl ttl-ms)
                                  atom))
     :exit-code-syncer-state (fnk [[:settings [:exit-code-syncer publish-batch-size publish-interval-ms]]]
                               ((util/lazy-load-var 'cook.mesos.sandbox/prepare-exit-code-publisher)
                                datomic/conn publish-batch-size publish-interval-ms))
     :trigger-chans (fnk [[:settings rebalancer progress optimizer task-constraints]]
                      ((util/lazy-load-var 'cook.mesos/make-trigger-chans) rebalancer progress optimizer task-constraints))
     :clear-uncommitted-canceler (fnk [leadership-atom]
                                   ((util/lazy-load-var 'cook.tools/clear-uncommitted-jobs-on-schedule)
                                    datomic/conn leadership-atom))
     :leadership-atom (fnk [] (atom false))
     :pool-name->pending-jobs-atom (fnk [] (atom {}))
     :curator-framework curator-framework}))

(defn -main
  "Entry point for Cook. Initializes configuration settings,
  instruments the JVM, and starts up the scheduler and API."
  [config-file-path & _]
  (println "Cook" @util/version "( commit" @util/commit ")")
  (try
    ; Note: If the mount/start-with-args fails to initialize a defstate S, and/or you get weird errors on startup,
    ; you need to require S's namespace with ns :require. 'ns :require' is how mount finds defstates to initialize.
    ;
    ; If you get an error about "Can't embed object in code, maybe print-dup not defined: clojure.lang.Delay"
    ; The issue is that at least metatransaction.core seems to be incompatible with mount. It cannot be in the
    ; dependency tree of anything using mount. See also issue #1370
    (mount/start-with-args (cook.config/read-config config-file-path))
    (pool/guard-invalid-default-pool (d/db datomic/conn))
    (metrics-jvm/instrument-jvm)
    (let [server (scheduler-server config)]
      (intern 'user 'main-graph server)
      (log/info "Started Cook, stored variable in user/main-graph"))
    (catch Throwable t
      (log/error t "Failed to start Cook")
      (System/exit 1))))
