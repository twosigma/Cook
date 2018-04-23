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
  (:require [clojure.core.cache :as cache]
            [clojure.pprint :refer (pprint)]
            [clojure.tools.logging :as log]
            [compojure.core :refer (GET POST routes context)]
            [compojure.route :as route]
            [congestion.middleware :refer (wrap-rate-limit ip-rate-limit)]
            [congestion.storage :as storage]
            [cook.config :refer (config)]
            [cook.cors :as cors]
            [cook.curator :as curator]
            [cook.datomic :as datomic]
            [cook.impersonation :refer (impersonation-authorized-wrapper)]
            [cook.mesos.pool :as pool]
            [cook.util :as util]
            [datomic.api :as d]
            [metrics.jvm.core :as metrics-jvm]
            [metrics.ring.instrument :refer (instrument)]
            [mount.core :as mount]
            [plumbing.core :refer (fnk)]
            [plumbing.graph :as graph]
            [ring.middleware.cookies :refer (wrap-cookies)]
            [ring.middleware.params :refer (wrap-params)]
            [ring.middleware.stacktrace :refer (wrap-stacktrace)]
            [ring.util.mime-type]
            [ring.util.response :refer (response)])
  (:import java.security.Principal
           javax.security.auth.Subject
           org.apache.curator.framework.CuratorFrameworkFactory
           org.apache.curator.framework.state.ConnectionStateListener
           org.apache.curator.retry.BoundedExponentialBackoffRetry
           org.eclipse.jetty.security.DefaultUserIdentity
           org.eclipse.jetty.security.UserAuthentication
           org.eclipse.jetty.server.NCSARequestLog
           org.eclipse.jetty.server.handler.HandlerCollection
           org.eclipse.jetty.server.handler.RequestLogHandler)
  (:gen-class))

(defn wrap-no-cache
  [handler]
  (fn [req]
    (let [resp (handler req)]
      (assoc-in resp
                [:headers "Cache-control"]
                "max-age=0"))))

(def raw-scheduler-routes
  {:scheduler (fnk [mesos mesos-leadership-atom mesos-pending-jobs-atom framework-id settings]
                ((util/lazy-load-var 'cook.mesos.api/main-handler)
                  datomic/conn
                  framework-id
                  (fn [] @mesos-pending-jobs-atom)
                  settings
                  (get-in mesos [:mesos-scheduler :leader-selector])
                  mesos-leadership-atom))
   :view (fnk [scheduler]
           scheduler)})

(def full-routes
  {:raw-scheduler raw-scheduler-routes
   :view (fnk [raw-scheduler]
           (routes (:view raw-scheduler)
                   (route/not-found "<h1>Not a valid route</h1>")))})

(def mesos-scheduler
  {:mesos-scheduler (fnk [[:settings fenzo-fitness-calculator fenzo-floor-iterations-before-reset
                           fenzo-floor-iterations-before-warn fenzo-max-jobs-considered fenzo-scaleback
                           good-enough-fitness hostname mea-culpa-failure-limit mesos-failover-timeout mesos-framework-name
                           mesos-gpu-enabled mesos-leader-path mesos-master mesos-master-hosts mesos-principal
                           mesos-role mesos-run-as-user offer-incubate-time-ms optimizer progress rebalancer server-port
                           task-constraints]
                          curator-framework framework-id mesos-datomic-mult mesos-leadership-atom
                          mesos-offer-cache mesos-pending-jobs-atom sandbox-syncer-state]
                      (if (cook.config/api-only-mode?)
                        (if curator-framework
                          (throw (ex-info "This node is configured for API-only mode, but also has a curator configured"
                                          {:curator-framework curator-framework}))
                          (log/info "This node is in API-only mode, and will not participate in scheduling"))
                        (if curator-framework
                          (do
                            (log/info "Initializing mesos scheduler")
                            (let [make-mesos-driver-fn (partial (util/lazy-load-var 'cook.mesos/make-mesos-driver)
                                                                {:mesos-master mesos-master
                                                                 :mesos-failover-timeout mesos-failover-timeout
                                                                 :mesos-principal mesos-principal
                                                                 :mesos-role mesos-role
                                                                 :mesos-framework-name mesos-framework-name
                                                                 :gpu-enabled? mesos-gpu-enabled})
                                  get-mesos-utilization-fn (partial (util/lazy-load-var 'cook.mesos/get-mesos-utilization) mesos-master-hosts)
                                  trigger-chans ((util/lazy-load-var 'cook.mesos/make-trigger-chans) rebalancer progress optimizer task-constraints)]
                              (try
                                (Class/forName "org.apache.mesos.Scheduler")
                                ((util/lazy-load-var 'cook.mesos/start-mesos-scheduler)
                                  {:curator-framework curator-framework
                                   :fenzo-config {:fenzo-max-jobs-considered fenzo-max-jobs-considered
                                                  :fenzo-scaleback fenzo-scaleback
                                                  :fenzo-floor-iterations-before-warn fenzo-floor-iterations-before-warn
                                                  :fenzo-floor-iterations-before-reset fenzo-floor-iterations-before-reset
                                                  :fenzo-fitness-calculator fenzo-fitness-calculator
                                                  :good-enough-fitness good-enough-fitness}
                                   :framework-id framework-id
                                   :get-mesos-utilization get-mesos-utilization-fn
                                   :gpu-enabled? mesos-gpu-enabled
                                   :make-mesos-driver-fn make-mesos-driver-fn
                                   :mea-culpa-failure-limit mea-culpa-failure-limit
                                   :mesos-datomic-conn datomic/conn
                                   :mesos-datomic-mult mesos-datomic-mult
                                   :mesos-leadership-atom mesos-leadership-atom
                                   :mesos-pending-jobs-atom mesos-pending-jobs-atom
                                   :mesos-run-as-user mesos-run-as-user
                                   :offer-cache mesos-offer-cache
                                   :offer-incubate-time-ms offer-incubate-time-ms
                                   :optimizer-config optimizer
                                   :progress-config progress
                                   :rebalancer-config rebalancer
                                   :sandbox-syncer-state sandbox-syncer-state
                                   :server-config {:hostname hostname
                                                   :server-port server-port}
                                   :task-constraints task-constraints
                                   :trigger-chans trigger-chans
                                   :zk-prefix mesos-leader-path})
                                (catch ClassNotFoundException e
                                  (log/warn e "Not loading mesos support...")
                                  nil))))
                          (throw (ex-info "This node does not have a curator configured" {})))))})

(defn health-check-middleware
  "This adds /debug to return 200 OK"
  [h mesos-leadership-atom leader-reports-unhealthy]
  (fn healthcheck [req]
    (if (and (= (:uri req) "/debug")
             (= (:request-method req) :get))
      {:status (if (and leader-reports-unhealthy @mesos-leadership-atom)
                 503
                 200)
       :headers {}
       :body (str "Server is running version: " @util/version " (commit " @util/commit ")")}
      (h req))))

(def curator-framework
  (fnk [[:settings zookeeper]]
    (when zookeeper
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
  "Our auth in cook.spnego doesn't hook in to Jetty - this handler
  does so to make sure it's available for Jetty to log"
  (fn [req]
    (do
      (.setAuthentication (:servlet-request req)
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

(defn framework-id-from-zk
  "Returns the framework id from ZooKeeper, or nil if not present"
  [curator-framework mesos-leader-path]
  (when-let [bytes (curator/get-or-nil curator-framework (str mesos-leader-path "/framework-id"))]
    (let [framework-id (String. bytes)]
      (log/info "Found framework id in zookeeper:" framework-id)
      framework-id)))

(defn conditional-auth-bypass
  "Skip authentication on some hard-coded endpoints."
  [h auth-middleware]
  (let [auth-fn (auth-middleware h)]
    (fn filtered-auth [{:keys [uri request-method] :as req}]
      (if (and (= "/info" uri) (= :get request-method))
        (h req)
        (auth-fn req)))))

(def scheduler-server
  (graph/eager-compile
    {:route full-routes
     :http-server (fnk [[:settings cors-origins server-port authorization-middleware impersonation-middleware
                         leader-reports-unhealthy server-https-port server-keystore-path server-keystore-type
                         server-keystore-pass [:rate-limit user-limit]]
                        [:route view] mesos-leadership-atom]
                    (log/info "Launching http server")
                    (let [rate-limit-storage (storage/local-storage)
                          jetty ((util/lazy-load-var 'qbits.jet.server/run-jetty)
                                  (cond-> {:ring-handler (routes
                                                           (route/resources "/resource")
                                                           (-> view
                                                               tell-jetty-about-usename
                                                               (wrap-rate-limit {:storage rate-limit-storage
                                                                                 :limit user-limit})
                                                               impersonation-middleware
                                                               (conditional-auth-bypass authorization-middleware)
                                                               wrap-stacktrace
                                                               wrap-no-cache
                                                               wrap-cookies
                                                               wrap-params
                                                               (cors/cors-middleware cors-origins)
                                                               (health-check-middleware mesos-leadership-atom leader-reports-unhealthy)
                                                               instrument))
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
     ; If the framework id was not found in the configuration settings, we attempt reading it from
     ; ZooKeeper. The read from ZK is present for backwards compatibility (the framework id used to
     ; get written to ZK as well). Without this, Cook would connect to mesos with a different
     ; framework id and mark all jobs that were running failed because mesos wouldn't have tasks for
     ; those jobs under the new framework id.
     :framework-id (fnk [curator-framework [:settings mesos-leader-path mesos-framework-id]]
                     (when curator-framework
                       (let [framework-id (or mesos-framework-id
                                              (framework-id-from-zk curator-framework mesos-leader-path))]
                         (when-not framework-id
                           (throw (ex-info "Framework id not configured and not in ZooKeeper" {})))
                         (log/info "Using framework id:" framework-id)
                         framework-id)))
     :mesos-datomic-mult (fnk []
                           (first ((util/lazy-load-var 'cook.datomic/create-tx-report-mult) datomic/conn)))
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
     :sandbox-syncer-state (fnk [[:settings [:sandbox-syncer max-consecutive-sync-failure
                                             publish-batch-size publish-interval-ms sync-interval-ms]]
                                 framework-id mesos-agent-query-cache]
                             (when framework-id
                               ((util/lazy-load-var 'cook.mesos.sandbox/prepare-sandbox-publisher)
                                 framework-id datomic/conn publish-batch-size publish-interval-ms sync-interval-ms
                                 max-consecutive-sync-failure mesos-agent-query-cache)))
     :mesos-leadership-atom (fnk [] (atom false))
     :mesos-pending-jobs-atom (fnk [] (atom {}))
     :mesos-offer-cache (fnk [[:settings {offer-cache nil}]]
                          (when offer-cache
                            (-> {}
                                (cache/lru-cache-factory :threshold (:max-size offer-cache))
                                (cache/ttl-cache-factory :ttl (:ttl-ms offer-cache))
                                atom)))
     :curator-framework curator-framework}))

(defn -main
  "Entry point for Cook. Initializes configuration settings,
  instruments the JVM, and starts up the scheduler and API."
  [config-file-path & _]
  (println "Cook" @util/version "( commit" @util/commit ")")
  (try
    (mount/start-with-args (cook.config/read-config config-file-path))
    (pool/guard-invalid-default-pool (d/db datomic/conn))
    (metrics-jvm/instrument-jvm)
    (let [server (scheduler-server config)]
      (intern 'user 'main-graph server)
      (log/info "Started Cook, stored variable in user/main-graph"))
    (catch Throwable t
      (log/error t "Failed to start Cook")
      (System/exit 1))))
