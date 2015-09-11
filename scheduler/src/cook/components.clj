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
(ns cook.components
  (:require [plumbing.core :refer (fnk)]
            [ring.middleware.params :refer (wrap-params)]
            [clj-http-async-pool.core]
            [clj-http-async-pool.circuit-breaker :as circuit-breaker]
            [clj-http-async-pool.pool :as http-pool]
            [clj-http-async-pool.router :as http-pool-router]
            [clj-pid.core :as pid]
            [clojure.java.io :as io]
            [clj-logging-config.log4j :as log4j-conf]
            [clojure.tools.logging :as log]
            [clojure.pprint :refer (pprint)]
            [cook.spnego :as spnego]
            [ring.middleware.stacktrace :refer (wrap-stacktrace-web)]
            [ring.util.response :refer (response)]
            [ring.util.mime-type]
            [metrics.ring.instrument :refer (instrument)]
            [compojure.core :refer (GET POST routes context)]
            [compojure.route :as route]
            [plumbing.graph :as graph]
            [cook.curator :as curator])
  (:import org.apache.curator.retry.BoundedExponentialBackoffRetry
           org.apache.curator.framework.state.ConnectionStateListener
           org.apache.curator.framework.CuratorFrameworkFactory)
  (:gen-class))

(defn wrap-no-cache
  [handler]
  (fn [req]
    (let [resp (handler req)]
      (assoc-in resp
                [:headers "Cache-control"]
                "max-age=0"))))

(defn lazy-load-var
  "Takes a symbol name of a var, requires the ns if not yet required, and
   returns the var."
  [var-sym]
  (let [ns (namespace var-sym)]
    (when-not ns
      (throw (ex-info "Can only load vars that are ns-qualified!" {})))
    (require (symbol ns))
    (resolve var-sym)))

(def raw-scheduler-routes
  {:scheduler (fnk [mesos-datomic framework-id]
                   ((lazy-load-var 'cook.mesos.api/handler)
                    mesos-datomic
                    framework-id))
   :view (fnk [scheduler]
              scheduler)})

(def full-routes
  {:raw-scheduler raw-scheduler-routes
   :view (fnk [raw-scheduler]
              (routes (:view raw-scheduler)
                      (route/not-found "<h1>Not a valid route</h1>")))})

(def mesos-scheduler
  {:mesos-scheduler (fnk [[:settings mesos-master mesos-leader-path mesos-failover-timeout mesos-principal mesos-role offer-incubate-time-ms task-constraints] mesos-datomic mesos-datomic-mult curator-framework]
                         (try
                           (Class/forName "org.apache.mesos.Scheduler")
                           ((lazy-load-var 'cook.mesos/start-mesos-scheduler)
                            mesos-master
                            curator-framework
                            mesos-datomic
                            mesos-datomic-mult
                            mesos-leader-path
                            mesos-failover-timeout
                            mesos-principal
                            mesos-role
                            offer-incubate-time-ms
                            task-constraints)
                           (catch ClassNotFoundException e
                             (log/warn e "Not loading mesos support...")
                             nil)))})

(defn health-check-middleware
  "This adds /debug to return 200 OK"
  [h]
  (fn healthcheck [req]
    (if (and (= (:uri req) "/debug")
             (= (:request-method req) :get))
      {:status 200
       :headers {}
       :body (str "Server is running: "
                  (try (slurp (io/resource "git-log"))
                       (catch Exception e
                         "dev")))}
      (h req))))

(def mesos-datomic
  (fnk [[:settings mesos-datomic-uri]]
       ((lazy-load-var 'datomic.api/create-database) mesos-datomic-uri)
       (let [conn ((lazy-load-var 'datomic.api/connect) mesos-datomic-uri)]
         (doseq [txn (deref (lazy-load-var 'cook.mesos.schema/work-item-schema))]
           (deref ((lazy-load-var 'datomic.api/transact) conn txn))
           ((lazy-load-var 'metatransaction.core/install-metatransaction-support) conn)
           ((lazy-load-var 'metatransaction.utils/install-utils-support) conn))
         conn)))

(def curator-framework
  (fnk [[:settings zookeeper] local-zookeeper]
       (let [retry-policy (BoundedExponentialBackoffRetry. 100 120000 10)
             ;; The 180s session and 30s connection timeouts were pulled from a google group
             ;; recommendation
             curator-framework (CuratorFrameworkFactory/newClient zookeeper 180000 30000 retry-policy)
             ]
         (.. curator-framework
             getConnectionStateListenable
             (addListener (reify ConnectionStateListener
                            (stateChanged [_ client newState]
                              (log/info "Curator state changed:"
                                        (str newState))))))
         (.start curator-framework)
         curator-framework)))

(def jet-runner
  "This is the jet server constructor. It could be cool."
  (fnk [[:settings server-port]]
       (fn [handler]
         (let [jetty ((lazy-load-var 'qbits.jet.server/run-jetty)
                      {:port server-port
                       :ring-handler handler
                       :join? false
                       :max-threads 200})]
           (fn [] (.stop jetty))))))

(def scheduler-server
  (graph/eager-compile
    {:mesos-datomic mesos-datomic

     :route full-routes
     :http-server (fnk [[:settings server-port authorization-middleware] [:route view]]
                       (log/info "Launching http server")
                       (let [jetty ((lazy-load-var 'qbits.jet.server/run-jetty)
                                    {:port server-port
                                     :ring-handler (-> view
                                                       authorization-middleware
                                                       wrap-stacktrace-web
                                                       wrap-no-cache
                                                       wrap-params
                                                       health-check-middleware
                                                       instrument)
                                     :join? false
                                     :max-threads 200})]
                         (fn [] (.stop jetty))))

     :framework-id (fnk [curator-framework [:settings mesos-leader-path]]
                        (when-let [bytes (curator/get-or-nil curator-framework
                                                             (str mesos-leader-path "/framework-id"))]
                          (String. bytes)))
     :mesos-datomic-mult (fnk [mesos-datomic]
                              (first ((lazy-load-var 'cook.datomic/create-tx-report-mult) mesos-datomic)))
     :local-zookeeper (fnk [[:settings zookeeper-server]]
                           (when zookeeper-server
                             (log/info "Starting local ZK server")
                             (.start zookeeper-server)))
     :mesos mesos-scheduler
     :metric-riemann-reporter (fnk []
                                   ((lazy-load-var 'cook.reporter/metric-riemann-reporter) :service-prefix "cook scheduler"))
     :curator-framework curator-framework}))

(def simple-dns-name
  (fnk [] (str (System/getProperty "user.name")
               "."
               (.getHostName (java.net.InetAddress/getLocalHost)))))

#_(def dev-mem-settings
  {:server-port (fnk [] 12321)
   :authorization-middleware (fnk [] (fn [h] (fn [req] (h (assoc req :authorization/user (System/getProperty "user.name"))))))
   ;:authorization-middleware (fnk [federation-privileged-principal]
   ;                               (partial (lazy-load-var 'cook.spnego/require-gss) federation-privileged-principal))
   :sim-agent-path (fnk [] "/usr/bin/sim-agent") ;;TODO parameterize
   :mesos-datomic-uri (fnk [] "datomic:mem://mesos-jobs")
   :dns-name simple-dns-name
   :federation-remotes (fnk [] [])
   :federation-privileged-principal (fnk [] nil)
   :federation-threads (fnk [] 4)
   :federation-circuit-breaker-opts (fnk []
                                         {:failure-threshold 0
                                          :lifetime-ms (* 1000 60)
                                          :response-timeout-ms (* 1000 60)
                                          :reset-timeout-ms (* 1000 60)
                                          :failure-logger-size 10000})
   ;:federation-circuit-breaker-opts (fnk []
   ;                                      {:failure-threshold 0
   ;                                       :lifetime-ms (* 1000 60 10)
   ;                                       :response-timeout-ms (* 1000 60 5)
   ;                                       :reset-timeout-ms (* 1000 60)
   ;                                       :failure-logger-size 10000})
   :zookeeper (fnk []
                   "localhost:3291")
   :zookeeper-server (fnk []
                          (org.apache.curator.test.TestingServer. 3291 true) ;; Start a local ZK for simplicity
                          )
   :hostname (fnk [] (.getCanonicalHostName (java.net.InetAddress/getLocalHost)))
   :offer-incubate-time-ms (fnk [] (* 15 1000))
   :task-constraints (fnk []
                          {:timeout-hours 1
                           :timeout-interval-minutes 1
                           :memory-gb 48
                           :cpus 6})
   ;:task-constraints (fnk []
   ;                       {:timeout-hours (* 5 24)
   ;                        :timeout-interval-minutes 10
   ;                        :memory-gb 128
   ;                        :cpus 24})
   :mesos-master (fnk [] "zk://localhost:2181/mesos")
   :mesos-failover-timeout (fnk [] nil) ; don't fail over
   ;:mesos-failover-timeout (fnk [] (* 1.0
   ;                                   3600  ; seconds/hour
   ;                                   24    ; hours/day
   ;                                   7     ; days/week
   ;                                   2     ; weeks
   ;                                   ))
   :mesos-leader-path (fnk [] (str "/cook-scheduler"))
   :mesos-principal (fnk [] nil) ;; You can change this to customize for your environment
   :exception-handler (fnk [] ((lazy-load-var 'cook.util/install-email-on-exception-handler))) ;;TODO parameterize
   })

(def config-settings
  "Parses the settings out of a config file"
  (graph/eager-compile
    {:server-port (fnk [[:config port]]
                       port)
     :authorization-middleware (fnk [[:config [:authorization {one-user false} {kerberos false}]] federation-privileged-principal]
                                    (cond
                                      one-user (do
                                                 (log/info "Using single user authorization")
                                                 (fn one-user-middleware [h]
                                                   (fn one-user-auth-wrapper [req]
                                                     (h (assoc req :authorization/user one-user)))))
                                      kerberos (do
                                                 (log/info "Using kerberos middleware")
                                                 (partial (lazy-load-var 'cook.spnego/require-gss) federation-privileged-principal))
                                      :else (throw (ex-info "Missing authorization configuration" {}))))
     :sim-agent-path (fnk [] "/usr/bin/sim-agent")
     :mesos-datomic-uri (fnk [[:config [:database datomic-uri]]]
                             (when-not datomic-uri
                               (throw (ex-info "Must set a the :database's :datomic-uri!" {})))
                             datomic-uri)
     :dns-name simple-dns-name
     :hostname (fnk [] (.getCanonicalHostName (java.net.InetAddress/getLocalHost)))
     :federation-remotes (fnk [] [])
     :federation-privileged-principal (fnk [] nil)
     :federation-threads (fnk [] 4)
     :federation-circuit-breaker-opts (fnk []
                                           {:failure-threshold 0
                                            :lifetime-ms (* 1000 60)
                                            :response-timeout-ms (* 1000 60)
                                            :reset-timeout-ms (* 1000 60)
                                            :failure-logger-size 10000})
     :local-zk-port (fnk [[:config [:zookeeper {local-port 3291}]]]
                         local-port)
     :zookeeper-server (fnk [[:config [:zookeeper {local? false}]] local-zk-port]
                            (when local?
                              (log/info "Created local testing server; not yet started")
                              (org.apache.curator.test.TestingServer. local-zk-port false)))
     :zookeeper (fnk [[:config [:zookeeper {local? false} {connection nil}]] local-zk-port]
                     (cond
                       local? (str "localhost:" local-zk-port)
                       connection connection
                       :else (throw (ex-info "Must specify a zookeeper connection" {}))))
     :task-constraints (fnk [[:config [:scheduler {task-constraints nil}]]]
                            ;; Trying to pick conservative defaults
                            (merge
                              {:timeout-hours 1
                               :timeout-interval-minutes 1
                               :memory-gb 12
                               :cpus 4}
                              task-constraints))
     :offer-incubate-time-ms (fnk [[:config [:scheduler {offer-incubate-ms 15000}]]]
                                  offer-incubate-ms)
     :mesos-master (fnk [[:config [:mesos master]]]
                        master)
     :mesos-failover-timeout (fnk [[:config [:mesos {failover-timeout-ms nil}]]]
                                  failover-timeout-ms)
     :mesos-leader-path (fnk [[:config [:mesos leader-path]]]
                             leader-path)
     :mesos-principal (fnk [[:config [:mesos {principal nil}]]]
                           principal)
     :mesos-role (fnk [[:config [:mesos {role "*"}]]]
                           role)
     :nrepl-server (fnk [[:config [:nrepl {enabled? false} {port 0}]]]
                        (when enabled?
                          (when (zero? port)
                            (throw (ex-info "You enabled nrepl but didn't configure a port. Please configure a port in your config file." {})))
                          ((lazy-load-var 'clojure.tools.nrepl.server/start-server) :port port)))}))

(defn- init-logger
  ([] (init-logger {:levels {"datomic.db" :warn
                             "datomic.peer" :warn
                             "datomic.kv-cluster" :warn}}))
  ([{:keys [file] :or {file "log/cook.log"} {:keys [default] :or {default :info} :as overrides} :levels}]
   (try
     (.. (org.apache.log4j.Logger/getRootLogger)
       (getLoggerRepository)
       (resetConfiguration))
   ;; This lets you inspect which loggers have which appenders turned on
   ;;(clojure.pprint/pprint (map #(select-keys % [:allAppenders :name]) (log4j-conf/get-loggers)))
   (let [overrides (->> overrides
                        (filter (comp string? key))
                        (mapcat (fn [[logger level]]
                                  [[logger] {:level level}])))]
     (apply log4j-conf/set-loggers!
            (org.apache.log4j.Logger/getRootLogger)
            {:out (org.apache.log4j.DailyRollingFileAppender.
                    (org.apache.log4j.PatternLayout.
                      "%d{ISO8601} %-5p %c [%t] - %m%n")
                    file
                    "'.'yyyy-MM-dd")
             :level default}
            overrides))
     (catch Throwable t
       (.println System/err "Failed to initialize logging!")
       (.printCauseTrace t)
       (System/exit 1)))))

(def pre-configuration
  "This configures logging and exception handling, to make the configuration phase simpler to understand"
  (graph/eager-compile
    {:exception-handler (fnk [[:config [:unhandled-exceptions email]] logging]
                             ((lazy-load-var 'cook.util/install-email-on-exception-handler) email))
     :logging (fnk [[:config log]]
                   (init-logger log))}))

(defn -main
  [config & args]
  (when-not (.exists (java.io.File. config))
    (.println System/err (str "The config file doesn't appear to exist: " config)))
  (println "Reading config from file:" config)
  (try
    (let [config-format (com.google.common.io.Files/getFileExtension config)
        literal-config {:config
                        (case config-format
                          "edn" (read-string (slurp config))
                          (do
                            (.println System/err (str "Invalid config file format " config-format))
                            (System/exit 1)))}
        base-init (pre-configuration literal-config)
        _ (println "Configured logging")
        _ (log/info "Configured logging")
        settings {:settings (config-settings literal-config)}
        _ (log/info "Interpreted settings")
        server (scheduler-server settings)]
    (intern 'user 'main-graph server)
    (log/info "Started cook, stored variable in user/main-graph"))
    (println "Started cook, stored variable in user/main-graph") 
    (catch Throwable t
      (log/error t "Failed to start Cook")
      (println "Failed to start Cook")
      (System/exit 1))))

(comment
  ;; Here are some helpful fragments for changing debug levels, especiall with datomic
  (require 'datomic.api)
  (log4j-conf/set-logger! :level :debug)

  (do
    (log4j-conf/set-loggers! (org.apache.log4j.Logger/getRootLogger)
                             {:level :info :out (org.apache.log4j.FileAppender.
                                                 (org.apache.log4j.PatternLayout.
                                                  "%d{ISO8601} %-5p %c [%t] - %m%n")
                                                 "debug.log")}
                             ["datomic.peer"]
                             {:level :warn})
    (log/info "confirm we're online"))

  (log4j-conf/set-loggers! (org.apache.log4j.Logger/getRootLogger)
                           {:level :info :out (org.apache.log4j.ConsoleAppender.
                                               (org.apache.log4j.PatternLayout.
                                                "%d{ISO8601} %-5p %c [%t] - %m%n"))}
                           ["datomic.peer"]
                           {:level :warn})
  )
