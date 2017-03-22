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
  (:require [clj-logging-config.log4j :as log4j-conf]
            [clj-pid.core :as pid]
            [clj-time.core :as t]
            [clojure.java.io :as io]
            [clojure.pprint :refer (pprint)]
            [clojure.tools.logging :as log]
            [compojure.core :refer (GET POST routes context)]
            [compojure.route :as route]
            [congestion.limits :refer (RateLimit)]
            [congestion.middleware :refer (wrap-rate-limit ip-rate-limit)]
            [congestion.storage :as storage]
            [cook.curator :as curator]
            [cook.spnego :as spnego]
            [metrics.ring.instrument :refer (instrument)]
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
  {:scheduler (fnk [mesos-datomic framework-id mesos-pending-jobs-atom [:settings task-constraints mesos-gpu-enabled is-authorized-fn]]
                   ((lazy-load-var 'cook.mesos.api/main-handler)
                    mesos-datomic
                    framework-id
                    task-constraints
                    mesos-gpu-enabled
                    (fn [] @mesos-pending-jobs-atom)
                    is-authorized-fn))
   :view (fnk [scheduler]
              scheduler)})

(def full-routes
  {:raw-scheduler raw-scheduler-routes
   :view (fnk [raw-scheduler]
              (routes (:view raw-scheduler)
                      (route/not-found "<h1>Not a valid route</h1>")))})

(def mesos-scheduler
  {:mesos-scheduler (fnk [[:settings mesos-master mesos-master-hosts mesos-leader-path mesos-failover-timeout mesos-principal mesos-role mesos-framework-name offer-incubate-time-ms mea-culpa-failure-limit fenzo-max-jobs-considered fenzo-scaleback fenzo-floor-iterations-before-warn fenzo-floor-iterations-before-reset task-constraints riemann mesos-gpu-enabled rebalancer good-enough-fitness] mesos-datomic mesos-datomic-mult curator-framework mesos-pending-jobs-atom]
                         (try
                           (Class/forName "org.apache.mesos.Scheduler")
                           ((lazy-load-var 'cook.mesos/start-mesos-scheduler)
                            mesos-master
                            mesos-master-hosts
                            curator-framework
                            mesos-datomic
                            mesos-datomic-mult
                            mesos-leader-path
                            mesos-failover-timeout
                            mesos-principal
                            mesos-role
                            mesos-framework-name
                            offer-incubate-time-ms
                            mea-culpa-failure-limit
                            task-constraints
                            (:host riemann)
                            (:port riemann)
                            mesos-pending-jobs-atom
                            mesos-gpu-enabled
                            rebalancer
                            {:fenzo-max-jobs-considered fenzo-max-jobs-considered
                             :fenzo-scaleback fenzo-scaleback
                             :fenzo-floor-iterations-before-warn fenzo-floor-iterations-before-warn
                             :fenzo-floor-iterations-before-reset fenzo-floor-iterations-before-reset
                             :good-enough-fitness good-enough-fitness})
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
                                (getName [this]
                                  (:authorization/user req))
                                (toString [this]
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

(defrecord UserRateLimit [id quota ttl]
  RateLimit
  (get-key [self req]
    (str (.getName (type self)) id "-" (:authorization/user req)))
  (get-quota [self req]
    quota)
  (get-ttl [self req]
    ttl))

(def scheduler-server
  (graph/eager-compile
    {:mesos-datomic mesos-datomic
     :route full-routes
     :http-server (fnk [[:settings server-port authorization-middleware [:rate-limit user-limit]] [:route view]]
                       (log/info "Launching http server")
                       (let [rate-limit-storage (storage/local-storage)
                             jetty ((lazy-load-var 'qbits.jet.server/run-jetty)
                                    {:port server-port
                                     :ring-handler (-> view
                                                       tell-jetty-about-usename
                                                       (wrap-rate-limit {:storage rate-limit-storage
                                                                         :limit user-limit})
                                                       authorization-middleware
                                                       wrap-stacktrace
                                                       wrap-no-cache
                                                       wrap-cookies
                                                       wrap-params
                                                       health-check-middleware
                                                       instrument)
                                     :join? false
                                     :configurator configure-jet-logging
                                     :max-threads 200
                                     :request-header-size 32768})]
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
     :mesos-pending-jobs-atom (fnk [] (atom {}))
     :curator-framework curator-framework}))

(def simple-dns-name
  (fnk [] (str (System/getProperty "user.name")
               "."
               (.getHostName (java.net.InetAddress/getLocalHost)))))

#_(def dev-mem-settings
  {:server-port (fnk [] 12321)
   :authorization-middleware (fnk [] (fn [h] (fn [req] (h (assoc req :authorization/user (System/getProperty "user.name"))))))
   :sim-agent-path (fnk [] "/usr/bin/sim-agent") ;;TODO parameterize
   :mesos-datomic-uri (fnk [] "datomic:mem://mesos-jobs")
   :dns-name simple-dns-name
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
                           :cpus 6
                           :retry-limit 5})
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

(def default-authorization {:authorization-fn 'cook.authorization/open-auth})



(def config-settings
  "Parses the settings out of a config file"
  (graph/eager-compile
    {:server-port (fnk [[:config port]]
                       port)
     :is-authorized-fn (fnk [[:config {authorization-config default-authorization}]]
                                (partial (lazy-load-var 'cook.authorization/is-authorized?)
                                         authorization-config))
     :authorization-middleware (fnk [[:config [:authorization {one-user false} {kerberos false} {http-basic false}]]]
                                    (cond
                                      http-basic 
                                      (let [validation (get http-basic :validation :none)
                                            user-password-valid? 
                                            ((lazy-load-var 'cook.basic-auth/make-user-password-valid?) validation http-basic)]
                                        (log/info "Using http basic authorization with validation" validation)
                                        ((lazy-load-var 'cook.basic-auth/create-http-basic-middleware) user-password-valid?))

                                      one-user 
                                      (do
                                        (log/info "Using single user authorization")
                                        (fn one-user-middleware [h]
                                          (fn one-user-auth-wrapper [req]
                                            (h (assoc req :authorization/user one-user)))))

                                      kerberos 
                                      (do
                                        (log/info "Using kerberos middleware")
                                        (lazy-load-var 'cook.spnego/require-gss))
                                      :else (throw (ex-info "Missing authorization configuration" {}))))
     :rate-limit (fnk [[:config {rate-limit nil}]]
                      (let [{:keys [user-limit-per-m]
                             :or {user-limit-per-m 600}} rate-limit]
                        {:user-limit (->UserRateLimit :user-limit user-limit-per-m (t/minutes 1))}))
     :sim-agent-path (fnk [] "/usr/bin/sim-agent")
     :mesos-datomic-uri (fnk [[:config [:database datomic-uri]]]
                             (when-not datomic-uri
                               (throw (ex-info "Must set a the :database's :datomic-uri!" {})))
                             datomic-uri)
     :dns-name simple-dns-name
     :hostname (fnk [] (.getCanonicalHostName (java.net.InetAddress/getLocalHost)))
     :local-zk-port (fnk [[:config [:zookeeper {local-port 3291}]]]
                         local-port)
     :zookeeper-server (fnk [[:config [:zookeeper {local? false}]] local-zk-port]
                            (when local?
                              (log/info "Created local ZooKeeper; not yet started")
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
                               :retry-limit 20
                               :memory-gb 12
                               :cpus 4}
                              task-constraints))
     :offer-incubate-time-ms (fnk [[:config [:scheduler {offer-incubate-ms 15000}]]]
                                  offer-incubate-ms)
     :mea-culpa-failure-limit (fnk [[:config [:scheduler {mea-culpa-failure-limit nil}]]]
                                   mea-culpa-failure-limit)
     :fenzo-max-jobs-considered (fnk [[:config [:scheduler {fenzo-max-jobs-considered 1000}]]]
                                     fenzo-max-jobs-considered)
     :fenzo-scaleback (fnk [[:config [:scheduler {fenzo-scaleback 0.95}]]]
                           fenzo-scaleback)
     :fenzo-floor-iterations-before-warn (fnk [[:config [:scheduler {fenzo-floor-iterations-before-warn 10}]]]
                                              fenzo-floor-iterations-before-warn)
     :fenzo-floor-iterations-before-reset (fnk [[:config [:scheduler {fenzo-floor-iterations-before-reset 1000}]]]
                                               fenzo-floor-iterations-before-reset)
     :mesos-gpu-enabled (fnk [[:config [:mesos {enable-gpu-support false}]]]
                             (boolean enable-gpu-support))
     :good-enough-fitness (fnk [[:config [:scheduler {good-enough-fitness 0.8}]]]
                               good-enough-fitness)
     :mesos-master (fnk [[:config [:mesos master]]]
                        master)
     :mesos-master-hosts (fnk [[:config [:mesos master {master-hosts nil}]]]
                              (if master-hosts
                                (if (and (sequential? master-hosts) (every? string? master-hosts))
                                  master-hosts
                                  (throw (ex-info ":mesos-master should be a list of hostnames (e.g. [\"host1.example.com\", ...])" {})))
                                (->> master
                                     (re-seq #"[/|,]?([^/,:]+):\d+")
                                     (mapv second))))
     :mesos-failover-timeout (fnk [[:config [:mesos {failover-timeout-ms nil}]]]
                                  failover-timeout-ms)
     :mesos-leader-path (fnk [[:config [:mesos leader-path]]]
                             leader-path)
     :mesos-principal (fnk [[:config [:mesos {principal nil}]]]
                           principal)
     :mesos-role (fnk [[:config [:mesos {role "*"}]]]
                           role)
     :mesos-framework-name (fnk [[:config [:mesos {framework-name "Cook"}]]]
                                framework-name)
     ;:riemann-metrics (fnk [[:config [:metrics {riemann nil}]]]
     ;                  (when riemann
     ;                    (when-not (= 4 (count (select-keys riemann [:host :port])))
     ;                      (throw (ex-info "You must specify the riemann :host and :port!" riemann)))
     ;                    ((lazy-load-var 'cook.reporter/riemann-reporter) riemann)))
     :jmx-metrics (fnk [[:config [:metrics {jmx false}]]]
                       (when jmx
                         ((lazy-load-var 'cook.reporter/jmx-reporter))))
     :graphite-metrics (fnk [[:config [:metrics {graphite nil}]]]
                            (when graphite
                              (when-not (:host graphite)
                                (throw (ex-info "You must specify the graphite host!" {:graphite graphite})))
                              (let [config (merge {:port 2003 :pickled? true} graphite)]
                                ((lazy-load-var 'cook.reporter/graphite-reporter) config))))
     :riemann (fnk [[:config [:metrics {riemann nil}]]]
                   riemann)
     :riemann-metrics (fnk [[:config [:metrics {riemann nil}]]]
                           (when riemann
                             (when-not (:host riemann)
                               (throw (ex-info "You must specific the :host to send the riemann metrics to!" {:riemann riemann})))
                             (when-not (every? string? (:tags riemann))
                               (throw (ex-info "Riemann tags must be a [\"list\", \"of\", \"strings\"]" riemann)))
                             (let [config (merge {:port 5555
                                                  :local-host (.getHostName
                                                                (java.net.InetAddress/getLocalHost))}
                                                 riemann)]
                               ((lazy-load-var 'cook.reporter/riemann-reporter) config))))

     :rebalancer (fnk [[:config {rebalancer nil}]]
                      (merge {:interval-seconds 300
                              :dru-scale 1.0}
                             rebalancer))

     :nrepl-server (fnk [[:config [:nrepl {enabled? false} {port 0}]]]
                        (when enabled?
                          (when (zero? port)
                            (throw (ex-info "You enabled nrepl but didn't configure a port. Please configure a port in your config file." {})))
                          ((lazy-load-var 'clojure.tools.nrepl.server/start-server) :port port)))}))

(defn- init-logger
  ([] (init-logger {:levels {"datomic.db" :warn
                             "datomic.peer" :warn
                             "datomic.kv-cluster" :warn
                             "com.netflix.fenzo.AssignableVMs" :warn
                             "com.netflix.fenzo.TaskScheduler" :warn
                             "com.netflix.fenzo.AssignableVirtualMachine" :warn}}))
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
    {:exception-handler (fnk [[:config [:unhandled-exceptions {log-level :error} {email nil}]] logging]
                             ((lazy-load-var 'cook.util/install-email-on-exception-handler) log-level email))
     :logging (fnk [[:config log]]
                   (init-logger log))}))

(defn -main
  [config & args]
  (when-not (.exists (java.io.File. config))
    (.println System/err (str "The config file doesn't appear to exist: " config)))
  (.println System/err (str "Reading config from file:" config))
  (try
    (let [config-format (try
                          (com.google.common.io.Files/getFileExtension config)
                          (catch Throwable t
                            (.println System/err (str "Failed to start Cook" t))
                            (System/exit 1)))
          literal-config (try
                           {:config
                            (case config-format
                              "edn" (read-string (slurp config))
                              (do
                                (.println System/err (str "Invalid config file format " config-format))
                                (System/exit 1)))}
                           (catch Throwable t
                             (.println System/err (str "Failed to start Cook" t))
                             (System/exit 1)))]
      (pre-configuration literal-config)
      (.println System/err "Configured logging")
      (log/info "Configured logging")
      (let [settings {:settings (config-settings literal-config)}
            _ (log/info "Interpreted settings")
            server (scheduler-server settings)]
        (intern 'user 'main-graph server)
        (log/info "Started cook, stored variable in user/main-graph")))
    (catch Throwable t
      (log/error t "Failed to start Cook")
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
                           {:level :warn}))
