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
(ns cook.config
  (:require [clj-logging-config.log4j :as log4j-conf]
            [clj-time.core :as t]
            [clojure.edn :as edn]
            [clojure.stacktrace :as stacktrace]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [congestion.limits :refer (RateLimit)]
            [cook.impersonation :refer (impersonation-authorized-wrapper)]
            [cook.util :as util]
            [mount.core :as mount]
            [plumbing.core :refer (fnk)]
            [plumbing.graph :as graph])
  (:import (com.google.common.io Files)
           (java.io File)
           (java.net InetAddress)
           (org.apache.curator.test TestingServer)
           (org.apache.log4j DailyRollingFileAppender Logger PatternLayout)))

(defn env
  ^String
  [name]
  (System/getenv name))

(defn read-edn-config
  [config]
  (edn/read-string
    {:readers
     {'config/env #(env %)
      'config/env-int-default (fn [[variable default]]
                                (if-let [value (env variable)]
                                  (Integer/parseInt value)
                                  default))
      'config/env-int #(Integer/parseInt (env %))
      'config/env-bool #(Boolean/valueOf (env %))}}
    config))

(defn init-logger
  ([] (init-logger {:levels {"datomic.db" :warn
                             "datomic.peer" :warn
                             "datomic.kv-cluster" :warn
                             "com.netflix.fenzo.AssignableVMs" :warn
                             "com.netflix.fenzo.TaskScheduler" :warn
                             "com.netflix.fenzo.AssignableVirtualMachine" :warn}}))
  ([{:keys [file] :or {file "log/cook.log"} {:keys [default] :or {default :info} :as overrides} :levels}]
   (try
     (-> (Logger/getRootLogger) .getLoggerRepository .resetConfiguration)
     (let [overrides (->> overrides
                          (filter (comp string? key))
                          (mapcat (fn [[logger level]]
                                    [[logger] {:level level}])))]
       (apply log4j-conf/set-loggers!
              (Logger/getRootLogger)
              {:out (DailyRollingFileAppender.
                      (PatternLayout.
                        "%d{ISO8601} %-5p %c [%t] - %m%n")
                      file
                      "'.'yyyy-MM-dd")
               :level default}
              overrides))
     (catch Throwable t
       (.println System/err "Failed to initialize logging!")
       (stacktrace/print-cause-trace t)
       (throw t)))))

(def pre-configuration
  "This configures logging and exception handling, to make the configuration phase simpler to understand"
  (graph/eager-compile
    {:exception-handler (fnk [[:config [:unhandled-exceptions {log-level :error} {email nil}]]]
                          ((util/lazy-load-var 'cook.util/install-email-on-exception-handler) log-level email))
     :logging (fnk [[:config log]]
                (init-logger log))}))

(def default-authorization {:authorization-fn 'cook.authorization/open-auth})
(def default-fitness-calculator "com.netflix.fenzo.plugins.BinPackingFitnessCalculators/cpuMemBinPacker")

(defrecord UserRateLimit [id quota ttl]
  RateLimit
  (get-key [self req]
    (str (.getName (type self)) id "-" (:authorization/user req)))
  (get-quota [_ _]
    quota)
  (get-ttl [_ _]
    ttl))

(def config-settings
  "Parses the settings out of a config file"
  (graph/eager-compile
    {:agent-query-cache (fnk [[:config {agent-query-cache nil}]]
                          (merge
                            {:max-size 5000
                             :ttl-ms (* 60 1000)}
                            agent-query-cache))
     :cors-origins (fnk [[:config {cors-origins nil}]]
                     (map re-pattern (or cors-origins [])))
     :sandbox-syncer (fnk [[:config {sandbox-syncer nil}]]
                       (merge
                         {:max-consecutive-sync-failure 15
                          :publish-batch-size 100
                          :publish-interval-ms 2500
                          ;; The default should ideally be lower than the agent-query-cache ttl-ms
                          :sync-interval-ms 15000}
                         sandbox-syncer))
     :server-port (fnk [[:config {port nil} {ssl nil}]]
                    (when (and (nil? port) (nil? (get ssl :port)))
                      (throw (ex-info "Must provide either port or https-port" {})))
                    port)
     :server-https-port (fnk [[:config {ssl nil}]]
                          (get ssl :port))
     :server-keystore-path (fnk [[:config {ssl nil}]]
                             (get ssl :keystore-path))
     :server-keystore-type (fnk [[:config {ssl nil}]]
                             (get ssl :keystore-type))
     :server-keystore-pass (fnk [[:config {ssl nil}]]
                             (get ssl :keystore-pass))
     :is-authorized-fn (fnk [[:config {authorization-config default-authorization}]]
                         (let [auth-fn @(util/lazy-load-var 'cook.authorization/is-authorized?)]
                           ; we only wrap the authorization function if we have impersonators configured
                           (if (-> authorization-config :impersonators seq)
                             (impersonation-authorized-wrapper auth-fn authorization-config)
                             (partial auth-fn authorization-config))))
     :authorization-middleware (fnk [[:config [:authorization {one-user false} {kerberos false} {http-basic false}]]]
                                 (cond
                                   http-basic
                                   (let [validation (get http-basic :validation :none)
                                         user-password-valid?
                                         ((util/lazy-load-var 'cook.basic-auth/make-user-password-valid?) validation http-basic)]
                                     (log/info "Using http basic authorization with validation" validation)
                                     (with-meta
                                       ((util/lazy-load-var 'cook.basic-auth/create-http-basic-middleware) user-password-valid?)
                                       {:json-value "http-basic"}))

                                   one-user
                                   (do
                                     (log/info "Using single user authorization")
                                     (with-meta
                                       (fn one-user-middleware [h]
                                         (fn one-user-auth-wrapper [req]
                                           (h (assoc req :authorization/user one-user))))
                                       {:json-value "one-user"}))

                                   kerberos
                                   (do
                                     (log/info "Using kerberos middleware")
                                     (with-meta
                                       @(util/lazy-load-var 'cook.spnego/require-gss)
                                       {:json-value "kerberos"}))
                                   :else (throw (ex-info "Missing authorization configuration" {}))))
     :impersonation-middleware (fnk [[:config {authorization-config nil}]]
                                 (let [{impersonators :impersonators} authorization-config]
                                   (with-meta
                                     ((util/lazy-load-var 'cook.impersonation/create-impersonation-middleware) impersonators)
                                     {:json-value "config-impersonation"})))
     :rate-limit (fnk [[:config {rate-limit nil}]]
                   (let [{:keys [user-limit-per-m]
                          :or {user-limit-per-m 600}} rate-limit]
                     {:user-limit (->UserRateLimit :user-limit user-limit-per-m (t/minutes 1))}))
     :sim-agent-path (fnk [] "/usr/bin/sim-agent")
     :executor (fnk [[:config {executor {}}]]
                 (if (str/blank? (:command executor))
                   (do
                     (log/info "Executor config is missing command, will use the command executor by default"
                               {:executor-config executor})
                     {})
                   (do
                     (when-let [environment (:environment executor)]
                       (when-not (and (map? environment)
                                      (every? string? (keys environment))
                                      (every? string? (vals environment)))
                         (throw (ex-info "Executor environment must be a map from string to string!" {:executor executor}))))
                     (when (and (:portion executor) (not (<= 0 (:portion executor) 1)))
                       (throw (ex-info "Executor portion must be in the range [0, 1]!" {:executor executor})))
                     (let [default-executor-config {:default-progress-regex-string "progress:\\s+([0-9]*\\.?[0-9]+)($|\\s+.*)"
                                                    :environment {}
                                                    :log-level "INFO"
                                                    :max-message-length 512
                                                    :portion 0.0
                                                    :progress-sample-interval-ms (* 1000 60 5)
                                                    :retry-limit 5}
                           default-uri-config {:cache true
                                               :executable true
                                               :extract false}
                           executor-uri-configured (and (:uri executor)
                                                        (-> executor (get-in [:uri :value]) str/blank? not))]
                       (when-not executor-uri-configured
                         (log/info "Executor uri value is missing, the uri config will be disabled" {:executor executor}))
                       (let [base-executor-config (merge default-executor-config executor)]
                         (if executor-uri-configured
                           (update base-executor-config :uri #(merge default-uri-config %1))
                           (dissoc base-executor-config :uri)))))))
     :mesos-datomic-uri (fnk [[:config [:database datomic-uri]]]
                          (when-not datomic-uri
                            (throw (ex-info "Must set a the :database's :datomic-uri!" {})))
                          datomic-uri)
     :hostname (fnk [[:config {hostname (.getCanonicalHostName (InetAddress/getLocalHost))}]]
                 hostname)
     :leader-reports-unhealthy (fnk [[:config {mesos nil}]]
                                 (when mesos
                                   (or (:leader-reports-unhealthy mesos) false)))
     :local-zk-port (fnk [[:config {zookeeper nil}]]
                      (when zookeeper
                        (or (:local-port zookeeper) 3291)))
     :zookeeper-server (fnk [[:config {zookeeper nil}] local-zk-port]
                         (when (:local? zookeeper)
                           (log/info "Created local ZooKeeper; not yet started")
                           (TestingServer. ^Integer local-zk-port false)))
     :zookeeper (fnk [[:config {zookeeper nil}] local-zk-port]
                  (when zookeeper
                    (cond
                      (:local? zookeeper) (str "localhost:" local-zk-port)
                      (:connection zookeeper) (:connection zookeeper)
                      :else (throw (ex-info "Must specify a zookeeper connection" {})))))
     :task-constraints (fnk [[:config {scheduler nil}]]
                         ;; Trying to pick conservative defaults
                         (merge
                           {:timeout-hours 1
                            :timeout-interval-minutes 1
                            :retry-limit 20
                            :memory-gb 12
                            :cpus 4}
                           (:task-constraints scheduler)))
     :offer-incubate-time-ms (fnk [[:config {scheduler nil}]]
                               (when scheduler
                                 (or (:offer-incubate-ms scheduler) 15000)))
     :offer-cache (fnk [[:config {scheduler nil}]]
                    (when scheduler
                      (merge
                        {:max-size 2000
                         :ttl-ms 15000}
                        (:offer-cache scheduler))))
     :mea-culpa-failure-limit (fnk [[:config {scheduler nil}]]
                                (:mea-culpa-failure-limit scheduler))
     :fenzo-max-jobs-considered (fnk [[:config {scheduler nil}]]
                                  (when scheduler
                                    (or (:fenzo-max-jobs-considered scheduler) 1000)))
     :fenzo-scaleback (fnk [[:config {scheduler nil}]]
                        (when scheduler
                          (or (:fenzo-scaleback scheduler) 0.95)))
     :fenzo-floor-iterations-before-warn (fnk [[:config {scheduler nil}]]
                                           (when scheduler
                                             (or (:fenzo-floor-iterations-before-warn scheduler) 10)))
     :fenzo-floor-iterations-before-reset (fnk [[:config {scheduler nil}]]
                                            (when scheduler
                                              (or (:fenzo-floor-iterations-before-reset scheduler) 1000)))
     :fenzo-fitness-calculator (fnk [[:config {scheduler nil}]]
                                 (when scheduler
                                   (or (:fenzo-fitness-calculator scheduler) default-fitness-calculator)))
     :mesos-gpu-enabled (fnk [[:config {mesos nil}]]
                          (when mesos
                            (boolean (or (:enable-gpu-support mesos) false))))
     :good-enough-fitness (fnk [[:config {scheduler nil}]]
                            (when scheduler
                              (or (:good-enough-fitness scheduler) 0.8)))
     :mesos-master (fnk [[:config {mesos nil}]]
                     (when mesos
                       (:master mesos)))
     :mesos-master-hosts (fnk [[:config {mesos nil}]]
                           (when mesos
                             (if (:master-hosts mesos)
                               (if (and (sequential? (:master-hosts mesos)) (every? string? (:master-hosts mesos)))
                                 (:master-hosts mesos)
                                 (throw (ex-info ":mesos-master should be a list of hostnames (e.g. [\"host1.example.com\", ...])" {})))
                               (->> (:master mesos)
                                    (re-seq #"[/|,]?([^/,:]+):\d+")
                                    (mapv second)))))
     :mesos-failover-timeout (fnk [[:config {mesos nil}]]
                               (:failover-timeout-ms mesos))
     :mesos-leader-path (fnk [[:config {mesos nil}]]
                          (:leader-path mesos))
     :mesos-principal (fnk [[:config {mesos nil}]]
                        (:principal mesos))
     :mesos-role (fnk [[:config {mesos nil}]]
                   (when mesos
                     (or (:role mesos) "*")))
     :mesos-run-as-user (fnk [[:config {mesos nil}]]
                          (when (:run-as-user mesos)
                            (log/warn "Tasks launched in Mesos will ignore user specified in the job and run as" (:run-as-user mesos)))
                          (:run-as-user mesos))
     :mesos-framework-name (fnk [[:config {mesos nil}]]
                             (when mesos
                               (or (:framework-name mesos) "Cook")))
     :mesos-framework-id (fnk [[:config {mesos nil}]]
                           (:framework-id mesos))
     :jmx-metrics (fnk [[:config [:metrics {jmx false}]]]
                    (when jmx
                      ((util/lazy-load-var 'cook.reporter/jmx-reporter))))
     :graphite-metrics (fnk [[:config [:metrics {graphite nil}]]]
                         (when graphite
                           (when-not (:host graphite)
                             (throw (ex-info "You must specify the graphite host!" {:graphite graphite})))
                           (let [config (merge {:port 2003 :pickled? true} graphite)]
                             ((util/lazy-load-var 'cook.reporter/graphite-reporter) config))))
     :progress (fnk [[:config {progress nil}]]
                 (merge {:batch-size 100
                         :pending-threshold 4000
                         :publish-interval-ms 2500
                         :sequence-cache-threshold 1000}
                        progress))
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
                                                             (InetAddress/getLocalHost))}
                                              riemann)]
                            ((util/lazy-load-var 'cook.reporter/riemann-reporter) config))))
     :console-metrics (fnk [[:config [:metrics {console false}]]]
                        (when console
                          ((util/lazy-load-var 'cook.reporter/console-reporter))))

     :user-metrics-interval-seconds (fnk [[:config [:metrics {user-metrics-interval-seconds 20}]]]
                                      user-metrics-interval-seconds)

     :rebalancer (fnk [[:config {rebalancer nil}]]
                   (merge {:interval-seconds 300
                           :dru-scale 1.0}
                          rebalancer))

     :optimizer (fnk [[:config {optimizer nil}]]
                  (let [optimizer-config
                        (merge {:host-feed {:create-fn 'cook.mesos.optimizer/create-dummy-host-feed
                                            :config {}}
                                :optimizer {:create-fn 'cook.mesos.optimizer/create-dummy-optimizer
                                            :config {}}
                                :optimizer-interval-seconds 30}
                               optimizer)]
                    optimizer-config))

     :nrepl-server (fnk [[:config [:nrepl {enabled? false} {port 0}]]]
                     (when enabled?
                       (when (zero? port)
                         (throw (ex-info "You enabled nrepl but didn't configure a port. Please configure a port in your config file." {})))
                       ((util/lazy-load-var 'clojure.tools.nrepl.server/start-server) :port port)))

     :pools (fnk [[:config {pools nil}]]
              pools)

     :api-only? (fnk [[:config {api-only? false}]]
                  api-only?)}))

(defn read-config
  "Given a config file path, reads the config and returns the map"
  [config-file-path]
  (when-not (.exists (File. ^String config-file-path))
    (.println System/err (str "The config file doesn't appear to exist: " config-file-path)))
  (.println System/err (str "Reading config from file: " config-file-path))
  (let [config-format
        (try
          (Files/getFileExtension config-file-path)
          (catch Throwable t
            (.println System/err "Failed to get config file extension")
            (stacktrace/print-cause-trace t)
            (throw t)))]
    (try
      (case config-format
        "edn" (read-edn-config (slurp config-file-path))
        (do
          (.println System/err (str "Invalid config file format " config-format))
          (throw (ex-info "Invalid config file format" {:config-format config-format}))))
      (catch Throwable t
        (.println System/err "Failed to read edn config")
        (stacktrace/print-cause-trace t)
        (throw t)))))

(defn init-settings
  "Given a 'raw' config map, initializes and returns the settings map"
  [config-map]
  (try
    (let [literal-config {:config config-map}]
      (pre-configuration literal-config)
      (log/info "Configured logging")
      (log/info "Cook" @util/version "( commit" @util/commit ")")
      (let [settings {:settings (config-settings literal-config)}]
        (log/info "Interpreted settings")
        settings))
    (catch Throwable t
      (log/error t "Failed to initialize settings")
      (throw t))))

(mount/defstate config
                :start (init-settings (mount/args)))

(defn executor-config
  "Returns the executor config map from the config map"
  []
  (-> config :settings :executor))

(defn default-pool
  "Returns the default pool name from the config map"
  []
  (-> config :settings :pools :default))

(defn api-only-mode?
  "Returns true if api-only? mode is turned on"
  []
  (-> config :settings :api-only?))
