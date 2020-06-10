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
            [cook.rest.impersonation :refer (impersonation-authorized-wrapper)]
            [cook.util :as util]
            [mount.core :as mount]
            [plumbing.core :refer (fnk)]
            [plumbing.graph :as graph]
            [schema.core :as s])
  (:import (com.google.common.io Files)
           (com.netflix.fenzo VMTaskFitnessCalculator)
           (java.io File)
           (java.net InetAddress)
           (java.util.concurrent.locks ReentrantLock)
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
       (.println System/err (str "Failed to initialize logging! Error: " (.toString t)))
       (stacktrace/print-cause-trace t)
       (throw t)))))

(def pre-configuration
  "This configures logging and exception handling, to make the configuration phase simpler to understand"
  (graph/eager-compile
    {:exception-handler (fnk [[:config [:unhandled-exceptions {log-level :error} {email nil}]]]
                          ((util/lazy-load-var 'cook.util/install-email-on-exception-handler) log-level email))
     :logging (fnk [[:config log]]
                (init-logger log))}))

(def default-authorization {:authorization-fn 'cook.rest.authorization/open-auth})
(def default-fitness-calculator "com.netflix.fenzo.plugins.BinPackingFitnessCalculators/cpuMemBinPacker")

(defrecord UserRateLimit [id quota ttl]
  RateLimit
  (get-key [self req]
    (str (.getName (type self)) id "-" (:authorization/user req)))
  (get-quota [_ _]
    quota)
  (get-ttl [_ _]
    ttl))

(defn config-string->fitness-calculator
  "Given a string specified in the configuration, attempt to resolve it
  to and return an instance of com.netflix.fenzo.VMTaskFitnessCalculator.
  The config string can either be a reference to a clojure symbol, or to a
  static member of a java class (for example, one of the fitness calculators
  that ship with Fenzo).  An exception will be thrown if a VMTaskFitnessCalculator
  can't be found using either method."
  ^VMTaskFitnessCalculator
  [config-string]
  (let [calculator
        (try
          (let [value (-> config-string symbol resolve deref)]
            (if (fn? value)
              (value)
              value))
          (catch NullPointerException e
            (log/debug "fitness-calculator" config-string
                       "couldn't be resolved to a clojure symbol."
                       "Seeing if it refers to a java static field...")
            (try
              (let [[java-class-name field-name] (str/split config-string #"/")
                    java-class (-> java-class-name symbol resolve)]
                (clojure.lang.Reflector/getStaticField java-class field-name))
              (catch Exception e
                (throw (IllegalArgumentException.
                         (str config-string " could not be resolved to a clojure symbol or to a java static field")))))))]
    (if (instance? VMTaskFitnessCalculator calculator)
      calculator
      (throw (IllegalArgumentException.
               (str config-string " is not a VMTaskFitnessCalculator"))))))

(def config-settings
  "Parses the settings out of a config file"
  (graph/eager-compile
    {:agent-query-cache (fnk [[:config {agent-query-cache nil}]]
                          (merge
                            {:max-size 5000
                             :ttl-ms (* 60 1000)}
                            agent-query-cache))
     :compute-clusters (fnk [[:config {compute-clusters []}
                              {mesos nil}]]
                         (if (seq compute-clusters)
                           compute-clusters
                           [{:factory-fn 'cook.mesos.mesos-compute-cluster/factory-fn
                             :config {:compute-cluster-name (or (:compute-cluster-name mesos)
                                                                "default-compute-cluster-from-config-defaulting")
                                      :framework-id (:framework-id mesos)
                                      :master (:master mesos)
                                      :failover-timeout (:failover-timeout-ms mesos)
                                      :principal (:principal mesos)
                                      :role (:role mesos)
                                      :framework-name (:framework-name mesos)}}]))
     :cors-origins (fnk [[:config {cors-origins nil}]]
                     (map re-pattern (or cors-origins [])))
     :exit-code-syncer (fnk [[:config {exit-code-syncer nil}]]
                         (merge
                           {:publish-batch-size 100
                            :publish-interval-ms 2500}
                           exit-code-syncer))
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
                         (let [auth-fn @(util/lazy-load-var 'cook.rest.authorization/is-authorized?)]
                           ; we only wrap the authorization function if we have impersonators configured
                           (if (-> authorization-config :impersonators seq)
                             (impersonation-authorized-wrapper auth-fn authorization-config)
                             (partial auth-fn authorization-config))))
     :authorization-middleware (fnk [[:config [:authorization {one-user false} {kerberos false} {http-basic false}]]]
                                 (cond
                                   http-basic
                                   (let [validation (get http-basic :validation :none)
                                         user-password-valid?
                                         ((util/lazy-load-var 'cook.rest.basic-auth/make-user-password-valid?) validation http-basic)]
                                     (log/info "Using http basic authorization with validation" validation)
                                     (with-meta
                                       ((util/lazy-load-var 'cook.rest.basic-auth/create-http-basic-middleware) user-password-valid?)
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
                                       @(util/lazy-load-var 'cook.rest.spnego/require-gss)
                                       {:json-value "kerberos"}))
                                   :else (throw (ex-info "Missing authorization configuration" {}))))
     :impersonation-middleware (fnk [[:config {authorization-config nil}]]
                                 (let [{impersonators :impersonators} authorization-config]
                                   (with-meta
                                     ((util/lazy-load-var 'cook.rest.impersonation/create-impersonation-middleware) impersonators)
                                     {:json-value "config-impersonation"})))
     :rate-limit (fnk [[:config {rate-limit nil}]]
                   (let [{:keys [expire-minutes user-limit-per-m global-job-launch job-submission job-launch]
                          :or {expire-minutes 120
                               user-limit-per-m 600}} rate-limit]
                     {:expire-minutes expire-minutes
                      :global-job-launch global-job-launch
                      :job-submission job-submission
                      :job-launch job-launch
                      :user-limit (->UserRateLimit :user-limit user-limit-per-m (t/minutes 1))}))
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
     :cluster-dns-name (fnk [[:config {cluster-dns-name nil}]]
                         cluster-dns-name)
     :scheduler-rest-url (fnk [cluster-dns-name hostname server-https-port server-port]
                           (let [[scheme port] (if server-https-port
                                                 ["https" server-https-port]
                                                 ["http" server-port])
                                 host (or cluster-dns-name
                                          (do (log/warn "Missing cluster-dns-name in config."
                                                        "REST callbacks will use the master's hostname.")
                                              hostname))]
                             (str scheme "://" host ":" port)))
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
                            :cpus 4
                            :max-ports 5}
                           (:task-constraints scheduler)))
     :offer-incubate-time-ms (fnk [[:config {scheduler nil}]]
                               (when scheduler
                                 (or (:offer-incubate-ms scheduler) 15000)))
     :agent-attributes-cache (fnk [[:config {scheduler nil}]]
                               (when scheduler
                                 (merge
                                   {:max-size 2000}
                                   (:offer-cache scheduler))))
     :mea-culpa-failure-limit (fnk [[:config {scheduler nil}]]
                                (:mea-culpa-failure-limit scheduler))
     :max-over-quota-jobs (fnk [[:config {scheduler nil}]]
                            (or (when scheduler
                                  (:max-over-quota-jobs scheduler))
                                100))
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
     ; TODO(pschorf): Rename
     :mesos-leader-path (fnk [[:config {mesos nil}]]
                          (:leader-path mesos))
     ; TODO(pschorf): Rename
     :mesos-run-as-user (fnk [[:config {mesos nil}]]
                          (when (:run-as-user mesos)
                            (log/warn "Tasks launched in Mesos will ignore user specified in the job and run as" (:run-as-user mesos)))
                          (:run-as-user mesos))
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
                   (when (:min-utilization-threshold rebalancer)
                     (log/warn "The :min-utilization-threshold configuration field is no longer used"))
                   (merge {:interval-seconds 300
                           :dru-scale 1.0}
                          rebalancer))

     :optimizer (fnk [[:config {optimizer nil}]]
                  (let [optimizer-config
                        (merge {:host-feed {:create-fn 'cook.scheduler.optimizer/create-dummy-host-feed
                                            :config {}}
                                :optimizer {:create-fn 'cook.scheduler.optimizer/create-dummy-optimizer
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
              (cond-> pools
                (:job-resource-adjustment pools)
                (update :job-resource-adjustment
                        #(-> %
                             (update :pool-regex re-pattern)))
                (not (:default-containers pools))
                (assoc :default-containers [])))
     :api-only? (fnk [[:config {api-only? false}]]
                  api-only?)
     :estimated-completion-constraint (fnk [[:config {estimated-completion-constraint nil}]]
                                        (merge {:agent-start-grace-period-mins 10}
                                               estimated-completion-constraint))
     :data-local-fitness-calculator (fnk [[:config {data-local {}}]]
                                      (let [fitness-calculator (get data-local :fitness-calculator {})]
                                        {:auth (get fitness-calculator :auth nil)
                                         :base-calculator (config-string->fitness-calculator
                                                            (get fitness-calculator :base-calculator "com.netflix.fenzo.plugins.BinPackingFitnessCalculators/cpuMemBinPacker"))
                                         :batch-size (get fitness-calculator :batch-size 500)
                                         :cache-ttl-ms (get fitness-calculator :cache-ttl-ms 300000)
                                         :cost-endpoint (get fitness-calculator :cost-endpoint nil)
                                         :data-locality-weight (get fitness-calculator :data-locality-weight 0.95)
                                         :launch-wait-seconds (get fitness-calculator :launch-wait-seconds 60)
                                         :update-interval-ms (get fitness-calculator :update-interval-ms nil)}))
     :plugins (fnk [[:config {plugins {}}]]
                (let [{:keys [job-launch-filter job-submission-validator
                              pool-selection]} plugins]
                  (merge plugins
                         {:job-launch-filter
                          (merge
                            {:age-out-last-seen-deadline-minutes 10
                             :age-out-first-seen-deadline-minutes 600
                             :age-out-seen-count 10}
                            job-launch-filter)
                          :job-submission-validator
                          (merge {:batch-timeout-seconds 40}
                                 job-submission-validator)
                          :pool-selection
                          (merge {:attribute-name "cook-pool"
                                  :default-pool "no-pool"}
                                 pool-selection)})))
     :kubernetes (fnk [[:config {kubernetes {}}]]
                   (let [{:keys [controller-lock-num-shards]
                          :or {controller-lock-num-shards 32}}
                         kubernetes
                         _
                         (when (not (< 0 controller-lock-num-shards 256))
                           (throw
                             (ex-info
                               "Please configure :controller-lock-num-shards to > 0 and < 256 in your config file."
                               kubernetes)))
                         lock-objects
                         (repeatedly
                           controller-lock-num-shards
                           #(ReentrantLock.))]
                     (merge {:controller-lock-num-shards controller-lock-num-shards
                             :controller-lock-objects (with-meta
                                                        lock-objects
                                                        {:json-value (str lock-objects)})
                             :default-workdir "/mnt/sandbox"
                             :pod-condition-containers-not-initialized-seconds 120
                             :pod-condition-unschedulable-seconds 60
                             :reconnect-delay-ms 60000
                             :set-container-cpu-limit? true}
                            kubernetes)))}))

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
        (log/info "Interpreted settings:" settings)
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
  (let [pool (-> config :settings :pools :default)]
    (if (str/blank? pool)
      nil
      pool)))

(defn valid-gpu-models
  "Returns valid GPU models for the pool the job is scheduled in"
   []
   (-> config :settings :pools :valid-gpu-models))

(defn job-resource-adjustments
  "Returns the specification for how to adjust resources requested by a job based on the pool it's scheduled on.
   The specification consists of an applicable pool name regex and the name of a function that adjusts resources."
  []
  (-> config :settings :pools :job-resource-adjustment))

(defn api-only-mode?
  "Returns true if api-only? mode is turned on"
  []
  (-> config :settings :api-only?))

(defn estimated-completion-config
  []
  (-> config :settings :estimated-completion-constraint))

(defn data-local-fitness-config
  []
  (-> config :settings :data-local-fitness-calculator))

(defn fitness-calculator-config
  []
  (-> config :settings :fenzo-fitness-calculator))

(defn fitness-calculator
  "Returns the fitness calculator specified by fitness-calculator, or the default if nil"
  [fitness-calculator]
  (config-string->fitness-calculator (or fitness-calculator default-fitness-calculator)))

(defn batch-timeout-seconds-config
  "Used by job submission plugin."
  []
  (-> config :settings :plugins :job-submission-validator :batch-timeout-seconds t/seconds))

(defn age-out-last-seen-deadline-minutes-config
  "Used by job launch plugin."
  []
  (-> config :settings :plugins :job-launch-filter :age-out-last-seen-deadline-minutes t/minutes))

(defn age-out-first-seen-deadline-minutes-config
  "Used by job launch plugin."
  []
  (-> config :settings :plugins :job-launch-filter :age-out-first-seen-deadline-minutes t/minutes))

(defn age-out-seen-count-config
  "Used by job launch plugin."
  []
  (-> config :settings :plugins :job-launch-filter :age-out-seen-count))

(defn max-over-quota-jobs
  []
  (get-in config [:settings :max-over-quota-jobs]))

(defn compute-clusters
  []
  (get-in config [:settings :compute-clusters]))

(defn kubernetes
  []
  (get-in config [:settings :kubernetes]))

(defn scheduler-rest-url
  "Get the URL for REST calls back to the Cook Scheduler API.
   Used by Kubernetes pod sidecar to send messages back to Cook."
  []
  (get-in config [:settings :scheduler-rest-url]))

(defn task-constraints
  []
  (get-in config [:settings :task-constraints]))
