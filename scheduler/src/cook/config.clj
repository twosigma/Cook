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
            [congestion.limits :refer [RateLimit]]
            [cook.plugins.definitions :refer [JobRouter]]
            [cook.plugins.util :as putil]
            [cook.rest.impersonation :refer [impersonation-authorized-wrapper]]
            [cook.util :as util]
            [mount.core :as mount]
            [plumbing.core :refer [fnk] :as pc]
            [plumbing.graph :as graph])
  (:import (com.google.common.io Files)
           (com.netflix.fenzo VMTaskFitnessCalculator)
           (java.io File)
           (java.net InetAddress)
           (org.apache.curator.test TestingServer)
           (org.apache.log4j DailyRollingFileAppender Logger PatternLayout)
           (org.apache.log4j.rolling RollingFileAppender TimeBasedRollingPolicy)))

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

(def passport-logger-ns "passport-logger")

(defn init-logger
  ([] (init-logger {:levels {"datomic.db" :warn
                             "datomic.peer" :warn
                             "datomic.kv-cluster" :warn
                             "com.netflix.fenzo.AssignableVMs" :warn
                             "com.netflix.fenzo.TaskScheduler" :warn
                             "com.netflix.fenzo.AssignableVirtualMachine" :warn}}))
  ([{:keys [file rotate-hourly] :or {file "log/cook.log" rotate-hourly false} {:keys [default] :or {default :info} :as overrides} :levels}]
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
                      (if rotate-hourly
                        "'.'yyyy-MM-dd-HH"
                        "'.'yyyy-MM-dd"))
               :level default}
              overrides)
       (log4j-conf/set-loggers!
         (Logger/getLogger ^String passport-logger-ns)
         {:out (doto (RollingFileAppender.)
                 (.setLayout (PatternLayout.
                               "%d{ISO8601} %-5p %c [%t] - %m%n"))
                 (.setRollingPolicy (doto (TimeBasedRollingPolicy.)
                                      (.setFileNamePattern "log/cook-passport.log.%d")
                                      (.activateOptions)))
                 (.activateOptions))
          :level default}))
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

(def default-fenzo-scheduler-config {:scheduler "fenzo"
                                     :good-enough-fitness 0.8
                                     :fenzo-fitness-calculator default-fitness-calculator
                                     :fenzo-max-jobs-considered 1000
                                     :fenzo-scaleback 0.95
                                     :fenzo-floor-iterations-before-warn 10
                                     :fenzo-floor-iterations-before-reset 1000})
(def default-kubernetes-scheduler-config {:scheduler "kubernetes"
                                          :max-jobs-considered 500
                                          :minimum-scheduling-capacity-threshold 50
                                          :scheduling-pause-time-ms 3000})
(def default-schedulers-config [{:pool-regex ".*"
                                 :scheduler-config default-fenzo-scheduler-config}])

(defrecord UserRateLimit [id quota auth-bypass-quota ttl]
  RateLimit

  (get-key [self {:keys [authorization/user]}]
    (str (.getName ^Class (type self)) id "-" user))

  (get-quota [_ {:keys [authorization/user]}]
    (if user
      quota
      auth-bypass-quota))

  (get-ttl [_ _]
    ttl))

(defrecord MetricsRateLimit [id quota ttl]
  RateLimit

  (get-key [self {:keys [authorization/user]}]
    (str (.getName ^Class (type self)) id "-" user))

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

(defn guard-invalid-gpu-config
  "Throws if either of the following is true:
  - any one of the keys (pool-regex, valid-models, default-model) is not configured
  - there is no gpu-model in valid-gpu-models matching the configured default"
  [valid-gpu-models]
  (when valid-gpu-models
    (doseq [{:keys [default-model pool-regex valid-models] :as entry} valid-gpu-models]
      (when-not pool-regex
        (throw (ex-info (str "pool-regex key is missing from config") entry)))
      (when-not valid-models
        (throw (ex-info (str "Valid GPU models for pool-regex " pool-regex " is not defined") entry)))
      (when-not default-model
        (throw (ex-info (str "Default GPU model for pool-regex " pool-regex " is not defined") entry)))
      (when-not (contains? valid-models default-model)
        (throw (ex-info (str "Default GPU model for pool-regex " pool-regex " is not listed as a valid GPU model") entry))))))

(defn guard-invalid-disk-config
  "Throws if either of the following is true:
  - any one of the keys (pool-regex, valid-types, default-type, max-size) is not configured
  - there is no disk-type in valid-disk-types matching the configured default"
  [disk]
  (when disk
    (doseq [{:keys [default-type pool-regex valid-types max-size] :as entry} disk]
      (when-not pool-regex
        (throw (ex-info (str "pool-regex key is missing from config") entry)))
      (when-not max-size
        (throw (ex-info (str "Max requestable disk size for pool-regex " pool-regex " is not defined") entry)))
      (when-not valid-types
        (throw (ex-info (str "Valid disk types for pool-regex " pool-regex " is not defined") entry)))
      (when-not default-type
        (throw (ex-info (str "Default disk type for pool-regex " pool-regex " is not defined") entry)))
      (when-not (contains? valid-types default-type)
        (throw (ex-info (str "Default disk type for pool-regex " pool-regex " is not listed as a valid disk type") entry))))))

(defn guard-invalid-schedulers-config
  "When scheduler config for pools is provided, ensure each is associated with a pool and is using a valid scheduler."
  [schedulers-config]
  (when schedulers-config
    (doseq [{pool-regex :pool-regex 
             {scheduler :scheduler :as scheduler-config} :scheduler-config 
             :as entry} schedulers-config] 
      (when-not pool-regex
        (throw (ex-info (str "pool-regex key is missing from config") entry)))
      (when-not scheduler-config
        (throw (ex-info (str "scheduler-config key is missing from config") entry)))
      (when-not scheduler
        (throw (ex-info (str "scheduler key is missing from scheduler-config") entry)))
      (when-not (or (= scheduler "fenzo") (= scheduler "kubernetes"))
        (throw (ex-info (str "scheduler must be fenzo or kubernetes") entry))))))

(def config-settings
  "Parses the settings out of a config file"
  (graph/eager-compile
    {:agent-query-cache (fnk [[:config {agent-query-cache nil}]]
                          (merge
                            {:max-size 5000
                             :ttl-ms (* 60 1000)}
                            agent-query-cache))
     :compute-clusters (fnk [[:config {compute-clusters []}]]
                         compute-clusters)
     :compute-cluster-options (fnk [[:config {compute-cluster-options {}}]]
                                (merge
                                  {:load-clusters-on-startup? false
                                   :compute-cluster-templates {}}
                                  compute-cluster-options))
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
                   (let [{:keys [auth-bypass-limit-per-m expire-minutes user-limit-per-m user-limit-for-metrics-per-m job-submission per-user-per-pool-job-launch]
                          :or {auth-bypass-limit-per-m 600
                               expire-minutes 120
                               user-limit-per-m 600
                               user-limit-for-metrics-per-m 30}} rate-limit]
                     {:expire-minutes expire-minutes
                      :job-submission job-submission
                      :per-user-per-pool-job-launch per-user-per-pool-job-launch
                      :user-limit (->UserRateLimit
                                    :user-limit
                                    user-limit-per-m
                                    auth-bypass-limit-per-m
                                    (t/minutes 1))
                      :user-limit-for-metrics (->MetricsRateLimit
                                            :user-limit-for-metrics
                                            user-limit-for-metrics-per-m
                                            (t/minutes 1))}))
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
     ;; Ignore these group ID's when computing supplemental group IDs.
     :ignore-supplemental-group-ids (fnk [[:config {ignore-supplemental-group-ids #{}}]]
                                      ignore-supplemental-group-ids)
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
     :mea-culpa-failure-limit (fnk [[:config {scheduler nil}]]
                                (:mea-culpa-failure-limit scheduler))
     :max-over-quota-jobs (fnk [[:config {scheduler nil}]]
                            (or (when scheduler
                                  (:max-over-quota-jobs scheduler))
                                100))
     :mesos-gpu-enabled (fnk [[:config {mesos nil}]]
                          (when mesos
                            (boolean (or (:enable-gpu-support mesos) false))))
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
     :console-metrics (fnk [[:config [:metrics {console false}]]]
                        (when console
                          ((util/lazy-load-var 'cook.reporter/console-reporter))))

     :user-metrics-interval-seconds (fnk [[:config [:metrics {user-metrics-interval-seconds 20}]]]
                                      user-metrics-interval-seconds)

     :rebalancer (fnk [[:config {rebalancer nil}]]
                   (when (:min-utilization-threshold rebalancer)
                     (log/warn "The :min-utilization-threshold configuration field is no longer used"))
                   (merge {:interval-seconds 300
                           :dru-scale 1.0
                           :pool-regex ".*"}
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
                 (guard-invalid-gpu-config (:valid-gpu-models pools))
                 (guard-invalid-disk-config (:disk pools))
                 (guard-invalid-schedulers-config (:schedulers pools))
                 (cond-> pools
                   (:job-resource-adjustment pools)
                   (update :job-resource-adjustment
                           #(-> %
                                (update :pool-regex re-pattern)))
                   (not (:default-containers pools))
                   (assoc :default-containers [])
                   (not (:default-env pools))
                   (assoc :default-env [])
                   (not (:quotas pools))
                   (assoc :quotas [])

                   ; If valid configs up-to "scheduler" are provided, merge defaults
                   (seq (:schedulers pools))
                   (assoc :schedulers
                          (mapv (fn [{:keys [pool-regex] {:keys [scheduler] :as scheduler-config} :scheduler-config}]
                                 {:pool-regex pool-regex
                                  :scheduler-config (cond
                                                      (= scheduler "fenzo") (merge default-fenzo-scheduler-config scheduler-config)
                                                      (= scheduler "kubernetes") (merge default-kubernetes-scheduler-config scheduler-config)
                                                      :else (throw (ex-info (str "Unexpected scheduler configured " scheduler) {})))}) 
                               (:schedulers pools)))
                   (empty? (:schedulers pools))
                   (assoc :schedulers default-schedulers-config)))
     :rank (fnk [[:config {rank {:number-to-force 1000}}]]
                rank)
     :api-only? (fnk [[:config {api-only? false}]]
                  api-only?)
     :cache-working-set-size (fnk [[:config {cache-working-set-size 1000000}]]
                               cache-working-set-size)
     :estimated-completion-constraint (fnk [[:config {estimated-completion-constraint nil}]]
                                        (merge {:agent-start-grace-period-mins 10}
                                               estimated-completion-constraint))
     :plugins (fnk [[:config {plugins {}}]]
                (let [{:keys [job-launch-filter job-routing job-submission-validator pool-selection job-submission-modifier]} plugins]
                  (merge plugins
                         {:job-launch-filter
                          (merge
                            {:age-out-last-seen-deadline-minutes 10
                             :age-out-first-seen-deadline-minutes 600
                             :age-out-seen-count 10}
                            job-launch-filter)
                          ; The job-routing config has the following shape:
                          ;
                          ;   pool-name -> job-router-factory-fn
                          ;
                          ; where job-router-factory-fn must return
                          ; something that satisfies the JobRouter protocol
                          :job-routing
                          (pc/map-vals
                            (fn [f]
                              (if-let [resolved-fn (putil/resolve-symbol (symbol f))]
                                (let [job-router (resolved-fn)]
                                  (if (and job-router (satisfies? JobRouter job-router))
                                    job-router
                                    (throw (ex-info (str "Calling fn " f " did not return a JobRouter") {}))))
                                (throw (ex-info (str "Unable to resolve fn " f) {}))))
                            job-routing)
                          :job-submission-validator
                          (merge {:batch-timeout-seconds 40}
                                 job-submission-validator)
                          :pool-selection
                          (merge {:attribute-name "cook-pool"
                                  :default-pool "no-pool"}
                                 pool-selection)
                          :job-submission-modifier
                          job-submission-modifier})))
     :quota-grouping (fnk [[:config {quota-grouping {}}]]
                       quota-grouping)
     :pg-config (fnk [[:config {pg-config nil}]]
                  pg-config)
     :passport (fnk [[:config {passport {}}]]
                 (merge {:job-cache-expiry-time-hours 24
                         :job-cache-set-size 1000000}
                        passport))
     :kubernetes (fnk [[:config {kubernetes {}}]]
                   (let [{:keys [controller-lock-num-shards
                                 telemetry-tags-key-invalid-char-pattern]
                          :or {controller-lock-num-shards 4095}}
                         kubernetes
                         _
                         (when (not (< 0 controller-lock-num-shards 32778))
                           (throw
                             (ex-info
                               "Please configure :controller-lock-num-shards to > 0 and < 32778 in your config file."
                               kubernetes)))]
                     (merge {:autoscaling-scale-factor 1.0
                             :clobber-synthetic-pods false
                             :controller-lock-num-shards controller-lock-num-shards
                             :default-workdir "/mnt/sandbox"
                             :list-pods-limit 2000
                             :max-jobs-for-autoscaling 1000
                             :pod-condition-containers-not-initialized-seconds 120
                             :pod-condition-containers-not-ready-seconds 120
                             :pod-condition-unschedulable-seconds 60
                             :reconnect-delay-ms 60000
                             :set-container-cpu-limit? true
                             :set-memory-limit? true
                             :synthetic-pod-condition-unschedulable-seconds 900
                             :synthetic-pod-recency-size 50000
                             :synthetic-pod-recency-seconds 120 ; Should be greater than the time to start a node and have it accept workloads.
                             }
                            (cond-> kubernetes
                                    telemetry-tags-key-invalid-char-pattern
                                    (assoc :telemetry-tags-key-invalid-char-pattern
                                           (re-pattern telemetry-tags-key-invalid-char-pattern))))))
     :offer-matching (fnk [[:config {offer-matching {}}]]
                       (merge {:considerable-job-threshold-to-collect-job-match-statistics 20
                               :global-min-match-interval-millis 100
                               :target-per-pool-match-interval-millis 3000
                               :unmatched-cycles-warn-threshold 500
                               :unmatched-fraction-warn-threshold 0.5}
                              offer-matching))
     :queue-limits (fnk [[:config {queue-limits {}}]]
                     (merge {:update-interval-seconds 180}
                            queue-limits))
     :constraint-attribute->transformation
     (fnk [[:config {constraint-attribute->transformation {}}]]
       (pc/map-vals
         #(update
            %
            :pattern-transformations
            (fn [pattern-transformations]
              (map
                (fn [pattern-transformation]
                  (update pattern-transformation :match re-pattern))
                pattern-transformations)))
         constraint-attribute->transformation))
     :production?
     (fnk [[:config {production? nil}]]
       production?)}))

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

(defn disk
  "Returns disk configurations"
  []
  (-> config :settings :pools :disk))

(defn valid-gpu-models
  "Returns valid GPU models for the pool the job is scheduled in"
   []
   (-> config :settings :pools :valid-gpu-models))

(defn job-resource-adjustments
  "Returns the specification for how to adjust resources requested by a job based on the pool it's scheduled on.
   The specification consists of an applicable pool name regex and the name of a function that adjusts resources."
  []
  (-> config :settings :pools :job-resource-adjustment))

(defn default-job-constraints
  "Returns the per-pool-regex default job constraints.
  This function returns a list with the following shape:
  [{:pool-regex ...
    :default-constraints [{:constraint/attribute ...
                           :constraint/operator ...
                           :constraint/pattern ...}
                          ...]}
   ...]"
  []
  (-> config :settings :pools :default-job-constraints))

(defn api-only-mode?
  "Returns true if api-only? mode is turned on"
  []
  (-> config :settings :api-only?))

(defn estimated-completion-config
  []
  (-> config :settings :estimated-completion-constraint))

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

(defn compute-cluster-options
  []
  (get-in config [:settings :compute-cluster-options]))

(defn compute-cluster-templates
  []
  (:compute-cluster-templates (compute-cluster-options)))

(defn passport
  []
  (get-in config [:settings :passport]))

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

(defn pool-quotas
  []
  (get-in config [:settings :pools :quotas]))

(defn offer-matching
  []
  (-> config :settings :offer-matching))

(defn queue-limits
  []
  (-> config :settings :queue-limits))

(defn job-resource-limits
  []
  (-> config :settings :plugins :job-shape-validation))

(defn job-routing
  []
  (-> config :settings :plugins :job-routing))

(defn job-routing-pool-name?
  "Returns truthy if the given pool name is a job-routing pool name"
  [pool-name-from-submission]
  (get (job-routing) pool-name-from-submission))

(defn constraint-attribute->transformation
  []
  (-> config :settings :constraint-attribute->transformation))

(defn quota-grouping-config
  "Configuration flags for grouping quota."
  []
  (-> config :settings :quota-grouping))

(defn pool-schedulers
  []
  (get-in config [:settings :pools :schedulers]))
