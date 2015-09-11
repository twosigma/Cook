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
(ns cook.reporter
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [datomic.api :as d :refer (q)]
            [metatransaction.core :as mt :refer (db)]
            ;[riemann.client :as riemann]
            ;[metrics.timers :as timers]
            [metrics.core :as metrics]
            ;[clj-time.core :as time]
            ;[clj-time.periodic :as periodic]
            ;[chime :refer [chime-at]]
            )
  ;(:import [java.util.concurrent Executors TimeUnit]
  ;         [com.codahale.metrics.riemann Riemann RiemannReporter JvmMetricSet]
  ;         [com.codahale.metrics Counter Timer Gauge Histogram Meter])
  )

;; the default registry
(def registry metrics/default-registry)

;(defn metric-riemann-reporter
;  "Send all metric events to riemann for metrics registered at metric-registry.
;   Return a function to close this reporter.
;
;   Note that the service of an event is of format:
;   \"<event-service-prefix> default.default.<metrics title> <metric>\"
;   E.g. for a (timer \"i-am-timer\"), its 99 percentile metric is
;   \"cook metrics default.default.i-am-timer p99\". However, if a timer is
;   defined by (timer [\"\" \"\" \"i-am-timer\"]), its 99 percentile metric is
;   \"cook metrics i-am-timer p99\"."
;  [{:keys [host port tags prefix]}]
;  (let [reporter (try
;                   (.. RiemannReporter
;                       (forRegistry registry)
;                       (prefixedWith prefix)
;                       (tags tags)
;                       (build (Riemann. host (Integer. port))))
;                   (catch Exception e
;                     (log/error e "couldn't construct metric riemann reporter.")
;                     (throw e)))
;        jvm-metric-map (->> (JvmMetricSet.)
;                         (.getMetrics)
;                         (java.util.TreeMap.))
;        empty-map (java.util.TreeMap.)]
;    (or (when reporter
;          (log/info "Starting metric riemann reporter and send metrics to" host)
;          (chime-at (periodic/periodic-seq (time/now) (time/millis 10000))
;                    (fn [time]
;                      ;; sending metrics for timers, meters etc. registered via registry
;                      (.report reporter)
;                      ;; sending jvm metrics only
;                      (.report reporter jvm-metric-map empty-map empty-map empty-map empty-map)
;                      :error-handler (fn [ex]
;                                        (log/error ex "Reporting metrics to riemann failed!")))))
;        (fn [] (log/fatal "The metric riemann reporter was NOT started.")))))

(defn jmx-reporter
  []
  (.. (com.codahale.metrics.JmxReporter/forRegistry metrics/default-registry)
      (build)
      (start)))
