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
            [riemann.client :as riemann]
            [metrics.timers :as timers]
            [metrics.core :as metrics]
            [clj-time.core :as time]
            [clj-time.periodic :as periodic]
            [chime :refer [chime-at]])
  (:import [java.util.concurrent Executors TimeUnit]
           [com.codahale.metrics.riemann Riemann RiemannReporter JvmMetricSet]
           [com.codahale.metrics Counter Timer Gauge Histogram Meter]))

;;; ===========================================================================

;; the separator of service name
(def event-service-separator " ")

;; the event tags list
(def event-tags (java.util.ArrayList. ["cook-cloud"]))

;; the canonical name of *this* host
(def localhost (.getCanonicalHostName (java.net.InetAddress/getLocalHost)))

;; the default registry
(def registry metrics/default-registry)

(defn metric-riemann-reporter
  "Send all metric events to riemann for metrics registered at metric-registry.
   Return a function to close this reporter.

   Note that the service of an event is of format:
   \"<event-service-prefix> default.default.<metrics title> <metric>\"
   E.g. for a (timer \"i-am-timer\"), its 99 percentile metric is
   \"cook metrics default.default.i-am-timer p99\". However, if a timer is
   defined by (timer [\"\" \"\" \"i-am-timer\"]), its 99 percentile metric is
   \"cook metrics i-am-timer p99\"."
  [& {:keys [registry period host port service-prefix]
      :or {registry registry
           period   2000
           host     (or (System/getProperty "cook.riemann")
                        localhost)
           port     5555}}]
  (let [reporter (try
                   (.. RiemannReporter
                       ;; set the customized registry.
                       (forRegistry registry)
                       ;; set the prefix of service of events.
                       (prefixedWith service-prefix)
                       ;; set the event-tags
                       (tags event-tags)
                       ;; set the host fields of events.
                       (localHost localhost)
                       ;; set the separator to be a single space.
                       (useSeparator event-service-separator)
                       ;; construct the reporter instance.
                       (build (Riemann. host (Integer. port))))
                   (catch Exception e
                     (log/error e "Can NOT construct metric riemann reporter.")))
        jvm-metric-map (->> (JvmMetricSet. event-service-separator)
                         (.getMetrics)
                         (java.util.TreeMap.))
        empty-map (java.util.TreeMap.)]
    (or (when reporter
          (log/info "Starting metric riemann reporter and send metrics to" host)
          (chime-at (periodic/periodic-seq (time/now) (time/millis period))
                    (fn [time]
                      ;; sending metrics for timers, meters etc. registered via registry
                      (.report reporter)
                      ;; sending jvm metrics only
                      (.report reporter jvm-metric-map empty-map empty-map empty-map empty-map)
                      :error-handler (fn [ex]
                                        (log/error ex "Reporting metrics to riemann failed!")))))
        (fn [] (log/warn "The metric riemann reporter was NOT started."
                         "Do nothing here.")))))

(comment
  (let [reporter (metric-riemann-reporter :host "ramqa1.pit.twosigma.com")]
    (Thread/sleep 50000)
    (reporter))
  )
