(ns cook.kubernetes.metrics
  (:require [metrics.core :as core]
            [metrics.counters :as counters]
            ;[metrics.histograms :as histograms]
            [metrics.meters :as meters]
            [metrics.timers :as timers])
  (:import (com.codahale.metrics Histogram MetricRegistry MetricRegistry$MetricSupplier SlidingTimeWindowArrayReservoir)
           (java.util.concurrent TimeUnit)))

(defn calculate-name
  "Given a metric name and compute cluster name, come up with the metric path to use."
  [metric-name compute-cluster-name]
  ["cook-k8s"
   metric-name
   (str "compute-cluster-" compute-cluster-name)])

(defn counter
  "Given a metric name and a compute cluster name, returns a counter metric."
  [metric-name compute-cluster-name]
  (counters/counter (calculate-name metric-name compute-cluster-name)))

(defn meter
  "Given a metric name and a compute cluster name, returns a meter metric."
  [metric-name compute-cluster-name]
  (meters/meter (calculate-name metric-name compute-cluster-name)))

(defn timer
  "Given a metric name and a compute cluster name, returns a timer metric."
  [metric-name compute-cluster-name]
  (timers/timer (calculate-name metric-name compute-cluster-name)))

(def histogram-supplier
  (reify
    MetricRegistry$MetricSupplier
    (newMetric [_]
      (Histogram. (SlidingTimeWindowArrayReservoir. 1 TimeUnit/MINUTES)))))

(defn histogram
  "Given a metric name and a compute cluster name, returns a histogram metric."
  [metric-name compute-cluster-name]
  ;(histograms/histogram (calculate-name metric-name compute-cluster-name))
  (.histogram
    ^MetricRegistry metrics.core/default-registry
    (core/metric-name
      (calculate-name metric-name compute-cluster-name))
    histogram-supplier))
