(ns cook.kubernetes.metrics
  (:require [metrics.core :as core]
            [metrics.counters :as counters]
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
      (Histogram.
        ; The default implementation of `Reservoir` in dropwizard metrics is
        ; `ExponentiallyDecayingReservoir`, which stores data samples for some
        ; time. When new samples stop arriving, it uses the historical data and
        ; returns the same characteristics for the data distribution again and
        ; again, simply because the data distribution doesn’t change. Here we
        ; switch from the default `ExponentiallyDecayingReservoir` to a sliding
        ; time window reservoir, which gives zeros when there is no data. See
        ; https://engineering.salesforce.com/be-careful-with-reservoirs-708884018daf
        ; for more information.
        (SlidingTimeWindowArrayReservoir. 300 TimeUnit/SECONDS)))))

(defn histogram
  "Given a metric name and a compute cluster name, returns a histogram metric."
  [metric-name compute-cluster-name]
  (.histogram
    ^MetricRegistry core/default-registry
    (core/metric-name
      (calculate-name metric-name compute-cluster-name))
    histogram-supplier))
