(ns cook.compute-cluster.metrics
  (:require [metrics.timers :as timers]))

(defn calculate-name
  "Given a metric name and compute cluster name, come up with the metric path to use."
  [metric-name compute-cluster-name]
  ["cook"
   metric-name
   (str "compute-cluster-" compute-cluster-name)])

(defn timer
  "Given a metric name and a compute cluster name, returns a timer metric."
  [metric-name compute-cluster-name]
  (timers/timer (calculate-name metric-name compute-cluster-name)))
