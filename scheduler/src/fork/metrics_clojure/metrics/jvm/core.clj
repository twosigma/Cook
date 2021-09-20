(ns fork.metrics-clojure.metrics.jvm.core
  (:import (com.codahale.metrics MetricRegistry)
           (com.codahale.metrics.jvm ThreadStatesGaugeSet GarbageCollectorMetricSet FileDescriptorRatioGauge
                                     MemoryUsageGaugeSet  JvmAttributeGaugeSet))
  (:require [metrics.core :refer [add-metric default-registry]]))

(defn register-jvm-attribute-gauge-set
  ([^MetricRegistry reg]
   (register-jvm-attribute-gauge-set reg ["jvm" "attribute"]))
  ([^MetricRegistry reg title]
   (add-metric reg title (new JvmAttributeGaugeSet))))

(defn register-memory-usage-gauge-set
  ([^MetricRegistry reg]
   (register-memory-usage-gauge-set reg ["jvm" "memory"]))
  ([^MetricRegistry reg title]
   (add-metric reg title (new MemoryUsageGaugeSet))))

(defn register-file-descriptor-ratio-gauge-set
  ([^MetricRegistry reg]
   (register-file-descriptor-ratio-gauge-set reg ["jvm" "file"]))
  ([^MetricRegistry reg title]
   (add-metric reg title (new FileDescriptorRatioGauge))))

(defn register-garbage-collector-metric-set
  ([^MetricRegistry reg]
   (register-garbage-collector-metric-set reg ["jvm" "gc"]))
  ([^MetricRegistry reg title]
   (add-metric reg title (new GarbageCollectorMetricSet))))

(defn register-thread-state-gauge-set
  ([^MetricRegistry reg]
   (register-thread-state-gauge-set reg ["jvm" "thread"]))
  ([^MetricRegistry reg title]
   (add-metric reg title (new ThreadStatesGaugeSet))))

(defn instrument-jvm
  ([]
   (instrument-jvm default-registry))
  ([^MetricRegistry reg]
   (doseq [register-metric-set [register-jvm-attribute-gauge-set
                                register-memory-usage-gauge-set
                                register-file-descriptor-ratio-gauge-set
                                register-garbage-collector-metric-set
                                register-thread-state-gauge-set]]
     (register-metric-set reg))))
