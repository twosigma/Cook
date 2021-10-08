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
  (:require [clojure.tools.logging :as log]
            [datomic.api :refer [q]]
            [metatransaction.core :refer [db]]
            [metrics.core :as metrics])
  (:import (com.codahale.metrics ConsoleReporter MetricFilter)
           (com.codahale.metrics.graphite Graphite GraphiteReporter PickledGraphite)
           (java.net InetSocketAddress)
           (java.util.concurrent TimeUnit)))

;; the default registry
(def registry metrics/default-registry)

(defn jmx-reporter
  []
  (.. (com.codahale.metrics.jmx.JmxReporter/forRegistry metrics/default-registry)
      (build)
      (start)))

(defn graphite-reporter
  [{:keys [prefix host port pickled?]}]
  (log/info "Starting graphite reporter")
  (let [addr (InetSocketAddress. host port)
        graphite (if pickled?
                   (PickledGraphite. addr)
                   (Graphite. addr))]
    (doto (.. (GraphiteReporter/forRegistry metrics/default-registry)
              (prefixedWith prefix)
              (filter MetricFilter/ALL)
              (convertRatesTo TimeUnit/SECONDS)
              (convertDurationsTo TimeUnit/MILLISECONDS)
              (build graphite))
      (.start 30 TimeUnit/SECONDS))))

(defn console-reporter
  "Creates and starts a ConsoleReporter for metrics"
  []
  (doto (.. (ConsoleReporter/forRegistry metrics/default-registry)
            (convertRatesTo TimeUnit/SECONDS)
            (convertDurationsTo TimeUnit/MILLISECONDS)
            (build))
    (.start 30 TimeUnit/SECONDS)))
