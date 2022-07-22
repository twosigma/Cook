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

;; Declares prometheus metrics for cook scheduler.

(ns cook.prometheus-metrics
  (:require [iapetos.core :as prometheus]
            [iapetos.export :as prometheus-export]))

(defonce registry
         ;; A global registry for all metrics reported by Cook.
         ;; All metrics must be registered before they can be recorded.
         ;; We are standardizing the metric format to be :cook/<component>-<metric-name>-<unit>
         (-> (prometheus/collector-registry)
           (prometheus/register
             ;; Scheduler metrics --------------------------------------------------------------------------------------
             ;; Note that we choose to use a summary instead of a histogram for the latency metrics because we only have
             ;; one scheduler process running per cluster, so we do not need to aggregate data from multiple sources.
             ;; The quantiles are specified as a map of quantile to error margin.
             (prometheus/summary :cook/scheduler-rank-cycle-duration-seconds
                                 {:description "Distribution of rank cycle latency"
                                  :quantiles {0.5 0.01 0.75 0.01 0.9 0.01 0.95 0.01 0.99 0.01 0.999 0.01}})
             (prometheus/summary :cook/scheduler-match-cycle-duration-seconds
                                 {:description "Distribution of overall match cycle latency"
                                  :labels [:pool]
                                  :quantiles {0.5 0.01 0.75 0.01 0.9 0.01 0.95 0.01 0.99 0.01 0.999 0.01}})
             (prometheus/summary :cook/scheduler-generate-user-usage-map-duration-seconds
                                 {:description "Distribution of generating user->usage map latency"
                                  :labels [:pool]
                                  :quantiles {0.5 0.01 0.75 0.01 0.9 0.01 0.95 0.01 0.99 0.01 0.999 0.01}})
             (prometheus/summary :cook/scheduler-handle-resource-offers-total-duration-seconds
                                 {:description "Distribution of total handle-resource-offers! duration"
                                  :labels [:pool]
                                  :quantiles {0.5 0.01 0.75 0.01 0.9 0.01 0.95 0.01 0.99 0.01 0.999 0.01}})
             (prometheus/summary :cook/scheduler-handle-resource-offers-pending-to-considerable-duration-seconds
                                 {:description "Distribution of filtering pending to considerable jobs duration"
                                  :labels [:pool]
                                  :quantiles {0.5 0.01 0.75 0.01 0.9 0.01 0.95 0.01 0.99 0.01 0.999 0.01}})
             (prometheus/summary :cook/scheduler-fenzo-schedule-once-duration-seconds
                                 {:description "Distribution of fenzo schedule once latency"
                                  :labels [:pool]
                                  :quantiles {0.5 0.01 0.75 0.01 0.9 0.01 0.95 0.01 0.99 0.01 0.999 0.01}})
             (prometheus/summary :cook/scheduler-handle-resource-offers-match-duration-seconds
                                 {:description "Distribution of matching resource offers to jobs latency"
                                  :labels [:pool]
                                  :quantiles {0.5 0.01 0.75 0.01 0.9 0.01 0.95 0.01 0.99 0.01 0.999 0.01}})
             (prometheus/summary :cook/scheduler-handle-resource-offers-matches-to-job-uuids-duration-seconds
                                 {:description "Distribution of generating matches->job-uuids map latency"
                                  :labels [:pool]
                                  :quantiles {0.5 0.01 0.75 0.01 0.9 0.01 0.95 0.01 0.99 0.01 0.999 0.01}})
             (prometheus/summary :cook/scheduler-launch-all-matched-tasks-total-duration-seconds
                                 {:description "Distribution of total launch all matched tasks latency"
                                  :labels [:pool]
                                  :quantiles {0.5 0.01 0.75 0.01 0.9 0.01 0.95 0.01 0.99 0.01 0.999 0.01}})
             (prometheus/summary :cook/scheduler-launch-all-matched-tasks-transact-duration-seconds
                                 {:description "Distribution of launch all matched tasks--transact in datomic latency"
                                  :labels [:pool]
                                  :quantiles {0.5 0.01 0.75 0.01 0.9 0.01 0.95 0.01 0.99 0.01 0.999 0.01}})
             (prometheus/summary :cook/scheduler-launch-all-matched-tasks-submit-duration-seconds
                                 {:description "Distribution of launch all matched tasks--submit to compute cluster latency"
                                  :labels [:pool]
                                  :quantiles {0.5 0.01 0.75 0.01 0.9 0.01 0.95 0.01 0.99 0.01 0.999 0.01}})
             (prometheus/summary :cook/scheduler-trigger-autoscaling-duration-seconds
                                 {:description "Distribution of trigger autoscaling latency"
                                  :labels [:pool]
                                  :quantiles {0.5 0.01 0.75 0.01 0.9 0.01 0.95 0.01 0.99 0.01 0.999 0.01}})
             (prometheus/summary :cook/scheduler-kill-cancelled-tasks-duration-seconds
                                 {:description "Distribution of kill cancelled tasks latency"
                                  :labels [:pool]
                                  :quantiles {0.5 0.01 0.75 0.01 0.9 0.01 0.95 0.01 0.99 0.01 0.999 0.01}})
             (prometheus/summary :cook/scheduler-sort-jobs-hierarchy-duration-seconds
                                 {:description "Distribution of sorting jobs by DRU latency"
                                  :labels [:pool]
                                  :quantiles {0.5 0.01 0.75 0.01 0.9 0.01 0.95 0.01 0.99 0.01 0.999 0.01}})
             (prometheus/summary :cook/scheduler-filter-offensive-jobs-duration-seconds
                                 {:description "Distribution of filter offensive jobs latency"
                                  :quantiles {0.5 0.01 0.75 0.01 0.9 0.01 0.95 0.01 0.99 0.01 0.999 0.01}})
             (prometheus/summary :cook/scheduler-handle-status-update-duaration-seconds
                                 {:description "Distribution of handle compute cluster status update latency"
                                  :quantiles {0.5 0.01 0.75 0.01 0.9 0.01 0.95 0.01 0.99 0.01 0.999 0.01}})
             (prometheus/summary :cook/scheduler-handle-framework-message-duration-seconds
                                 {:description "Distribution of handle framework message latency"
                                  :quantiles {0.5 0.01 0.75 0.01 0.9 0.01 0.95 0.01 0.99 0.01 0.999 0.01}}))))

(defn get-collector
  "Returns the named collector from the global registry (with the associated labels, if specified)."
  ([name]
   (registry name))
  ([name labels]
   (registry name labels)))

(defmacro with-duration
  "Wraps the given block and records its execution time to the given collector.
  Since thi sfunction requires a collector, use get-collector to call it."
  {:arglists '([collector & body])}
  [collector & body]
  `(prometheus/with-duration ~collector ~@body))

(defn export []
  "Returns the current values of all registered metrics in plain text format."
  (prometheus-export/text-format registry))
