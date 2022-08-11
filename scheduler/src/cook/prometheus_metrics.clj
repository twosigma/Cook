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
            [iapetos.export :as prometheus-export]
            [mount.core :as mount]))

;; Quantiles are specified as a map of quantile to error margin.
(def default-summary-quantiles {0.25 0.01 0.5 0.01 0.75 0.01 0.9 0.005 0.95 0.005 0.98 0.001 0.99 0.001 0.999 0.0001})

;; We define all the metric names here to get IDE support and avoid the chance of runtime
;; errors due to misspelled metric names.
;; We are standardizing the metric format to be :cook/<component>-<metric-name>-<unit>
(def scheduler-rank-cycle-duration :cook/scheduler-rank-cycle-duration-seconds)
(def scheduler-match-cycle-duration :cook/scheduler-match-cycle-duration-seconds)
(def scheduler-generate-user-usage-map-duration :cook/scheduler-generate-user-usage-map-duration-seconds)
(def scheduler-handle-resource-offers-total-duration :cook/scheduler-handle-resource-offers-total-duration-seconds)
(def scheduler-handle-resource-offers-pending-to-considerable-duration :cook/scheduler-handle-resource-offers-pending-to-considerable-duration-seconds)
(def scheduler-fenzo-schedule-once-duration :cook/scheduler-fenzo-schedule-once-duration-seconds)
(def scheduler-handle-resource-offers-match-duration :cook/scheduler-handle-resource-offers-match-duration-seconds)
(def scheduler-handle-resource-offers-matches-to-job-uuids-duration :cook/scheduler-handle-resource-offers-matches-to-job-uuids-duration-seconds)
(def scheduler-launch-all-matched-tasks-total-duration :cook/scheduler-launch-all-matched-tasks-total-duration-seconds)
(def scheduler-launch-all-matched-tasks-transact-duration :cook/scheduler-launch-all-matched-tasks-transact-duration-seconds)
(def scheduler-launch-all-matched-tasks-submit-duration :cook/scheduler-launch-all-matched-tasks-submit-duration-seconds)
(def scheduler-trigger-autoscaling-duration :cook/scheduler-trigger-autoscaling-duration-seconds)
(def scheduler-kill-cancelled-tasks-duration :cook/scheduler-kill-cancelled-tasks-duration-seconds)
(def scheduler-sort-jobs-hierarchy-duration :cook/scheduler-sort-jobs-hierarchy-duration-seconds)
(def scheduler-filter-offensive-jobs-duration :cook/scheduler-filter-offensive-jobs-duration-seconds)
(def scheduler-handle-status-update-duaration :cook/scheduler-handle-status-update-duaration-seconds)
(def scheduler-handle-framework-message-duration :cook/scheduler-handle-framework-message-duration-seconds)
(def scheduler-jobs-launched :cook/scheduler-jobs-launched-total)
(def scheduler-match-cycle-jobs-count :cook/scheduler-match-cycle-jobs-count)
(def scheduler-match-cycle-matched-percent :cook/scheduler-match-cycle-matched-percent)
(def scheduler-match-cycle-head-was-matched :cook/scheduler-match-cycle-head-was-matched)
(def scheduler-match-cycle-queue-was-full :cook/scheduler-match-cycle-queue-was-full)
(def scheduler-match-cycle-all-matched :cook/scheduler-match-cycle-all-matched)
(def user-state-count :cook/scheduler-users-state-count)
;; For user resource metrics, we access them by resource type at runtime, so it is
;; easier to define them all in a map instead of separate vars.
(def resource-metric-map
  {:cpus :cook/scheduler-users-cpu-count
   :mem :cook/scheduler-users-memory-mebibytes
   :jobs :cook/scheduler-users-jobs-count
   :gpus :cook/scheduler-users-gpu-count
   :launch-rate-saved :cook/scheduler-users-launch-rate-saved
   :launch-rate-per-minute :cook/scheduler-users-launch-rate-per-minute})
(def total-synthetic-pods :cook/scheduler-kubernetes-synthetic-pods-count)
(def max-synthetic-pods :cook/scheduler-kubernethes-max-synthetic-pods)


(defn create-registry
  []
  (-> (prometheus/collector-registry)
    (prometheus/register
      ;; Scheduler metrics --------------------------------------------------------------------------------------
      ;; Note that we choose to use a summary instead of a histogram for the latency metrics because we only have
      ;; one scheduler process running per cluster, so we do not need to aggregate data from multiple sources.
      ;; The quantiles are specified as a map of quantile to error margin.
      (prometheus/summary scheduler-rank-cycle-duration
                          {:description "Distribution of rank cycle latency"
                           :quantiles default-summary-quantiles})
      (prometheus/summary scheduler-match-cycle-duration
                          {:description "Distribution of overall match cycle latency"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary scheduler-generate-user-usage-map-duration
                          {:description "Distribution of generating user->usage map latency"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary scheduler-handle-resource-offers-total-duration
                          {:description "Distribution of total handle-resource-offers! duration"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary scheduler-handle-resource-offers-pending-to-considerable-duration
                          {:description "Distribution of filtering pending to considerable jobs duration"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary scheduler-fenzo-schedule-once-duration
                          {:description "Distribution of fenzo schedule once latency"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary scheduler-handle-resource-offers-match-duration
                          {:description "Distribution of matching resource offers to jobs latency"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary scheduler-handle-resource-offers-matches-to-job-uuids-duration
                          {:description "Distribution of generating matches->job-uuids map latency"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary scheduler-launch-all-matched-tasks-total-duration
                          {:description "Distribution of total launch all matched tasks latency"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary scheduler-launch-all-matched-tasks-transact-duration
                          {:description "Distribution of launch all matched tasks--transact in datomic latency"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary scheduler-launch-all-matched-tasks-submit-duration
                          {:description "Distribution of launch all matched tasks--submit to compute cluster latency"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary scheduler-trigger-autoscaling-duration
                          {:description "Distribution of trigger autoscaling latency"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary scheduler-kill-cancelled-tasks-duration
                          {:description "Distribution of kill cancelled tasks latency"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary scheduler-sort-jobs-hierarchy-duration
                          {:description "Distribution of sorting jobs by DRU latency"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary scheduler-filter-offensive-jobs-duration
                          {:description "Distribution of filter offensive jobs latency"
                           :quantiles default-summary-quantiles})
      (prometheus/summary scheduler-handle-status-update-duaration
                          {:description "Distribution of handle compute cluster status update latency"
                           :quantiles default-summary-quantiles})
      (prometheus/summary scheduler-handle-framework-message-duration
                          {:description "Distribution of handle framework message latency"
                           :quantiles default-summary-quantiles})
      (prometheus/counter scheduler-jobs-launched
                        {:description "Total count of jobs launched per pool and compute cluster"
                         :labels [:pool :compute-cluster]})
      ;; Match cycle metrics ------------------------------------------------------------------------------------
      (prometheus/gauge scheduler-match-cycle-jobs-count
                        {:description "Aggregate match cycle job counts stats"
                         :labels [:pool :status]})
      (prometheus/gauge scheduler-match-cycle-matched-percent
                        {:description "Percent of jobs matched in last match cycle"
                         :labels [:pool]})
      ; The follow 1/0 metrics are useful for value map visualizations in Grafana
      (prometheus/gauge scheduler-match-cycle-head-was-matched
                        {:description "1 if head was matched, 0 otherwise"
                         :labels [:pool]})
      (prometheus/gauge scheduler-match-cycle-queue-was-full
                        {:description "1 if queue was full, 0 otherwise"
                         :labels [:pool]})
      (prometheus/gauge scheduler-match-cycle-all-matched
                        {:description "1 if all jobs were matched, 0 otherwise"
                         :labels [:pool]})
      ;; Resource usage stats -----------------------------------------------------------------------------------
      ;; We set these up using a map so we can access them easily by resource type when we set the metric.
      (prometheus/gauge (resource-metric-map :mem)
                        {:description "Current memory by state"
                        :labels [:pool :user :state]})
      (prometheus/gauge (resource-metric-map :cpus)
                        {:description "Current cpu count by state"
                        :labels [:pool :user :state]})
      (prometheus/gauge (resource-metric-map :gpus)
                        {:description "Current gpu count by state"
                        :labels [:pool :user :state]})
      (prometheus/gauge (resource-metric-map :jobs)
                        {:description "Current jobs count by state"
                        :labels [:pool :user :state]})
      (prometheus/gauge (resource-metric-map :launch-rate-saved)
                        {:description "Current launch-rate-saved count by state"
                        :labels [:pool :user :state]})
      (prometheus/gauge (resource-metric-map :launch-rate-per-minute)
                        {:description "Current launch-rate-per-minute count by state"
                        :labels [:pool :user :state]})
      ;; Metrics for user resource allocation counts
      (prometheus/gauge user-state-count
                        {:description "Current user count by state"
                         :labels [:pool :state]})
      ;; Resource usage stats -----------------------------------------------------------------------------------

      ;; Other metrics -------------------------------------------------------------------------------------------
      (prometheus/gauge total-synthetic-pods
                        {:description "Total current number of synthetic pods per pool and compute cluster"
                         :labels [:pool :compute-cluster]})
      (prometheus/gauge max-synthetic-pods
                        {:description "Max number of synthetic pods per pool and compute cluster"
                         :labels [:pool :compute-cluster]}))))

;; A global registry for all metrics reported by Cook.
;; All metrics must be registered before they can be recorded.
(mount/defstate registry :start (create-registry))

(defmacro with-duration
  "Wraps the given block and records its execution time to the collector with the given name.
  If using a collector with no labels, pass {} for the labels value."
  {:arglists '([name labels & body])}
  [name labels & body]
  `(prometheus/with-duration (registry ~name ~labels) ~@body))

(defmacro set
  "Sets the value of the given metric."
  {:arglists '([name amount] [name labels amount])}
  ([name amount]
   `(prometheus/set registry ~name ~amount))
  ([name labels amount]
   `(prometheus/set registry ~name ~labels ~amount)))

(defmacro inc
  "Sets the value of the given metric."
  {:arglists '([name] [name labels])}
  ([name]
   `(prometheus/inc registry ~name))
  ([name labels]
   `(prometheus/inc registry ~name ~labels)))

(defn export []
  "Returns the current values of all registered metrics in plain text format."
  (prometheus-export/text-format registry))
