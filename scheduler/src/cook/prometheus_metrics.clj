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
(def scheduler-pool-handler-pending-to-considerable-duration :cook/scheduler-pool-handler-pending-to-considerable-duration)
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

;; Kubernetes metrics
(def total-pods :cook/scheduler-kubernetes-pods-count)
(def max-pods :cook/scheduler-kubernetes-max-pods)
(def total-synthetic-pods :cook/scheduler-kubernetes-synthetic-pods-count)
(def max-synthetic-pods :cook/scheduler-kubernethes-max-synthetic-pods)
(def synthetic-pods-submitted :cook/scheduler-kubernetes-synthetic-pods-submitted-count)
(def total-nodes :cook/scheduler-kubernetes-nodes-count)
(def max-nodes :cook/scheduler-kubernetes-max-nodes)
(def watch-gap :cook/scheduler-kubernetes-watch-gap-millis)
(def disconnected-watch-gap :cook/scheduler-kubernetes-disconnected-watch-gap-millis)
(def delete-pod-errors :cook/scheduler-kubernetes-delete-pod-errors-count)
(def delete-finalizer-errors :cook/scheduler-kubernetes-delete-finalizer-errors-count)
(def launch-pod-errors :cook/scheduler-launch-pod-errors-count)
(def list-pods-chunk-duration :cook/scheduler-kubernetes-list-pods-chunk-duration-seconds)
(def list-pods-duration :cook/scheduler-kubernetes-list-pods-duration-seconds)
(def list-nodes-duration :cook/scheduler-kubernetes-list-nodes-duration-seconds)
(def delete-pod-duration :cook/scheduler-kubernetes-delete-pod-duration-seconds)
(def delete-finalizer-duration :cook/scheduler-kubernetes-delete-finalizer-duration-seconds)
(def launch-pod-duration :cook/scheduler-kubernetes-launch-pod-duration-seconds)
(def launch-task-duration :cook/scheduler-kubernetes-launch-task-duration-seconds)
(def kill-task-duration :cook/scheduler-kubernetes-kill-task-duration-seconds)
(def compute-pending-offers-duration :cook/scheduler-kubernetes-compute-pending-offers-duration-seconds)
(def autoscale-duration :cook/scheduler-kubernetes-autoscale-duration-seconds)
(def launch-synthetic-tasks-duration :cook/scheduler-kubernetes-launch-synthetic-tasks-duration-seconds)
(def pods-processed-unforced :cook/scheduler-kubernetes-pods-processed-unforced-count)
(def process-lock-duration :cook/scheduler-kubernetes-process-lock-duration-seconds)
(def process-lock-acquire-duration :cook/scheduler-kubernetes-process-lock-acquire-duration-seconds)
(def controller-process-duration :cook/scheduler-kubernetes-controller-process-duration-seconds)
(def handle-pod-update-duration :cook/scheduler-kubernetes-handle-pod-update-duration-seconds)
(def handle-pod-deletion-duration :cook/scheduler-kubernetes-handle-pod-deletion-duration-seconds)
(def update-cook-expected-state-duration :cook/scheduler-kubernetes-update-cook-expected-state-duration-seconds)
(def scan-process-duration :cook/scheduler-kubernetes-scan-process-pod-duration-seconds)
(def pod-waiting-duration :cook/scheduler-kubernetes-pod-duration-until-waiting-seconds)
(def pod-running-duration :cook/scheduler-kubernetes-pod-duration-until-running-seconds)
(def offer-match-timer :cook/scheduler-kubernetes-offer-match-duration-seconds)

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
      (prometheus/summary scheduler-pool-handler-pending-to-considerable-duration
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
      ;; Kubernetes metrics -------------------------------------------------------------------------------------
      (prometheus/gauge total-pods
                        {:description "Total current number of pods per compute cluster"
                         :labels [:pool]})
      (prometheus/gauge max-pods
                        {:description "Max number of pods per compute cluster"
                         :labels [:pool]})
      (prometheus/gauge total-synthetic-pods
                        {:description "Total current number of synthetic pods per pool and compute cluster"
                         :labels [:pool :compute-cluster]})
      (prometheus/gauge max-synthetic-pods
                        {:description "Max number of synthetic pods per pool and compute cluster"
                         :labels [:pool :compute-cluster]})
      (prometheus/gauge synthetic-pods-submitted
                        {:description "Count of synthetic pods submitted in the last match cycle"
                         :labels [:compute-cluster]})
      (prometheus/gauge total-nodes
                        {:description "Total current number of nodes per compute cluster"
                         :labels [:pool]})
      (prometheus/gauge max-nodes
                        {:description "Max number of nodes per compute cluster"
                         :labels [:pool]})
      (prometheus/summary watch-gap
                          {:description "Latency distribution of the gap between last watch response and current response"
                           :labels [:compute-cluster :object]
                           :quantiles default-summary-quantiles})
      (prometheus/summary disconnected-watch-gap
                          {:description "Latency distribution of the gap between last watch response and current response after reconnecting"
                           :labels [:compute-cluster :object]
                           :quantiles default-summary-quantiles})
      (prometheus/counter delete-pod-errors
                          {:description "Total number of errors when deleting pods"
                           :labels [:compute-cluster]})
      (prometheus/counter delete-finalizer-errors
                          {:description "Total number of errors when deleting pod finalizers"
                           :labels [:compute-cluster :type]})
      (prometheus/counter launch-pod-errors
                          {:description "Total number of errors when launching pods"
                           :labels [:compute-cluster :bad-spec]})
      (prometheus/summary list-pods-chunk-duration
                          {:description "Latency distribution of listing a chunk of pods"
                           :labels [:compute-cluster]
                           :quantiles default-summary-quantiles})
      (prometheus/summary list-pods-duration
                          {:description "Latency distribution of listing all pods"
                           :labels [:compute-cluster]
                           :quantiles default-summary-quantiles})
      (prometheus/summary list-nodes-duration
                          {:description "Latency distribution of listing all nodes"
                           :labels [:compute-cluster]
                           :quantiles default-summary-quantiles})
      (prometheus/summary delete-pod-duration
                          {:description "Latency distribution of deleting a pod"
                           :labels [:compute-cluster]
                           :quantiles default-summary-quantiles})
      (prometheus/summary delete-finalizer-duration
                          {:description "Latency distribution of deleting a pod's finalizer"
                           :labels [:compute-cluster]
                           :quantiles default-summary-quantiles})
      (prometheus/summary launch-pod-duration
                          {:description "Latency distribution of launching a pod"
                           :labels [:compute-cluster]
                           :quantiles default-summary-quantiles})
      (prometheus/summary launch-task-duration
                          {:description "Latency distribution of launching a task (more inclusive than launch-pod)"
                           :labels [:compute-cluster]
                           :quantiles default-summary-quantiles})
      (prometheus/summary kill-task-duration
                          {:description "Latency distribution of killing a task (more inclusive than delete-pod)"
                           :labels [:compute-cluster]
                           :quantiles default-summary-quantiles})
      (prometheus/summary compute-pending-offers-duration
                          {:description "Latency distribution of computing pending offers"
                           :labels [:compute-cluster]
                           :quantiles default-summary-quantiles})
      (prometheus/summary autoscale-duration
                          {:description "Latency distribution of autoscaling"
                           :labels [:compute-cluster]
                           :quantiles default-summary-quantiles})
      (prometheus/summary launch-synthetic-tasks-duration
                          {:description "Latency distribution of launching synthetic tasks"
                           :labels [:compute-cluster]
                           :quantiles default-summary-quantiles})
      (prometheus/counter pods-processed-unforced
                          {:description "Count of processed pods"
                           :labels [:compute-cluster]})
      (prometheus/summary process-lock-duration
                          {:description "Latency distribution of processing an event while holding the process lock"
                           :labels [:compute-cluster]
                           :quantiles default-summary-quantiles})
      (prometheus/summary process-lock-acquire-duration
                          {:description "Latency distribution of acquiring the process lock"
                           :labels [:compute-cluster]
                           :quantiles default-summary-quantiles})
      (prometheus/summary controller-process-duration
                          {:description "Latency distribution of processing a pod event"
                           :labels [:compute-cluster :doing-scan]
                           :quantiles default-summary-quantiles})
      (prometheus/summary handle-pod-update-duration
                          {:description "Latency distribution of handling a pod update"
                           :labels [:compute-cluster]
                           :quantiles default-summary-quantiles})
      (prometheus/summary handle-pod-deletion-duration
                          {:description "Latency distribution of handling a pod deletion"
                           :labels [:compute-cluster]
                           :quantiles default-summary-quantiles})
      (prometheus/summary update-cook-expected-state-duration
                          {:description "Latency distribution of updating cook's expected state"
                           :labels [:compute-cluster]
                           :quantiles default-summary-quantiles})
      (prometheus/summary scan-process-duration
                          {:description "Latency distribution of scanning for and processing a pod"
                           :labels [:compute-cluster]
                           :quantiles default-summary-quantiles})
      (prometheus/summary pod-waiting-duration
                          {:description "Latency distribution of the time until a pod is waiting"
                           :labels [:compute-cluster :synthetic]
                           :quantiles default-summary-quantiles})
      (prometheus/summary pod-running-duration
                          {:description "Latency distribution of the time until a pod is running"
                           :labels [:compute-cluster :synthetic]
                           :quantiles default-summary-quantiles})
      (prometheus/summary offer-match-timer
                          {:description "Latency distribution of matching an offer"
                           :labels [:compute-cluster]
                           :quantiles default-summary-quantiles}))))

;; A global registry for all metrics reported by Cook.
;; All metrics must be registered before they can be recorded.
(mount/defstate registry :start (create-registry))

(defmacro with-duration
  "Wraps the given block and records its execution time to the collector with the given name.
  If using a collector with no labels, pass {} for the labels value."
  {:arglists '([name labels & body])}
  [name labels & body]
  `(prometheus/with-duration (registry ~name ~labels) ~@body))

(defmacro start-timer
  "Starts a timer that, when stopped, will store the duration in the given metric.
  The return value will be a function that should be called once the operation to time has run."
  {:arglists '([name] [name labels])}
  ([name]
   `(prometheus/start-timer registry ~name))
  ([name labels]
   `(prometheus/start-timer registry ~name ~labels)))

(defmacro set
  "Sets the value of the given metric."
  {:arglists '([name amount] [name labels amount])}
  ([name amount]
   `(prometheus/set registry ~name ~amount))
  ([name labels amount]
   `(prometheus/set registry ~name ~labels ~amount)))

(defmacro inc
  "Increments the value of the given metric."
  {:arglists '([name] [name labels])}
  ([name]
   `(prometheus/inc registry ~name))
  ([name labels]
   `(prometheus/inc registry ~name ~labels)))

(defmacro observe
  "Records the value for the given metric (for histograms and summaries)."
  {:arglists '([name amount] [name labels amount])}
  ([name amount]
   `(prometheus/observe registry ~name ~amount))
  ([name labels amount]
   `(prometheus/observe registry ~name ~labels ~amount)))

(defn export []
  "Returns the current values of all registered metrics in plain text format."
  (prometheus-export/text-format registry))
