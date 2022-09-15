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
  (:require [iapetos.collector.jvm :as jvm]
            [iapetos.collector.ring :as ring]
            [iapetos.core :as prometheus]
            [iapetos.export :as prometheus-export]
            [mount.core :as mount]))

;; Quantiles are specified as a map of quantile to error margin.
(def default-summary-quantiles {0.25 0.01 0.5 0.01 0.75 0.01 0.9 0.005 0.95 0.005 0.98 0.001 0.99 0.001 0.999 0.0001})

;; We define all the metric names here to get IDE support and avoid the chance of runtime
;; errors due to misspelled metric names.
;; We are standardizing the metric format to be :cook/<component>-<metric-name>-<unit>

;; Scheduler metrics
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
(def scheduler-schedule-jobs-on-kubernetes-duration :cook/scheduler-schedule-jobs-on-kubernetes-duration-seconds)
(def scheduler-distribute-jobs-for-kubernetes-duration :cook/scheduler-distribute-jobs-for-kubernetes-duration-seconds)
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
(def init-user-to-dry-divisors-duration :cook/scheduler-init-user-to-dru-divisors-duration-seconds)
(def generate-sorted-task-scored-task-pairs-duration :cook/scheduler-generate-sorted-task-scored-task-duration-seconds)
(def get-shares-duration :cook/scheduler-get-shares-duration-seconds)
(def create-user-to-share-fn-duration :cook/scheduler-create-user-to-share-fn-duration-seconds)
(def task-failure-reasons :cook/scheduler-task-failures-by-reason)
(def iterations-at-fenzo-floor :cook/scheduler-iterations-at-fenzo-floor-count)
(def in-order-queue-count :cook/scheduler-in-order-queue-count)
(def task-times-by-status :cook/scheduler-task-runtimes-by-status)
(def number-offers-matched :cook/scheduler-number-offers-matched-distribution)
(def fraction-unmatched-jobs :cook/scheduler-fraction-unmatched-jobs)
(def offer-size-by-resource :cook/scheduler-offer-size-by-resource)
(def task-completion-rate :cook/scheduler-task-completion-rate)
(def task-completion-rate-by-resource :cook/scheduler-task-completion-rate-by-resource)
(def transact-report-queue-datoms :cook/scheduler-transact-report-queue-datoms-count)
(def transact-report-queue-update-job-state :cook/scheduler-transact-report-queue-update-job-state-count)
(def transact-report-queue-job-complete :cook/scheduler-transact-report-queue-job-complete-count)
(def transact-report-queue-tasks-killed :cook/scheduler-transact-report-queue-tasks-killed-count)
(def scheduler-offers-declined :cook/scheduler-offers-declined-count)
(def scheduler-handle-resource-offer-errors :cook/scheduler-handle-resource-offer-errors-count)
(def scheduler-matched-resource-counts :cook/scheduler-matched-resource-count)
(def scheduler-matched-tasks :cook/scheduler-matched-tasks-count)
(def scheduler-abandon-and-reset :cook/scheduler-abandon-and-reset-count)
(def scheduler-rank-job-failures :cook/scheduler-rank-job-failures)
(def scheduler-offer-channel-full-error :cook/scheduler-offer-channel-full-error)
(def scheduler-schedule-jobs-event-duration :cook/scheduler-schedule-jobs-event-duration-seconds)
(def match-jobs-event-duration :cook/scheduler-match-jobs-event-duration-seconds)
(def in-order-queue-delay-duration :cook/scheduler-in-order-queue-delay-duration-seconds)

;; Monitor / user resource metrics
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
(def resource-capacity :cook/scheduler-kubernetes-resource-capacity)
(def resource-consumption :cook/scheduler-kubernetes-resource-consumption)

;; Mesos metrics
(def mesos-heartbeats :cook/scheduler-mesos-heartbeats-count)
(def mesos-heartbeat-timeouts :cook/scheduler-mesos-heartbeat-timeouts-count)
(def mesos-datomic-sync-duration :cook/scheduler-mesos-heartbeat-datomic-sync-duration-seconds)
(def mesos-offer-chan-depth :cook/scheduler-mesos-offer-chan-depth)
(def mesos-error :cook/scheduler-mesos-error-count)
(def mesos-handle-framework-message :cook/scheduler-mesos-handle-framework-message)
(def mesos-handle-status-update :cook/scheduler-mesos-handle-status-update)
(def mesos-tasks-killed-in-status-update :cook/scheduler-mesos-tasks-killed-in-status-update-count)
(def mesos-aggregator-pending-count :cook/scheduler-mesos-aggregator-pending-count)
(def mesos-pending-sync-host-count :cook/scheduler-mesos-pending-sync-host-count)
(def mesos-updater-unprocessed-count :cook/scheduler-mesos-field-updater-unprocessed-count)
(def mesos-aggregator-message :cook/scheduler-mesos-field-aggregator-message-count)
(def mesos-updater-publish-duration :cook/scheduler-mesos-field-updater-publish-duration-seconds)
(def mesos-updater-transact-duration :cook/scheduler-mesos-field-updater-transact-duration-seconds)
(def mesos-updater-pending-entries :cook/scheduler-mesos-field-updater-pending-entries-distribution)
(def mesos-updater-unprocessed-entries :cook/scheduler-mesos-unprocessed-entries-distribution)

;; API metrics
(def jobs-created :cook/api-jobs-created)
(def list-request-param-time-range :cook/api-list-request-param-time-range-millis)
(def list-request-param-limit :cook/api-list-request-param-limit-number)
(def list-response-job-count :cook/api-list-request-job-count)
(def fetch-instance-map-duration :cook/api-internal-fetch-instance-map-duration-seconds)
(def fetch-job-map-duration :cook/api-internal-fetch-job-map-duration-seconds)
(def fetch-jobs-duration :cook/api-internal-fetch-jobs-duration-seconds)
(def list-jobs-duration :cook/api-internal-list-jobs-duration-seconds)
(def endpoint-duration :cook/api-endpoint-duration-seconds)

;; Tools metrics
(def get-jobs-by-user-and-state-duration :cook/tools-get-jobs-by-user-duration-seconds)
(def get-jobs-by-user-and-state-total-duration :cook/tools-get-jobs-by-user-and-states-duration-seconds)
(def get-all-running-tasks-duration :cook/tools-get-all-running-tasks-duration-seconds)
(def get-user-running-jobs-duration :cook/tools-get-user-running-jobs-duration-seconds)
(def get-all-running-jobs-duration :cook/tools-get-all-running-jobs-duration-seconds)

;; Plugin metrics
(def pool-mover-jobs-updated :cook/scheduler-plugins-pool-mover-jobs-updated-count)

;; Rebalancer metrics
(def compute-preemption-decision-duration :cook/rebalancer-compute-premeption-decision-duration-seconds)
(def rebalance-duration :cook/rebalancer-rebalance-duration-seconds)
(def pending-job-drus :cook/rebalancer-pending-job-drus)
(def nearest-task-drus :cook/rebalancer-nearest-task-drus)
(def positive-dru-diffs :cook/rebalancer-positive-dru-diffs)
(def preemption-counts-for-host :cook/rebalancer-preemption-counts-for-host)
(def task-counts-to-preempt :cook/rebalancer-task-counts-to-preempt)
(def job-counts-to-run :cook/rebalancer-job-counts-to-run)

;; Progress metrics
(def progress-aggregator-drop-count :cook/progress-aggregator-drop-count)
(def progress-aggregator-pending-states-count :cook/progress-aggregator-pending-states-count)
(def progress-updater-pending-states :cook/progress-updater-pending-states)
(def progress-aggregator-message-count :cook/progress-aggregator-message-count)
(def progress-updater-publish-duration :cook/progress-updater-publish-duration-seconds)
(def progress-updater-transact-duration :cook/progress-updater-transact-duration-seconds)

;; Other metrics
(def is-leader :cook/scheduler-is-leader)
(def update-queue-lengths-duration :cook/scheduler-update-queue-lengths-duration-seconds)
(def acquire-kill-lock-for-kill-duration :cook/scheduler-acquire-kill-lock-for-kill-duration-seconds)
(def get-pending-jobs-duration :cook/scheduler-get-pending-jobs-duration-seconds)

(defn create-registry
  []
  (-> (prometheus/collector-registry)
    ;; Initialize default JVM metrics
    (jvm/initialize)
    ;; Initialize ring metrics
    (ring/initialize)
    (prometheus/register
      ;; Scheduler metrics ---------------------------------------------------------------------------------------------
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
      (prometheus/summary scheduler-schedule-jobs-on-kubernetes-duration
                          {:description "Distribution of scheduling jobs on Kubernetes latency"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary scheduler-distribute-jobs-for-kubernetes-duration
                          {:description "Distribution of distributing jobs for Kubernetes latency"
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
      (prometheus/summary init-user-to-dry-divisors-duration
                          {:description "Latency distribution of initializing the user to dru divisors map"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary generate-sorted-task-scored-task-pairs-duration
                          {:description "Latency distribution of generating the sorted list of task and scored task pairs"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary get-shares-duration
                          {:description "Latency distribution of getting all users' share"
                           :quantiles default-summary-quantiles})
      (prometheus/summary create-user-to-share-fn-duration
                          {:description "Latency distribution of creating the user-to-share function"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary task-times-by-status
                          {:description "Distribution of task runtime by status"
                           :labels [:status]
                           :quantiles default-summary-quantiles})
      (prometheus/summary number-offers-matched
                          {:description "Distribution of number of offers matched"
                           :labels [:pool :compute-cluster]
                           :quantiles default-summary-quantiles})
      (prometheus/summary fraction-unmatched-jobs
                          {:description "Distribution of fraction of unmatched jobs"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary offer-size-by-resource
                          {:description "Distribution of offer size by resource type"
                           :labels [:pool :resource]
                           :quantiles default-summary-quantiles})
      (prometheus/counter task-completion-rate
                          {:description "Total count of completed tasks per pool"
                           :labels [:pool :status]})
      (prometheus/counter task-completion-rate-by-resource
                          {:description "Total count of completed resources per pool"
                           :labels [:pool :status :resource]})
      (prometheus/counter transact-report-queue-datoms
                          {:description "Total count of report queue datoms"})
      (prometheus/counter transact-report-queue-update-job-state
                          {:description "Total count of job state updates"})
      (prometheus/counter transact-report-queue-job-complete
                          {:description "Total count of completed jobs"})
      (prometheus/counter transact-report-queue-tasks-killed
                          {:description "Total count of tasks killed"})
      (prometheus/counter scheduler-offers-declined
                          {:description "Total offers declined"
                           :labels [:compute-cluster]})
      (prometheus/counter scheduler-matched-resource-counts
                          {:description "Total matched count per resource type"
                           :labels [:pool :resource]})
      (prometheus/counter scheduler-matched-tasks
                          {:description "Total matched tasks"
                           :labels [:pool :compute-cluster]})
      (prometheus/counter scheduler-handle-resource-offer-errors
                          {:descrpiption "Total count of errors encountered in handle-resource-offer!"
                           :labels [:pool]})
      (prometheus/counter scheduler-abandon-and-reset
                          {:descrpiption "Total count of fenzo abandon-and-reset"
                           :labels [:pool]})
      (prometheus/counter scheduler-rank-job-failures
                          {:descrpiption "Total count of rank job failures"})
      (prometheus/counter scheduler-offer-channel-full-error
                          {:descrpiption "Total count of offer channel full failures"
                           :labels [:pool]})
      (prometheus/summary scheduler-schedule-jobs-event-duration
                          {:description "Latency distribution of scheduling jobs in Kubernetes in the full Kenzo codepath"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary match-jobs-event-duration
                          {:description "Latency distribution of matching jobs in the full Fenzo codepath"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary in-order-queue-delay-duration
                          {:description "Latency distribution of processing in-order-queue tasks"
                           :quantiles default-summary-quantiles})
      ;; Match cycle metrics -------------------------------------------------------------------------------------------
      (prometheus/gauge scheduler-match-cycle-jobs-count
                        {:description "Aggregate match cycle job counts stats"
                         :labels [:pool :status]})
      (prometheus/gauge scheduler-match-cycle-matched-percent
                        {:description "Percent of jobs matched in last match cycle"
                         :labels [:pool]})
      ; The following 1/0 metrics are useful for value map visualizations in Grafana
      (prometheus/gauge scheduler-match-cycle-head-was-matched
                        {:description "1 if head was matched, 0 otherwise"
                         :labels [:pool]})
      (prometheus/gauge scheduler-match-cycle-queue-was-full
                        {:description "1 if queue was full, 0 otherwise"
                         :labels [:pool]})
      (prometheus/gauge scheduler-match-cycle-all-matched
                        {:description "1 if all jobs were matched, 0 otherwise"
                         :labels [:pool]})
      (prometheus/summary task-failure-reasons
                          {:description "Distribution of task failures by reason"
                           :labels [:reason :resource]
                           :quantiles default-summary-quantiles})
      (prometheus/gauge iterations-at-fenzo-floor
                        {:descriptiion "Current number of iterations at fenzo floor (i.e. 1 considerable job)"
                         :labels [:pool]})
      (prometheus/gauge in-order-queue-count
                        {:description "Depth of queue for in-order processing"})
      ;; Resource usage stats ------------------------------------------------------------------------------------------
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
      ;; Kubernetes metrics --------------------------------------------------------------------------------------------
      (prometheus/gauge total-pods
                        {:description "Total current number of pods per compute cluster"
                         :labels [:compute-cluster]})
      (prometheus/gauge max-pods
                        {:description "Max number of pods per compute cluster"
                         :labels [:compute-cluster]})
      (prometheus/gauge total-synthetic-pods
                        {:description "Total current number of synthetic pods per pool and compute cluster"
                         :labels [:pool :compute-cluster]})
      (prometheus/gauge max-synthetic-pods
                        {:description "Max number of synthetic pods per pool and compute cluster"
                         :labels [:pool :compute-cluster]})
      (prometheus/gauge synthetic-pods-submitted
                        {:description "Count of synthetic pods submitted in the last match cycle"
                         :labels [:compute-cluster :pool]})
      (prometheus/gauge total-nodes
                        {:description "Total current number of nodes per compute cluster"
                         :labels [:compute-cluster]})
      (prometheus/gauge max-nodes
                        {:description "Max number of nodes per compute cluster"
                         :labels [:compute-cluster]})
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
                           :labels [:compute-cluster :pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary launch-synthetic-tasks-duration
                          {:description "Latency distribution of launching synthetic tasks"
                           :labels [:compute-cluster :pool]
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
                           :labels [:compute-cluster :synthetic :kubernetes-scheduler-pod]
                           :quantiles default-summary-quantiles})
      (prometheus/summary pod-running-duration
                          {:description "Latency distribution of the time until a pod is running"
                           :labels [:compute-cluster :synthetic :kubernetes-scheduler-pod]
                           :quantiles default-summary-quantiles})
      (prometheus/summary offer-match-timer
                          {:description "Latency distribution of matching an offer"
                           :labels [:compute-cluster]
                           :quantiles default-summary-quantiles})
      (prometheus/gauge resource-capacity
                        {:description "Total available capacity of the given resource per cluster and pool"
                         :labels [:compute-cluster :pool :resource :resource-subtype]})
      (prometheus/gauge resource-consumption
                        {:description "Total consumption of the given resource per cluster"
                         :labels [:compute-cluster :resource :resource-subtype]})
      ;; Mesos metrics -------------------------------------------------------------------------------------------------
      (prometheus/counter mesos-heartbeats
                          {:description "Count of mesos heartbeats"})
      (prometheus/counter mesos-heartbeat-timeouts
                          {:description "Count of mesos heartbeat timeouts"})
      (prometheus/summary mesos-datomic-sync-duration
                          {:description "Latency distribution of mesos datomic sync duration"
                           :quantiles default-summary-quantiles})
      (prometheus/gauge mesos-offer-chan-depth
                          {:description "Depth of mesos offer channel"
                           :labels [:pool]})
      (prometheus/counter mesos-error
                          {:description "Count of errors in mesos"})
      (prometheus/counter mesos-handle-framework-message
                          {:description "Count of framework messages received in mesos"})
      (prometheus/counter mesos-handle-status-update
                          {:description "Count of status updates received in mesos"})
      (prometheus/counter mesos-tasks-killed-in-status-update
                          {:description "Count of tasks killed during status updates in mesos"})
      (prometheus/gauge mesos-aggregator-pending-count
                          {:description "Count of pending entries in the aggregator"
                           :labels [:field-name]})
      (prometheus/gauge mesos-pending-sync-host-count
                          {:description "Count of pending sync hosts"})
      (prometheus/gauge mesos-updater-unprocessed-count
                          {:description "Count of unprocessed tasks in mesos"
                           :labels []})
      (prometheus/summary mesos-updater-unprocessed-entries
                          {:description "Distribution of count of unprocessed entries"
                           :quantiles default-summary-quantiles})
      (prometheus/summary mesos-updater-pending-entries
                          {:description "Distribution of count of pending entries"
                           :quantiles default-summary-quantiles})
      (prometheus/counter mesos-aggregator-message
                          {:description "Count of messages received by the aggregator"
                           :labels [:field-name]})
      (prometheus/summary mesos-updater-publish-duration
                          {:description "Latency distribution of mesos updater publish duration"
                           :labels [:field-name]
                           :quantiles default-summary-quantiles})
      (prometheus/summary mesos-updater-transact-duration
                          {:description "Latency distribution of mesos updater transact duration"
                           :labels [:field-name]
                           :quantiles default-summary-quantiles})
      ;; API metrics ---------------------------------------------------------------------------------------------------
      (prometheus/counter jobs-created
                          {:description "Total count of jobs created"
                           :labels [:pool]})
      (prometheus/summary list-request-param-time-range
                          {:description "Distribution of time range specified in list endpoint requests"
                           :quantiles default-summary-quantiles})
      (prometheus/summary list-request-param-limit
                          {:description "Distribution of instance count limit specified in list endpoint requests"
                           :quantiles default-summary-quantiles})
      (prometheus/summary list-response-job-count
                          {:description "Distribution of instance count returned in list endpoint responses"
                           :quantiles default-summary-quantiles})
      (prometheus/summary fetch-instance-map-duration
                          {:description "Latency distribution of converting the instance entity to a map for API responses"
                           :quantiles default-summary-quantiles})
      (prometheus/summary fetch-job-map-duration
                          {:description "Latency distribution of converting the job entity to a map for API responses"
                           :quantiles default-summary-quantiles})
      (prometheus/summary fetch-jobs-duration
                          {:description "Latency distribution of fetching jobs by user and state for API responses"
                           :quantiles default-summary-quantiles})
      (prometheus/summary list-jobs-duration
                          {:description "Latency distribution of listing jobs for API responses"
                           :quantiles default-summary-quantiles})
      (prometheus/summary endpoint-duration
                          {:description "Latency distribution of API endpoints"
                           :labels [:endpoint]
                           :quantiles default-summary-quantiles})
      ;; Tools metrics -------------------------------------------------------------------------------------------------
      (prometheus/summary get-jobs-by-user-and-state-duration
                          {:description "Latency distribution of getting jobs by user for a particular state"
                           :labels [:state]
                           :quantiles default-summary-quantiles})
      (prometheus/summary get-jobs-by-user-and-state-total-duration
                          {:description "Latency distribution of getting jobs by user for a list of states"
                           :quantiles default-summary-quantiles})
      (prometheus/summary get-all-running-tasks-duration
                          {:description "Latency distribution of getting all running tasks"
                           :quantiles default-summary-quantiles})
      (prometheus/summary get-user-running-jobs-duration
                          {:description "Latency distribution of getting running jobs for a particular user"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary get-all-running-jobs-duration
                          {:description "Latency distribution of getting all running jobs"
                           :quantiles default-summary-quantiles})
      ;; Plugins metrics -----------------------------------------------------------------------------------------------
      (prometheus/counter pool-mover-jobs-updated
                          {:description "Total count of jobs moved to a different pool"})
      ;; Rebalancer metrics --------------------------------------------------------------------------------------------
      (prometheus/summary compute-preemption-decision-duration
                          {:description "Latency distribution of computing preemption decision"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary rebalance-duration
                          {:description "Latency distribution of rebalancing"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary pending-job-drus
                          {:description "Distribution of pending jobs drus in the rebalancer"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary nearest-task-drus
                          {:description "Distribution of nearest task drus in the rebalancer"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary positive-dru-diffs
                          {:description "Distribution of positive dru diffs in the rebalancer"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary preemption-counts-for-host
                          {:description "Distribution of preemption counts per host in the rebalancer"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary task-counts-to-preempt
                          {:description "Distribution of number of tasks to preempt in the rebalancer"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      (prometheus/summary job-counts-to-run
                          {:description "Distribution of number of jobs to run in the rebalancer"
                           :labels [:pool]
                           :quantiles default-summary-quantiles})
      ;; Progress metrics ----------------------------------------------------------------------------------------------
      (prometheus/counter progress-aggregator-drop-count
                          {:description "Total count of dropped progress messages"})
      (prometheus/counter progress-aggregator-message-count
                          {:description "Total count of received progress messages"})
      (prometheus/gauge progress-aggregator-pending-states-count
                          {:description "Total count of pending states"})
      (prometheus/summary progress-updater-pending-states
                          {:description "Distribution of pending states count in the progress updater"
                           :quantiles default-summary-quantiles})
      (prometheus/summary progress-updater-publish-duration
                          {:description "Latency distribution of the publish function in the progress updater"
                           :quantiles default-summary-quantiles})
      (prometheus/summary progress-updater-transact-duration
                          {:description "Latency distribution of the transact function in the progress updater"
                           :quantiles default-summary-quantiles})
      ;; Other metrics -------------------------------------------------------------------------------------------------
      (prometheus/gauge is-leader
                        {:description "1 if this host is the current leader, 0 otherwise"})
      (prometheus/summary update-queue-lengths-duration
                          {:description "Latency distribution of updating queue lengths from the database"
                           :quantiles default-summary-quantiles})
      (prometheus/summary acquire-kill-lock-for-kill-duration
                          {:description "Latency distribution of acquiring the kill lock for kill"
                           :quantiles default-summary-quantiles})
      (prometheus/summary get-pending-jobs-duration
                          {:description "Latency distribution of getting all pending jobs"
                           :quantiles default-summary-quantiles}))))


;; A global registry for all metrics reported by Cook.
;; All metrics must be registered before they can be recorded.
(mount/defstate registry :start (create-registry))

(defmacro value
  "Get the value of the given metric."
  {:arglists '([name] [name labels])}
  ([name]
   `(prometheus/value registry ~name))
  ([name labels]
   `(prometheus/value registry ~name ~labels)))

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
  {:arglists '([name] [name labels] [name labels amount])}
  ([name]
   `(prometheus/inc registry ~name))
  ([name labels]
   `(prometheus/inc registry ~name ~labels))
  ([name labels amount]
   `(prometheus/inc registry ~name ~labels ~amount)))

(defmacro dec
  "Decrements the value of the given metric."
  {:arglists '([name] [name labels])}
  ([name]
   `(prometheus/dec registry ~name))
  ([name labels]
   `(prometheus/dec registry ~name ~labels))
  ([name labels amount]
   `(prometheus/dec registry ~name ~labels ~amount)))

(defmacro observe
  "Records the value for the given metric (for histograms and summaries)."
  {:arglists '([name amount] [name labels amount])}
  ([name amount]
   `(prometheus/observe registry ~name ~amount))
  ([name labels amount]
   `(prometheus/observe registry ~name ~labels ~amount)))

(defmacro wrap-ring-instrumentation
  "Wraps the given Ring handler to write metrics to the given registry."
  [handler options]
  `(ring/wrap-instrumentation ~handler registry ~options))

(defn export []
  "Returns the current values of all registered metrics in plain text format."
  (prometheus-export/text-format registry))
