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
(ns cook.mesos.optimizer
  (:require [cheshire.core :as cheshire]
            [chime :refer [chime-at chime-ch]]
            [clj-http.client :as http]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]
            [clojure.tools.logging :as log]
            [cook.mesos.util :as util]
            [cook.util :refer [NonNegInt PosNum PosInt lazy-load-var]]
            [metrics.timers :as timers]
            [plumbing.core :as pc]
            [schema.core :as s])
  (:import (java.util UUID)))

(s/defschema TimePeriodMs NonNegInt)

(s/defschema HostInfo
  "Example:
   {:count 4
    :cpus 8
    :instance-type \"basic\"
    :mem 240000}"
  {(s/required-key :count) NonNegInt
   (s/required-key :cpus) PosNum
   (s/required-key :instance-type) s/Str
   (s/required-key :mem) PosNum
   (s/optional-key :attributes) {s/Keyword s/Any}
   (s/optional-key :gpus) PosNum
   (s/optional-key :time-to-start) TimePeriodMs})

(s/defschema Schedule
  "Schedule returned by optimizer.
   Each TimePeriodMs is some milliseconds into the future the optimizer recommendation map applies.
   The optimizer recommendation map currently only has one key,
   :suggested-matches which has the matches recommended as a value.
   The matches recommended are structured as a map from the host type in question to a list of job UUIDs
   the optimizer recommends scheduling.

   Example schedule:
   {0 {:suggested-matches [#uuid \"1db521d2-b1d5-41b4-9a93-a7e12abd940b\"]}
    60000 {:suggested-matches [#uuid \"724521d2-b1d5-41b4-9a93-a7e12abd940b\"]}}"
  {TimePeriodMs {(s/required-key :suggested-matches) [s/Uuid]}})

(defprotocol Optimizer
  "Protocol defining a tool to produce a schedule to execute"
  (produce-schedule
    [this queue running]
    "Returns a schedule of what jobs to place on what machines
     and what hosts to purchase at different time steps.
     The returned value must conform to the Schedule schema above.

     Parameters:
     queue -- ordered sequence of jobs to run (see cook.mesos.schema job)
     running -- sequence of running tasks (see cook.mesos.schema instance)

     Returns:
     A schedule: a map where the keys are milliseconds in the future and
     the values are recommendations to take at that point in the future."))

(defn create-dummy-optimizer
  "Returns an instance of Optimizer which returns an empty schedule."
  [_]
  (log/info "Creating dummy optimizer")
  (reify Optimizer
    (produce-schedule [_ _ _] {0 {:suggested-matches []}})))

(defn instance-runtime
  "Returns the instance runtime."
  [instance now]
  (let [start (:instance/start-time instance)
        end (:instance/end-time instance now)]
    (- (.getTime end)
       (.getTime start))))

(defn calculate-expected-complete-time
  "Returns the expected time remaining to job completion.
   Simple model for if run time exceeds expected runtime: the more it exceeds expected, the longer we expect it to take.
   TODO: Issue #484 get better model!"
  [instance expected-duration now]
  (->> (instance-runtime instance now)
       (- expected-duration)
       Math/abs))

(defn job->batch-uuid
  "Retrieves a batch UUID for the provided job."
  [job]
  (or (-> job :group/_job :group/uuid)
      (:job/uuid job)))

(defn waiting-job->optimizer-job
  "Converts a waiting job to an optimizer job representation."
  [job default-runtime]
  (let [expected-duration (get job :job/expected-runtime default-runtime)
        resources (util/job-ent->resources job)
        batch-uuid (job->batch-uuid job)]
    (merge {:batch batch-uuid
            :count 1
            :dur expected-duration
            :expected_complete_time -1
            :running_host_group -1 ; we do not support multiple host groups currently
            :state "waiting"
            :submit_time (-> job :job/submit-time .getTime)
            :user (:job/user job)
            :uuid (:job/uuid job)}
           resources)))

(defn running-instance->optimizer-job
  "Converts a running instance to an optimizer job representation."
  [instance default-runtime now host-name->host-group]
  (let [job (:job/_instance instance)
        expected-duration (get job :job/expected-runtime default-runtime)
        expected-complete-time (calculate-expected-complete-time instance expected-duration now)
        resources (util/job-ent->resources job)
        batch-uuid (job->batch-uuid job)]
    (merge {:batch batch-uuid
            :count 1
            :dur expected-duration
            :expected_complete_time expected-complete-time
            :running_host_group (-> instance :instance/hostname host-name->host-group)
            :state "running"
            :submit_time (-> job :job/submit-time .getTime)
            :user (:job/user job)
            :uuid (:job/uuid job)}
           resources)))

;; TODO Issue #485 remote optimizer should support cpu-share and mem-share computed per user.
(defn create-optimizer-server
  "Returns an instance of Optimizer which returns the schedule received from sending the request
   to a remote server. The input configuration has the following keys:
   cpu-share: the global cpu share of each user,
   default-runtime: the default expected runtime of a job,
   host-info: the homogeneous profile of the cluster we are targeting,
   max-waiting: the maximum number of waiting jobs to send to the optimizer,
   mem-share: the global mem share of each user,
   opt-params: a map with string keys that are passed to the optimizer as additional configuration parameters,
   opt-server: the url for the remote optimizer server,
   step-list: the list of future run times (s.g. starting at 0) we want the optimizer to compute for."
  [{:keys [cpu-share default-runtime host-info max-waiting mem-share opt-params opt-server step-list]
    :or {opt-params {}}
    :as config}]
  {:pre [(integer? default-runtime)
         (pos? default-runtime)
         (s/validate HostInfo host-info)
         (integer? max-waiting)
         (pos? max-waiting)
         (seq step-list)]}
  (log/info "Optimizer config" config)
  (let [host-group 0
        host-name->host-group (constantly host-group)
        host-group-descriptions [host-info]
        ;; exponential moving average (ema) of memory usage based on running tasks of users
        user->ema-mem-usage (atom {})]
    (reify Optimizer
      (produce-schedule [_ queued-jobs running-tasks]
        (let [waiting-jobs (take max-waiting queued-jobs)]
          (log/info "Optimizer inputs" {:running-tasks (count running-tasks)
                                        :waiting-jobs (count waiting-jobs)})
          (if (-> waiting-jobs count pos?)
            (let [alpha 0.5
                  now (tc/to-date (t/now))
                  _ (log/info "Converting waiting jobs to optimizer jobs")
                  waiting-opt-jobs (timers/time!
                                     (timers/timer ["cook-mesos" "scheduler" "optimizer" "waiting-jobs"])
                                     (->> waiting-jobs
                                          (take max-waiting)
                                          (mapv #(waiting-job->optimizer-job % default-runtime))))
                  _ (log/info "Converting running jobs to optimizer jobs")
                  running-opt-jobs (timers/time!
                                     (timers/timer ["cook-mesos" "scheduler" "optimizer" "running-jobs"])
                                     (->> running-tasks
                                          (mapv #(running-instance->optimizer-job % default-runtime now host-name->host-group))))
                  _ (log/info "Computing user memory usage")
                  user->mem-usage (timers/time!
                                    (timers/timer ["cook-mesos" "scheduler" "optimizer" "user->mem-usage"])
                                    (->> running-opt-jobs
                                         (group-by :user)
                                         (pc/map-vals #(reduce + 0 (map :mem %)))
                                         (pc/map-vals #(* (- 1 alpha) %))))
                  _ (swap! user->ema-mem-usage
                           (fn [ema-usage]
                             (->> (pc/map-vals (partial * alpha) ema-usage)
                                  (merge-with + user->mem-usage))))
                  ;; Running must be before waiting here because optimizer determines batch order from job order
                  opt-jobs (concat running-opt-jobs waiting-opt-jobs)
                  user->ema-usage (pc/map-vals #(/ % mem-share) @user->ema-mem-usage)
                  _ (log/info "Preparing request body")
                  request-body (timers/time!
                                 (timers/timer ["cook-mesos" "scheduler" "optimizer" "prepare-request"])
                                 (cheshire/generate-string
                                   {"cpu_share" cpu-share
                                    "host_groups" host-group-descriptions
                                    "mem_share" mem-share
                                    "now" (.getTime now)
                                    "opt_jobs" opt-jobs
                                    "opt_params" opt-params
                                    "seed" (.getTime now)
                                    "step_list" step-list
                                    "user_to_ema_usage" user->ema-usage}))
                  _ (log/info "Making request to optimizer")
                  raw-schedule (timers/time!
                                 (timers/timer ["cook-mesos" "scheduler" "optimizer" "server"])
                                 (-> opt-server
                                     (http/post {:as :json-string-keys
                                                 :body request-body
                                                 :content-type :json})
                                     :body))
                  _ (log/info "Received optimizer schedule")
                  scheduled-job-ids (->> (get-in raw-schedule ["schedule" "0" "suggested-matches" (str host-group)] [])
                                         (map #(UUID/fromString %)))]
              (log/info "Num jobs scheduled immediately:" (count scheduled-job-ids))
              {0 {:suggested-matches scheduled-job-ids}})
            {0 {:suggested-matches []}}))))))

(defn pool-optimizer-cycle
  "Starts a cycle that:
   1. Gets queue, running, offer and purchasable host info
   2. Calls the `optimizer` to get a schedule

   Parameters:
   get-queue -- fn, 1-args fn that returns ordered list of jobs to run for provided pool-name
   get-running -- fn, 1-args fn that returns a set of tasks running for provided pool-name
   optimizer -- instance of Optimizer

   Raises an exception if there was a problem in the execution"
  [pool-name get-queue get-running optimizer]
  (let [queue (future (get-queue pool-name))
        running (future (get-running pool-name))
        schedule (produce-schedule optimizer @queue @running)]
    (s/validate Schedule schedule)
    schedule))

(defn optimizer-cycle!
  "Run the pool-optimizer-cycle for every pool and populates the result into pool-name->optimizer-suggested-job-ids-atom."
  [optimizer get-pool-names pool-name->queue pool-name->running pool-name->optimizer-suggested-job-ids-atom]
  (doseq [pool-name (get-pool-names)]
    (try
      (log/info "Starting optimization cycle for pool" pool-name)
      (let [scheduler (pool-optimizer-cycle pool-name pool-name->queue pool-name->running optimizer)
            job-ids (get-in scheduler [0 :suggested-matches] [])]
        (swap! pool-name->optimizer-suggested-job-ids-atom assoc pool-name job-ids))
      (catch Exception ex
        (log/warn ex "Error running optimizer cycle for pool" pool-name)))))

(defn start-optimizer-cycles!
  "Every interval, call `optimizer-cycle!`.
   Returns a function of no arguments to stop"
  [optimizer-config trigger-chan get-pool-names pool-name->queue pool-name->running
   pool-name->optimizer-suggested-job-ids-atom]
  (log/info "Starting optimization cycles")
  (let [construct (fn construct-optimizer [{:keys [create-fn config]}]
                    ((lazy-load-var create-fn) config))
        optimizer (-> optimizer-config :optimizer construct)]
    (log/info "Optimizer constructed")
    (util/chime-at-ch trigger-chan
                      (fn []
                        (optimizer-cycle! optimizer get-pool-names pool-name->queue pool-name->running
                                          pool-name->optimizer-suggested-job-ids-atom))
                      {:error-handler (fn [e] (log/warn e "Error running optimizer cycle"))})))
