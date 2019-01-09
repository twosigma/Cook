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
(ns cook.hooks-definitions)

(defprotocol SchedulerHooks
  (check-job-submission-default [this]
    "The default return value to use if check-job-submission if we've run out of time.")

  (check-job-submission [this job-map]
    "Check a job submission for correctness at the time of submission. Returns a map with one of two possibilities:
      {:status :accepted}
      {:status :rejected}

      This check is run synchronously with jobs submisison and MUST respond within 2 seconds, and should ideally return within
      100ms, or less. Furthermore, if multiple jobs are submitted in a batch (which may contain tens to hundreds to
      thousands of jobs), the whole batch of responeses MUST complete within a different timeout seconds.")
  (check-job-invocation [this job-map]
    "Check a job submission for if we can run it now. Returns a map with one of two possibilities:
      {:status :accepted :cache-expires-at <DateTime to expire>}
      {:status :deferred :cache-expires-at <DateTime to expire>}

      This check is run just before a job is about to launch, and MUST return within milliseconds, without blocking
      (If you don't have have a definitive result, return a retry a few tens of milliseconds later)

      If the return value is :status :accepted, the job is considered ready to launch right now.
      If the return value is :status :deferred, the job execution should be deferred until at least the given datetime."))
