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
(ns cook.hooks
  (:require [clj-time.core :as t]
            [clj-time.periodic]
            [chime :as chime]
            [cook.cache :as ccache]
            [cook.config :refer [config]]
            [cook.datomic :as datomic]
            [mount.core :as mount]
            [clojure.tools.logging :as log])
  (:import (com.google.common.cache Cache CacheBuilder)
           (java.util.concurrent TimeUnit)))

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

(def default-accept
  "A default accept object with an expiration at infinity"
  {:status :accepted :cache-expires-at (t/date-time 2783 12 03)})

(defn create-job-submission-rate-limiter
  "Returns the hook object that matches to a given job map. Returns an always-accept Hook object if nothing is defined."
  [config]
  (let [{:keys [settings]} config
        {:keys [hook-factory-function]} settings]
    (if-let [factory (seq hook-factory-function)]
      (factory)
      (reify SchedulerHooks
        (check-job-submission-default [_] default-accept)
        (check-job-submission [_ _] default-accept)
        (check-job-invocation [_ _] default-accept)))))

;  Contains the hook object that matches to a given job map. This code may create a new hook object or re-use an existing one.
;  Assume nothing about the lifespan of a hook object. Never returns nil. Returns an always-accept Hook object if nothing is defined."
(def hook-object (create-job-submission-rate-limiter {}))
; TODO: Mount isn't initializing... :(
;(mount/defstate hook-object
;  :start (create-job-submission-rate-limiter config))

(def submission-hook-batch-timeout-seconds 60) ; Self-imposed deadline to submit a batch.

; We may see up to the entire scheduler queue, so have a big cache here.
; This is called in the scheduler loop. If it hasn't been looked at in more than 2 hours, the job has almost assuredly long since run.
(def ^Cache job-invocations-cache
  (-> (CacheBuilder/newBuilder)
      (.maximumSize 1000000)
      (.expireAfterAccess 2 TimeUnit/HOURS)
      (.build)))

(defn filter-job-invocations
  "Run the hooks for a set of jobs at invocation time, return true if a job is ready to run now."
  [job]
  (let [{:keys [status]} (ccache/lookup-cache-with-expiration!
                           job-invocations-cache
                           :uuid
                           (partial check-job-invocation hook-object) job)]
    (if (= status :accepted)
      job
      nil)))

(defn hook-jobs-submission
  [jobs]
  "Run the hooks for a set of jobs at submission time."
  (let [deadline (->> submission-hook-batch-timeout-seconds ; Self-imposed deadline to submit a batch.
                      t/seconds
                      (t/plus- (t/now)))
        do-one-job (fn do-one-job [job-map]
                     (let [now (t/now)
                           status (if (t/before? now deadline)
                                    (check-job-submission hook-object job-map)
                                    ; Running out of time, do the default.
                                    (check-job-submission-default hook-object))]
                       (or status default-accept)))
        results (map do-one-job jobs)
        an-error (some #(= :rejected (:status %)) results)]
    (or an-error {:status :accepted})))
