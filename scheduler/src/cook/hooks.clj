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
    ;            [cook.mesos :as mesos]
    ;        [cook.mesos.api]
            [cook.datomic :as datomic])
  (:import (com.google.common.cache CacheBuilder)
           (java.util.concurrent TimeUnit)))

(defn kill-job [job-uuid error-code]
  (cook.mesos/kill-job nil [job-uuid] error-code)
  ;; TODO: Need database connection.
  ;; TODO: Note. Can't actually use cook.mesos/kill-job. because of circular dependencies
  )



(defprotocol ScheduleHooks
  (check-job-submission-default [this]
    "The default return value to use if check-job-submission if we've run out of time.")

  (check-job-submission [this job-map]
    "Check a job submission for correctness at the time of submission. Returns a map with one of two possibilities:
      {:status :ok}
      {:status :error :message "<SOME MESSAGE>" :error-code <number>}

      This check is run synchronously with jobs submisison and MUST respond within 2 seconds, and should ideally return within
      100ms, or less. Furthermore, if multiple jobs are submitted in a batch (which may contain tens to hundreds to
      thousands of jobs), the whole batch of responeses MUST complete within 30 seconds. Note that this API is called on a
      best-effort basis, and not be called at all or may be called after hook-check-job-before-invocation.

      Deadline indicates when the response must return. If the current time is later than the deadline, the plugin
      MUST return a value immediately with no chance of blocking.")
  (check-job-invocation [this job-map]
    "Check a job submission for if we can run it now. Returns a map with one of three possibilities:
      {:status :ok}
      {:status :error :message \"Message\" :error-code <number>}
      {:status :later :message \"Message\" :cache-expire-at <DateTime to relaunch>}

      This check is run just before a job is about to launch, and MUST return within milliseconds, without blocking
      (If you don't have have a definitive result, return a retry a few tens of milliseconds later)

      If the return value is :status :ok, the job is considered ready to launch right now.
      If the return value is :status :error, the job is considered bad and should be failed with the given message.

      If the return value is :status :later, the job execution should be deferred until the given datetime. Note that you cannot
      delay a job indefinitely, after a certain time (set via XXXX in the config.edn), a job will be killed automatically.

      In addition, certain rate limits apply for re-invocation."))


(def TODO-query-timeout 60); Should get the default query timeout out of config and stuff it here.
(def max-possible-date  (t/date-time 2783 12 03)) ; 27831203

(defn get-hook-object
  [job-map]
  "Returns the hook object that matches to a given job map. This code may create a new hook object or re-use an existing one.
  Assume nothing about the lifespan of a hook object. Never returns nil. Returns an always-accept Hook object if nothing is defined."
  nil
  ; TODO: Empty list for now, should pull the factory methods out of the configuration dictionary,
  ; TODO: and invoke them to return the hook objects.
  )

; We only look at the head few thousad jobs of the scheduler queue, so 200k is overkill.
; This is called in the scheduler loop. If it hasn't been looked at in more than 2 hours, the job has almost assuredly long since run.
(defn new-cache []
  (-> (CacheBuilder/newBuilder)
      (.maximumSize 200000)
      (.expireAfterAccess 2 TimeUnit/HOURS)
      (.build)))

(defn filter-job-invocations
  "Run the hooks for a set of jobs at invocation time, return true if a job is ready to run now."
  [job]
  (let [miss-handler (fn miss-handler [{:keys [uuid] :as job-map}]
                       (let [hook-ob (get-hook-object job-map)
                             {:keys [status error-code] :as result} (or (check-job-invocation hook-ob job-map)
                                                                        (or {:status :ok}))]
                         (when (= status :error)
                           (kill-job uuid error-code))
                         result))
        {:keys [status]} (ccache/lookup-cache-with-expiration!
                           job-invocations-cache
                           :uuid
                           miss-handler job)]
    (= status :ok)))

(defn hook-jobs-submission
  [jobs]
  "Run the hooks for a set of jobs at submission time."
  (let [deadline (->> TODO-query-timeout ; Deadline is half of the query timeout.
                      (#(/ % 2))
                      t/seconds ; TODO: Check units.
                      (t/plus- (t/now)))
        do-one-job (fn do-one-job [job-map]
                     (let [hook-ob (get-hook-object job-map)
                           now (t/now)
                           status (if (< now deadline)
                                    (check-job-submission hook-ob job-map)
                                    ; Default to accept.
                                    (check-job-submission-default hook-ob))]
                       (or status {:status :ok})))]
    (->> jobs
         (map do-one-job)
         ; Return a sample for a bad job, or OK.
         (or (some #(and (= :error (:status %)) %)) {:status :ok}))))
