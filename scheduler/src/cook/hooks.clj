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
            [cook.cache :as ccache])
  (:import (com.google.common.cache CacheBuilder)
           (java.util.concurrent TimeUnit)))



(defprotocol ScheduleHooks
  (check-job-submission-default [this]
    "The default return value to use if check-job-submission if we've run out of time.")

  (check-job-submission [this job-map]
    "Check a job submission for correctness at the time of submission. Returns a map with one of two possibilities:
      {:status :ok}
      {:status :error :message "<SOME MESSAGE>"}

      This check is run synchronously with jobs submisison and MUST respond within 2 seconds, and should ideally return within
      100ms, or less. Furthermore, if multiple jobs are submitted in a batch (which may contain tens to hundreds to
      thousands of jobs), the whole batch of responeses MUST complete within 30 seconds. Note that this API is called on a
      best-effort basis, and not be called at all or may be called after hook-check-job-before-invocation.

      Deadline indicates when the response must return. If the current time is later than the deadline, the plugin
      MUST return a value immediately with no chance of blocking.")
  (check-job-invocation [this job-map]
    "Check a job submission for if we can run it now. Returns a map with one of three possibilities:
      {:status :ok}
      {:status :error :message \"Message\"}
      {:status :later :message \"Message\" :retry-at <DateTime to relaunch>}

      This check is run just before a job is about to launch, and MUST return within milliseconds, without blocking
      (If you don't have have a definitive result, return a retry a few tens of milliseconds later)

      If the return value is :status :ok, the job is considered ready to launch right now.
      If the return value is :status :error, the job is considered bad and should be failed with the given message.

      If the return value is :status :later, the job execution should be deferred until the given datetime. Note that you cannot
      delay a job indefinitely, after a certain time (set via XXXX in the config.edn), a job will be killed automatically.

      In addition, certain rate limits apply for re-invocation."))


(def TODO-query-timeout 60); Should get the default query timeout out of config and stuff it here.
(def job-max-deferral 300) ; TODO: This should come from the config.



(defn result-reducer
  [{accum-status :status accum-message :message accum-retry :retry-at :as accum}
   {in-status :status in-message :message in-retry :retry-at :as in}]
  (cond
    ; If either is OK, use the other.
    (= accum-status :ok) in
    (= in-status :ok) accum
    ;; If both are error, then merge the messages.
    (and (= accum-status :error) (= in-status :error))
    ({:status :error :message (str accum-message " AND " in-message)})
    ; If one is error and the other not.
    (and (= accum-status :later) (= in-status :error)) in
    (and (= in-status :later) (= accum-status :error)) accum
    ; If both are later, use the smallest one.
    :else
    ({:status :later :message (str accum-message " AND " in-message) :retry-at (min accum-retry in-retry)})))

(defn get-hook-objects
  [job-map]
  "Returns a list of all of the hook objects that match to a given job map. This code may create a new hook object or re-use an existing one.
  Assume nothing about the lifespan of a hook object"
  (list)
  ; TODO: Empty list for now, should pull the factory methods out of the configuration dictionary,
  ; TODO: and invoke them to return the hook objects.
  )


(defn run-all-job-submisison-hooks-and-merge-result
  [deadline job-map]
  "Identify all of the hook objects for a given job-map, and invoke the check-job-submission job hook on all relevant objects"
  (let [wrap-check-submission (fn [hook-ob]
                                (let [now (t/now)]
                                  (if (< now deadline)
                                    (check-job-submission hook-ob job-map)
                                    ; Default to accept.
                                    (check-job-submission-default hook-ob))))]

    (->> (get-hook-objects job-map)
         (pmap wrap-check-submission)
         (reduce result-reducer {:status :ok}))))

(defn run-all-check-job-invocation-and-merge-result
  [job-map]
  "Identify all of the hook objects for a given job-map, and invoke the check-job-invocation job hook on all relevant objects"
  (->> (get-hook-objects job-map)
       (pmap #(check-job-invocation % job-map))
       (reduce result-reducer {:status :ok})))

(defn new-cache []
  (-> (CacheBuilder/newBuilder)
      (.maximumSize 200000)
      (.expireAfterAccess 2 TimeUnit/HOURS)
      (.build)))


(defonce job-invocations-cache (new-cache))
(defn job-unschedule-kill-for-staleness [job]
  "This job has become OK to run. So, take it off of the murder list"
  ; TODO
  )
(defn job-schedule-kill-for-staleness [job]
  "This job has entered the murder list. To avoid :later jobs from clogging up the queue, we murder them
  once they get sufficiently old. They should be murdered TODO-murder-time after they are first contracted to
  be murdered. Murders can be cancelled via job-murder-rescue."
  ; TODO
  )

(defn job-kill-bad [job]
  "This job is marked as bad by a plugin, kill it."
  ; TODO
  )

(defn hook-jobs-submission
  [jobs]
  "Run the hooks for a set of jobs at submission time."
  (let [deadline (->> TODO-query-timeout ; Deadline is half of the query timeout.
                      (#(/ % 2))
                      t/seconds ; TODO: Check units.
                      (t/plus- (t/now)))]
    (->> jobs
         (map #(partial run-all-job-submisison-hooks-and-merge-result deadline %))
         (reduce result-reducer {:status :ok}))))


(defn filter-jobs-invocations
  "Run the hooks for a set of jobs at invocation time, filter jobs that are not ready yet."
  [job]
  (let [miss-handler (fn miss-handler [job]
                       (let [{:keys [status] :as result} (run-all-check-job-invocation-and-merge-result job)]
                         (cond (= status :ok)
                               (job-unschedule-kill-for-staleness job)
                               (= status :later)
                               (job-schedule-kill-for-staleness job)
                               (= status :error)
                               (do
                                 (job-unschedule-kill-for-staleness job)
                                 (job-kill-bad job)))
                         status))
        {:keys [status]} (ccache/lookup-cache-with-expiration!
                           job-invocations-cache
                           :uuid
                           miss-handler job)]
    (= status :ok)))
