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
            [cook.hooks-definitions :refer [SchedulerHooks check-job-invocation check-job-submission check-job-submission-default]]
            [mount.core :as mount]
            [clojure.tools.logging :as log])
  (:import (com.google.common.cache Cache CacheBuilder)
           (java.util.concurrent TimeUnit)))

(def default-accept
  "A default accept object with an expiration at infinity"
  {:status :accepted :message "No hook object defined" :cache-expires-at (t/date-time 2783 12 03)})

(def accept-all-hook
  "A hook object that accepts everything. Available for use as a default hook and unit testing."
  (reify SchedulerHooks
    (check-job-submission-default [_] default-accept)
    (check-job-submission [_ _] default-accept)
    (check-job-invocation [_ _] default-accept)))

(defn create-default-hook-object
  "Returns the hook object that matches to a given job map. Returns an always-accept Hook object if nothing is defined."
  [config]
  (let [{:keys [settings]} config
        {:keys [hook-factory-function]} settings]
    (if-let [factory (seq hook-factory-function)]
      (factory)
      accept-all-hook)))

;  Contains the hook object that matches to a given job map. This code may create a new hook object or re-use an existing one.
;  Assume nothing about the lifespan of a hook object. Never returns nil. Returns an always-accept Hook object if nothing is defined."
(def hook-object (create-default-hook-object {}))
; TODO: Mount isn't initializing... :(
;(mount/defstate hook-object
;  :start (create-job-submission-rate-limiter config))

(def submission-hook-batch-timeout-seconds 40) ; Self-imposed deadline to submit a batch.
(def age-out-last-seen-deadline-minutes (t/minutes 10))
(def age-out-first-seen-deadline-minutes (t/hours 10))
(def age-out-seen-count 10)

; We may see up to the entire scheduler queue, so have a big cache here.
; This is called in the scheduler loop. If it hasn't been looked at in more than 2 hours, the job has almost assuredly long since run.
(def ^Cache job-invocations-cache
  (-> (CacheBuilder/newBuilder)
      (.maximumSize 100000)
      (.expireAfterAccess 2 TimeUnit/HOURS)
      (.build)))


(defn aged-out?
  [{:keys [last-seen first-seen seen-count] :as old-result}]
  {:post [(or (true? %) (false? %))]}
  (let [last-seen-deadline (->> age-out-last-seen-deadline-minutes
                                (t/minus- (t/now)))
        first-seen-deadline (->> age-out-first-seen-deadline-minutes
                                 (t/minus- (t/now)))]
    ;; If I've seen the job for at least 10 hours, at least 20 times, and once in the last 10 minutes
    ;; Treat the job as if its aged out.
    (and
      (boolean old-result)
      (> seen-count age-out-seen-count)
      (t/before? first-seen first-seen-deadline)
      (t/before? last-seen-deadline last-seen))))


(defn filter-job-invocations-miss
  "This is the cache miss handler. It is invoked if we have a cache miss --- either the entry is expired, or
   its not there. Only invoke on misses or expirations, because we count the number of invocations."
  [job]
  {:post [(or (true? %) (false? %))]}
  (let [{:keys [first-seen seen-count] :as old-result} (ccache/get-if-present
                                                         job-invocations-cache
                                                         :job/uuid
                                                         job)
        not-found? (not old-result)
        is-aged-out? (aged-out? old-result)]
    ; If aged-out, no more backend queries. Return true so the job is kept and
    ; invoked and it sinks or swims. Short circuits the evaluation, done below.
    (or is-aged-out?
        ;; Ok. Not aging out. Query the underlying plugin as to the status.
        (let [{:keys [status] :as raw-result} (check-job-invocation hook-object job)
              result
              (if (or not-found?
                      (= status :accepted))
                ; If not found, or we got an accepted status, store it and reset the counters.
                (merge raw-result
                       {:last-seen (t/now)
                        :first-seen (t/now)
                        :seen-count 1})
                ; We were found, but didn't get an accepted status. Increment the counters for aging out.
                (merge raw-result
                       {:last-seen (t/now)
                        :first-seen first-seen
                        :seen-count (inc seen-count)}))]
          (assert (#{:accepted :deferred} status) (str "Plugin must return a status of :accepted or :deferred. Got " status))

          (ccache/put-cache! job-invocations-cache :job/uuid job
                             result)
          ; Did the query, If the status was accepted, then we're keeping it.
          (= status :accepted)))))

(defn filter-job-invocations
  "Run the hooks for a set of jobs at invocation time, returns true or false on whether the job is ready to run now."
  [job]
  {:post [(or (true? %) (false? %))]}
  (let [{:keys [status cache-expires-at] :as result} (ccache/get-if-present
                                                       job-invocations-cache
                                                       :job/uuid
                                                       job)
        expired? (and cache-expires-at (t/after? (t/now) cache-expires-at))]
    ; Fast path, if it has an expiration (its found), and its not expired, and it is accepted, then we're good.
    (if (and result (not expired?))
      (= status :accepted)
      (filter-job-invocations-miss job))))

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
        results (apply list (map do-one-job jobs))
        errors (apply list (filter #(= :rejected (:status %)) results))
        error-count (count errors)
        error-samples (apply list (take 3 errors))]
    (if (zero? error-count)
      {:status :accepted}
      {:status :rejected :message (str "Total of " error-count " errors. First 3 are: " error-samples)})))
