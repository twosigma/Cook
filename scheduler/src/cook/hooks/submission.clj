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
(ns cook.hooks.submission
  (:require [clj-time.core :as t]
            [clj-time.periodic]
            [chime :as chime]
            [cook.cache :as ccache]
            [cook.config :refer [config]]
            [cook.datomic :as datomic]
            [cook.hooks.definitions :refer [JobSubmissionValidator check-job-submission check-job-submission-default]]
            [cook.hooks.util]
            [mount.core :as mount]
            [clojure.tools.logging :as log])
  (:import (com.google.common.cache Cache CacheBuilder)
           (java.util.concurrent TimeUnit)))

(def default-accept
  "A default accept object with an expiration at infinity"
  {:status :accepted :message "No hook object defined" :cache-expires-at (t/date-time 2783 12 03)})

(def accept-all-hook
  "A hook object that accepts everything. Available for use as a default hook and unit testing."
  (reify JobSubmissionValidator
    (check-job-submission-default [_] default-accept)
    (check-job-submission [_ _] default-accept)))

(defn create-default-hook-object
  "Returns the hook object. If no submission hook factory defined, returns an always-accept hook object."
  [config]
  (let [{:keys [settings]} config
        {:keys [plugins]} settings
        {:keys [job-submission-valiator]} plugins
        {:keys [factory-fn]} job-submission-valiator]
    (log/info (str "Setting up submission hooks with factory config: " job-submission-valiator " and factory-fn " factory-fn))
    (if factory-fn
      (do
        (if-let [resolved-fn (cook.hooks.util/resolve-symbol (symbol factory-fn))]
          (do
            (log/info (str "Resolved as " resolved-fn))
            (resolved-fn))
          (throw (ex-info "Unable to resolve factory function" (assoc job-submission-valiator :ns (namespace factory-fn))))))
      accept-all-hook)))

;  Contains the hook object that matches to a given job map. This code may create a new hook object or re-use an existing one.
(mount/defstate hook-object
  :start (create-default-hook-object config))

(mount/defstate batch-timeout-seconds
  :start (-> config :settings :plugins :job-submission-valiator :batch-timeout-seconds t/seconds))


; We may see up to the entire scheduler queue, so have a big cache here.
; This is called in the scheduler loop. If it hasn't been looked at in more than 2 hours, the job has almost assuredly long since run.
(def ^Cache job-launch-cache
  (-> (CacheBuilder/newBuilder)
      (.maximumSize 100000)
      (.expireAfterAccess 2 TimeUnit/HOURS)
      (.build)))

(defn hook-jobs-submission
  [jobs]
  "Run the hooks for a set of jobs at submission time."
  (let [deadline (->> batch-timeout-seconds
                      ; One submission can include multiple jobs that must all be checked.
                      ; Self-imposed deadline to get them all checked.
                      (t/plus- (t/now)))
        do-one-job (fn do-one-job [job-map]
                     (let [now (t/now)
                           status (if (t/before? now deadline)
                                    (check-job-submission hook-object job-map)
                                    ; Running out of time, do the default.
                                    (check-job-submission-default hook-object))]
                       (or status default-accept)))
        results (map do-one-job jobs)
        errors (filter #(= :rejected (:status %)) results)
        error-count (count errors)
        ; Collect a few errors to show in the response. (not every error)
        error-samples (apply list (take 3 errors))]
    (if (zero? error-count)
      {:status :accepted}
      {:status :rejected :message (str "Total of " error-count " errors. First 3 are: " error-samples)})))
