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
(ns cook.plugins.submission
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clj-time.periodic]
            [cook.config :as config]
            [cook.plugins.definitions :refer [JobSubmissionValidator check-job-submission check-job-submission-default]]
            [cook.plugins.util]
            [mount.core :as mount]
            [clojure.tools.logging :as log]
            [clojure.string :as str])
  (:import (com.google.common.cache Cache CacheBuilder)
           (java.util.concurrent TimeUnit)))

(def default-accept
  "A default accept object with an expiration at infinity"
  {:status :accepted :message "No plugin object defined" :cache-expires-at cook.plugins.util/positive-infinity-date})

(defn accept-all-plugin-factory
  "A plugin object that accepts everything. Available for use as a default plugin and unit testing."
  []
  (reify JobSubmissionValidator
    (check-job-submission-default [_] default-accept)
    (check-job-submission [_ _] default-accept)))

(defn- minimum-time
  "Takes a list of clojure time objects and returns the earliest"
  [ts]
  (->> ts
       (sort-by tc/to-long)
       first))

(defrecord CompositeSubmissionPlugin [plugins]
  JobSubmissionValidator
  (check-job-submission-default [this]
    (check-job-submission-default (first plugins)))
  (check-job-submission [this job-map]
    (let [component-checks (map (fn [plugin] (check-job-submission plugin job-map))
                                plugins)]
      (if (every? (fn [{:keys [status]}] (= :accepted status)) component-checks)
        (let [composite-expiry (minimum-time (map :cache-expires-at component-checks))]
          {:status :accepted
           :cache-expires-at composite-expiry})
        (let [failed-checks (filter (fn [{:keys [status]}] (= status :rejected)) component-checks)
              composite-message (str/join "\n" (map :message failed-checks))
              composite-expiry (minimum-time (map :cache-expires-at component-checks))]
          {:status :rejected
           :message composite-message
           :cache-expires-at composite-expiry})))))

(defn- resolve-factory-fn
  [factory-fn factory-fns]
  (if factory-fns
    (let [resolved-fns (doall (map (fn [f] (if-let [resolved (cook.plugins.util/resolve-symbol (symbol f))]
                                             resolved
                                             (throw (ex-info "Unable to resolve factory function" factory-fn))))))
          plugins (map (fn [f] (f)) resolved-fns)]
      (->CompositeSubmissionPlugin plugins))
    (if-let [resolved-fn (cook.plugins.util/resolve-symbol (symbol factory-fn))]
      (resolved-fn)
      (throw (ex-info "Unable to resolve factory function" factory-fn)))))

(defn create-default-plugin-object
  "Returns the plugin object. If no submission plugin factory defined, returns an always-accept plugin object."
  [config]
  (let [{:keys [settings]} config
        {:keys [plugins]} settings
        {:keys [job-submission-validator]} plugins
        {:keys [factory-fn factory-fns]} job-submission-validator]
    (log/info (str "Setting up submission plugins with factory config: " job-submission-validator " and factory-fn " factory-fn))
    (if (or factory-fns factory-fn)
      (resolve-factory-fn factory-fn factory-fns)
      (accept-all-plugin-factory))))

;  Contains the plugin object that matches to a given job map. This code may create a new plugin object or re-use an existing one.
(mount/defstate plugin-object
  :start (create-default-plugin-object config/config))

; We may see up to the entire scheduler queue, so have a big cache here.
; This is called in the scheduler loop. If it hasn't been looked at in more than 2 hours, the job has almost assuredly long since run.
(def ^Cache job-launch-cache
  (-> (CacheBuilder/newBuilder)
      (.maximumSize 100000)
      (.expireAfterAccess 2 TimeUnit/HOURS)
      (.build)))

(defn plugin-jobs-submission
  [jobs]
  "Run the plugins for a set of jobs at submission time."
  (let [deadline (->> (config/batch-timeout-seconds-config)
                      ; One submission can include multiple jobs that must all be checked.
                      ; Self-imposed deadline to get them all checked.
                      (t/plus- (t/now)))
        do-one-job (fn do-one-job [job-map]
                     (let [now (t/now)
                           status (if (t/before? now deadline)
                                    (check-job-submission plugin-object job-map)
                                    ; Running out of time, do the default.
                                    (check-job-submission-default plugin-object))]
                       (or status default-accept)))
        results (map do-one-job jobs)
        errors (filter #(= :rejected (:status %)) results)
        error-count (count errors)
        ; Collect a few errors to show in the response. (not every error)
        error-samples (apply list (take 3 errors))]
    (if (zero? error-count)
      {:status :accepted}
      {:status :rejected :message (str "Total of " error-count " errors. First 3 are: " error-samples)})))
