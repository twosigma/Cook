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
(ns cook.plugins.launch
  (:require [clj-time.core :as t]
            [clj-time.periodic]
            [clojure.tools.logging :as log]
            [cook.cache :as ccache]
            [cook.config :as config]
            [cook.plugins.definitions :refer [check-job-launch JobLaunchFilter]]
            [cook.plugins.util]
            [mount.core :as mount])
  (:import (com.google.common.cache CacheBuilder)
           (java.util.concurrent TimeUnit)))

(def default-accept
  "A default accept object with an expiration at infinity"
  {:status :accepted :message "No plugin object defined" :cache-expires-at cook.plugins.util/positive-infinity-date})

(defn accept-all-plugin-factory
  "A plugin object that accepts everything. Available for use as a default plugin and unit testing."
  []
  (reify JobLaunchFilter
    (check-job-launch [_ _] default-accept)))

(defn create-default-plugin-object
  "Returns the plugin object. If no launch plugin factory defined, returns an always-accept plugin object."
  [config]
  (let [{:keys [settings]} config
        {:keys [plugins]} settings
        {:keys [job-launch-filter]} plugins
        {:keys [factory-fn]} job-launch-filter]
    (log/info (str "Setting up launch plugins with factory config: " job-launch-filter " and factory-fn " factory-fn))
    (if factory-fn
      (do
        (if-let [resolved-fn (cook.plugins.util/resolve-symbol (symbol factory-fn))]
          (do
            (log/info (str "Resolved as " resolved-fn))
            (resolved-fn))
          (throw (ex-info "Unable to resolve factory function" (assoc job-launch-filter :ns (namespace factory-fn))))))
      (accept-all-plugin-factory))))

;  Contains the plugin object that matches to a given job map. This code may create a new plugin object or re-use an existing one.
(mount/defstate plugin-object
  :start (create-default-plugin-object config/config))

; We may see up to the entire scheduler queue, so have a big cache here.
; This is called in the scheduler loop. If it hasn't been looked at in more than 2 hours, the job has almost assuredly long since run.
(mount/defstate job-launch-cache
  :start (-> (CacheBuilder/newBuilder)
             (.maximumSize (get-in config/config [:settings :cache-working-set-size]))
             (.expireAfterAccess 2 TimeUnit/HOURS)
             (.build)))


(defn aged-out?
  "Looking at the cached dictionary, Has this job been in the queue long enough that we should force it to run and get it done. This includes:
  1. Being in the queue for a long time (hours)
  2. Attempting to launch the job fairly recently (seen in last several minutess)
  3. Tried multiple times to get the job to launch."
  [{:keys [last-seen first-seen seen-count] :as old-result}]
  {:post [(or (true? %) (false? %))]}
  (let [last-seen-deadline (->> (config/age-out-last-seen-deadline-minutes-config)
                                (t/minus- (t/now)))
        first-seen-deadline (->> (config/age-out-first-seen-deadline-minutes-config)
                                 (t/minus- (t/now)))]
    (and
      (not (nil? old-result))
      (> seen-count (config/age-out-seen-count-config))
      (t/before? first-seen first-seen-deadline)
      (t/before? last-seen-deadline last-seen))))

(defn get-filter-status-miss
  "This is the cache miss handler. It is invoked if we have a cache miss --- either the
  entry is expired, or it's not there. Only invoke on misses or expirations, because we
  count the number of invocations."
  [job]
  (let [{:keys [first-seen seen-count] :as old-result} (ccache/get-if-present
                                                         job-launch-cache
                                                         :job/uuid
                                                         job)
        is-aged-out? (aged-out? old-result)]
    ; If aged-out, we're not going to do any more backend queries. Return true so the
    ; job is kept and invoked and it sinks or swims.
    (if is-aged-out?
      {:status :accepted :message "Was aged out." :cache-expires-at (-> 10 t/minutes t/from-now)}
      ;; Ok. Not aging out. Query the underlying plugin as to the status.
      (let [{:keys [status] :as raw-result} (check-job-launch plugin-object job)
            result
            ; If the job is accepted or wasn't found, reset or create timestamps for tracking the age.
            (if (or (not old-result)
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

        (ccache/put-cache! job-launch-cache :job/uuid job
                           result)
        result))))

(defn get-filter-status
  "Run the plugin for a job at launch time, returns the status dictionary (with status,
  message, and expiration)"
  [job]
  (let [{:keys [status cache-expires-at] :as result} (ccache/get-if-present
                                                       job-launch-cache
                                                       :job/uuid
                                                       job)
        expired? (and cache-expires-at (t/after? (t/now) cache-expires-at))]
    ; Fast path, if it cached, has an expiration (i.e., it was found), and its not
    ; expired, and it is accepted, then we're done.
    (if (and result (not expired?))
      result
      (get-filter-status-miss job))))

(defn filter-job-launches
  "Run the plugins for a job at launch time, returns true if the job is ready to run now."
  [job]
  {:post [(or (true? %) (false? %))]}
  (let [{:keys [status]} (get-filter-status job)]
    (= status :accepted)))