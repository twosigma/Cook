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
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cook.config :as config]
            [cook.plugins.definitions :refer [check-job-submission check-job-submission-default JobSubmissionValidator]]
            [cook.plugins.util]
            [cook.regexp-tools :as regexp-tools]
            [mount.core :as mount])
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
    (check-job-submission [_ _ _] default-accept)))

(defn- minimum-time
  "Takes a list of clojure time objects and returns the earliest"
  [ts]
  (->> ts
       (sort-by tc/to-long)
       first))

(defrecord CompositeSubmissionPlugin [plugins]
  JobSubmissionValidator
  (check-job-submission-default [_]
    (check-job-submission-default (first plugins)))
  (check-job-submission [_ job-map pool-name]
    (let [component-checks (map (fn [plugin] (check-job-submission plugin job-map pool-name))
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
                                             (throw (ex-info "Unable to resolve factory function" factory-fn))))
                                   factory-fns))
          plugins (map (fn [f] (f)) resolved-fns)]
      (log/info "Creating composite plugin with components" resolved-fns)
      (->CompositeSubmissionPlugin plugins))
    (if-let [resolved-fn (cook.plugins.util/resolve-symbol (symbol factory-fn))]
      (do
        (log/info "Using plugin" resolved-fn)
        (resolved-fn))
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
(mount/defstate ^Cache job-launch-cache
  :start (-> (CacheBuilder/newBuilder)
             (.maximumSize (get-in config/config [:settings :cache-working-set-size]))
             (.expireAfterAccess 2 TimeUnit/HOURS)
             (.build)))

(defn plugin-jobs-submission
  [job-pool-name-maps]
  "Run the plugins for a set of jobs at submission time."
  (let [deadline (->> (config/batch-timeout-seconds-config)
                      ; One submission can include multiple jobs that must all be checked.
                      ; Self-imposed deadline to get them all checked.
                      (t/plus- (t/now)))
        do-one-job (fn do-one-job
                     [{:keys [job pool-name]}]
                     (let [now (t/now)
                           status (if (t/before? now deadline)
                                    (check-job-submission plugin-object job pool-name)
                                    ; Running out of time, do the default.
                                    (check-job-submission-default plugin-object))]
                       (or status default-accept)))
        results (map do-one-job job-pool-name-maps)
        errors (filter #(= :rejected (:status %)) results)
        error-count (count errors)
        ; Collect a few errors to show in the response. (not every error)
        max-errors-to-show 3
        error-samples (->> errors
                           (map :message)
                           (take max-errors-to-show)
                           (apply list))]
    (if (zero? error-count)
      {:status :accepted}
      {:status :rejected
       :message
       (str
         (if (<= error-count max-errors-to-show)
           ""
           (str "Total of " error-count " errors; first "
                (count error-samples) " are:\n"))
         (str/join "\n" error-samples))})))

(defrecord JopShapeValidationPlugin
  []
  JobSubmissionValidator

  (check-job-submission-default [_]
    {:status :accepted})

  (check-job-submission [_ {:keys [constraints cpus disk gpus mem]} pool-name]
    (if (and gpus (pos? gpus))
      {:status :accepted}

      ; The limits-config map has the following shape:
      ;
      ; {:node-type->limits {"node-type-1" {:max-cpus ... :max-disk ... :max-mem ...}
      ;                      "node-type-2" {:max-cpus ... :max-disk ... :max-mem ...}
      ;                      ...
      ;                      "node-type-N" {:max-cpus ... :max-disk ... :max-mem ...}}
      ;  :non-gpu-jobs [{:pool-regex "pool-1" :node-type-lookup ...}
      ;                 {:pool-regex "pool-2" :node-type-lookup ...}
      ;                 ...
      ;                 {:pool-regex "pool-N" :node-type-lookup ...}]}
      ;
      ; where each :node-type-lookup value is a 3-level map:
      ;
      ;   node-type -> node-family -> cpu-architecture -> node-type
      ;
      ; where the three levels represent the job constraint values for node-type,
      ; node-family, and cpu-architecture that Cook can satisfy, and the value
      ; node-type represents the largest node-type Cook will match the job to given
      ; those constraints.
      ;
      ; Note that keys in the :node-type-lookup can be nil, representing the cases
      ; where the user did not specify a job constraint for that field. For example,
      ; if Cook is configured with this entry in :node-type-lookup:
      ;
      ;   nil -> nil -> "intel-cascade-lake" -> "c2-standard-16"
      ;
      ; it means that if the user only specifies the job constraint
      ;
      ;   ["cpu-architecure", "EQUALS", "intel-cascade-lake"]
      ;
      ; then Cook should use the "c2-standard-16" max sizes for validation purposes.
      (let [limits-config (config/job-resource-limits)
            node-type-lookup-map
            (regexp-tools/match-based-on-pool-name
              (:non-gpu-jobs limits-config)
              pool-name
              :node-type-lookup)]
        (if node-type-lookup-map
          (let [constraint-pattern-fn
                (fn [attribute-of-interest]
                  (->> constraints
                       (filter (fn [constraint]
                                 (let [attribute (nth constraint 0)
                                       operator (nth constraint 1)]
                                   (and (= attribute attribute-of-interest)
                                        (= operator "EQUALS")))))
                       first
                       last))
                node-type-specified (constraint-pattern-fn "node-type")
                node-family-specified (constraint-pattern-fn "node-family")
                cpu-architecture-specified (constraint-pattern-fn "cpu-architecture")
                node-type-for-validation
                (get-in node-type-lookup-map
                        [node-type-specified
                         node-family-specified
                         cpu-architecture-specified])]
            (if node-type-for-validation
              (let [node-type->resource-limits
                    (:node-type->limits limits-config)
                    {:keys [max-cpus max-disk max-mem] :as resource-limits}
                    (get node-type->resource-limits
                         node-type-for-validation)]
                (if resource-limits
                  (cond
                    (and cpus (> cpus max-cpus))
                    {:status :rejected
                     :message (str "Invalid cpus " cpus ", max is " max-cpus)}

                    (and disk (> (:request disk) max-disk))
                    {:status :rejected
                     :message (str "Invalid disk " disk ", max is " max-disk)}

                    (and mem (> mem max-mem))
                    {:status :rejected
                     :message (str "Invalid mem " mem ", max is " max-mem)}

                    :else
                    {:status :accepted})
                  {:status :rejected
                   :message (str "node-type " node-type-for-validation " is not configured")}))
              {:status :rejected
               :message (str "Invalid combination of "
                             "pool (" pool-name "), "
                             "node-type (" node-type-specified "), "
                             "node-family (" node-family-specified "), and "
                             "cpu-architecture (" cpu-architecture-specified ")")}))
          {:status :accepted})))))

(defn job-shape-validation-factory
  []
  (->JopShapeValidationPlugin))