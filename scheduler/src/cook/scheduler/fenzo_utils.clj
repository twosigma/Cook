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
(ns cook.scheduler.fenzo-utils
  (:require [clojure.tools.logging :as log]
            [datomic.api :as d])
  (:import (com.netflix.fenzo TaskAssignmentResult)))

(defn extract-message
  "For some reason, Fenzo's AssignmentFailure doesn't have a getter for the
  message field.  Need to parse it from the string representation of the
  AssignmentFailure.
  Example of string to be parsed:
  AssignmentFailure{resource=CPUS, asking=4.0, used=7.0 available=1.0, message=cpus}

  TODO: upgrade to newer version of Fenzo, which has the getter for Message,
  and this won't be necessary anymore."
  [resource-failure]
  (last (re-find (re-pattern "message=(\\w+)") (str resource-failure))))

(defn count-resource-failure
  "Fenzo TaskAssignmentResults tend to provide AssignmentFailures in pairs for each
   resource placement issue. One AssignmentFailure will describe the resource in the
   message field; the other will use the Resource field.  We actually want to use
   the message field because the Enumeration of the Resource field doesn't allow
   for GPU, whereas the message field can contain the string \"gpu\"."
  [totals resource-failure]
  (let [msg (extract-message resource-failure)]
    (if (empty? msg)
      totals
      (update-in totals [:resources msg] (fnil inc 0)))))

(defn summarize-placement-failure
  "Given an existing set of placement failure totals (e.g.
  {:resources {:cpus 1 :mem 4} :constraints {'unique_host_constraint' 2}})
  , and an individual fenzo AssignmentResult, increments the failure counts
  where appropriate and resturns the result."
  [totals assignment-result]
  (let [resources-counted (reduce count-resource-failure totals (.getFailures assignment-result))
        constraint-failure (.getConstraintFailure assignment-result)]
    (if constraint-failure
      (update-in resources-counted [:constraints (.getName constraint-failure)] (fnil inc 0))
      resources-counted)))

(defn accumulate-placement-failures
  "Param should be a collection of Fenzo placement failure results for a single
  job.  Returns a tuple of [job ent-id, single string summarizing
  all the messages].  Along the way, logs each result message if debug-level
  logging is enabled."
  [failures]
  (let [job (:job (.getRequest ^TaskAssignmentResult (first failures)))
        under-investigation? (:job/under-investigation job)
        summary (when (or under-investigation? (log/enabled? :debug))
                  (reduce summarize-placement-failure {} failures))]
    (log/debug "Job" (:job/uuid job) " placement failure summary:" summary)
    (when (and under-investigation? (seq summary))
      [(:job/uuid job) summary])))

(defn record-placement-failures!
  "For any failure-results for jobs that are under investigation,  persists all
  Fenzo placement errors as an attribute of the job.  Also logs information about
  placement errors if debug-level logging is enabled."
  [conn failure-results]
  (log/debug "Fenzo placement failure information:" failure-results)
  (let [failures-by-job (into {} (map accumulate-placement-failures
                                      failure-results))
        transactions (mapv (fn [[job-uuid failures-description]]
                             (let [failure-summary (str failures-description)]
                               (when (> (count failure-summary) 25000)
                                 (log/warn "Fenzo placement Failure summary is too long!" failure-summary))
                               {:db/id [:job/uuid job-uuid]
                                :job/under-investigation false
                                :job/last-fenzo-placement-failure failure-summary}))
                           failures-by-job)]
    (when (seq transactions)
      @(d/transact conn transactions))))

