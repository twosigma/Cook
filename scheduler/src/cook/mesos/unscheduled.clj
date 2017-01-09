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
(ns cook.mesos.unscheduled
  (:require [clojure.tools.logging :as log]
            [datomic.api :as d :refer (q)]
            [cook.mesos.scheduler :as scheduler]
            [cook.mesos.quota :as quota]
            [cook.mesos.share :as share]
            [cook.mesos.util :as util]
            [clojure.edn :as edn])
  (import java.util.Date))

(defn check-exhausted-retries
  [db job]
  (let [attempts-consumed (util/job-ent->attempts-consumed db job)
        max-retries (:job/max-retries job)]
    (if (>= attempts-consumed max-retries)
        ["Job has exhausted its maximum number of retries."
         {:max-retries max-retries, :instance-count attempts-consumed}])))


(defn how-job-would-exceed-resource-limits
  "Returns a data structure describing how the proposed job would cause a user's
  resource limits to be exceeded, given a set of jobs that are already running.
  Limits param should be something like {:mem 2000 :cpus 4 :count 3}.
  Return value will be something like {:mem {:limit 2000 :usage 2400}
                                       :cpus {:limit 4 :usage 5}}"
  [limits running-jobs job]
  (let [jobs-with-new (conj running-jobs job)
        usages (map scheduler/job->usage jobs-with-new)
        total-usage (reduce (partial merge-with +) usages)]
    (->> (map (fn [[k v]]
                (when (> (or (k total-usage) 0) v)
                  [k {:limit v :usage (k total-usage)}]))
              limits)
         (filter seq)
         (into {}))))

(defn check-exceeds-limit
  "If running the job would cause a user's resource limits to be exceeded,
  return [err-msg, (data structure describing the ways the limit would be exceeded)].
  This function can be used for different types of limts (quota or share);
  the function to read the user's limit as well as the error message on
  exceeding the limit are parameters."
  [read-limit-fn err-msg db job]
  (when (= (:job/state job) :job.state/waiting)
    (let [user (:job/user job)
          ways (how-job-would-exceed-resource-limits
                (read-limit-fn db user)
                (util/jobs-by-user-and-state db user :job.state/running)
                job)]
      (if (seq ways)
        [err-msg ways]
        nil))))

(defn reasons
  "Top level function which assembles a data structure representing the list
  of possible responses to the question \"Why isn't this job being scheduled?\".
  This is a vector of tuples of (answer, extra data about answer)
  e.g. [[\"Reason One\" {:key1 1 :key2 2}]
        [\"Reason Two\" {:keya 3 :keyb 4}]]
  Possible responses can include essentially saying,
  \"Actually, it WAS scheduled\"\" e.g. [[\"The job is running now.\" {}]]."
  [conn job]
  (let [db (d/db conn)]
    (case (:job/state job)
      :job.state/running [["The job is running now." {}]]
      :job.state/completed [["The job already completed." {}]]
      (filter some?
              [(check-exhausted-retries db job)
               (check-exceeds-limit quota/get-quota
                                    "The job would cause you to exceed resource quotas."
                                    db job)
               (check-exceeds-limit share/get-share
                                    "The job would cause you to exceed resource shares."
                                    db job)]))))

