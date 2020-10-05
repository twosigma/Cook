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
(ns cook.unscheduled
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.edn :as edn]
            [cook.plugins.launch :as launch-plugin]
            [cook.quota :as quota]
            [cook.rate-limit :as ratelimit]
            [cook.scheduler.scheduler :as scheduler]
            [cook.scheduler.share :as share]
            [cook.tools :as util]
            [datomic.api :as d :refer [q]]))

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
        usages (map util/job->usage jobs-with-new)
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
  [read-limit-fn err-msg db job running-jobs]
  (when (= (:job/state job) :job.state/waiting)
    (let [user (:job/user job)
          pool-name (-> job :job/pool :pool/name)
          ways (how-job-would-exceed-resource-limits
                (read-limit-fn db user pool-name)
                running-jobs
                job)]
      (when (seq ways)
        [err-msg ways]))))

(def constraint-name->message
  {"novel_host_constraint" "Job already ran on this host."
   "gpu_host_constraint" "Host has no GPU support."
   "non_gpu_host_constraint" "Host is reserved for jobs that need GPU support."
   "attribute-equals-host-placement-group-constraint" "Host had a different attribute than other jobs in the group."})

(defn fenzo-failures-for-user
  "Given a minimal data structure containing serialized summary of fenzo errors,
  e.g. {:resources {:cpus 2 :mem 3} :constraints {\"unique_host_constraints\" 4}},
  Returns a data structure suitable for presentation, e.g.
  [{:reason \"Not enough CPU available\" :host_count 2}
   {:reason \"Not enough Memory available\" :host_count 3}
   {:reason \"Job already ran on this host.\" :host_count 4}]"
  [raw-summary]
  (reduce into [] [(map (fn [[k v]] {:reason (str "Not enough " k " available.")
                                     :host_count v})
                        (:resources raw-summary))
                   (map (fn [[k v]] {:reason (or (constraint-name->message k) k)
                                     :host_count v})
                        (:constraints raw-summary))]))

(defn check-fenzo-placement
  "Places the job Under Investigation (next time Fenzo fails to place the job,
  the details of that will be recorded).
  If there are already any Fenzo placement failures associated with the job, returns
  [(String explaining placement failure), {data structure containing host counts for
  each type of failure}].
  Otherwise, if job wasn't already under investigation,
  returns [String indicating job is now under investigation, {}] "
  [conn job]
  (when-not (:job/under-investigation job)
    @(d/transact conn [{:db/id (:db/id job)
                        :job/under-investigation true}]))
  (if (:job/last-fenzo-placement-failure job)
    ["The job couldn't be placed on any available hosts."
     {:reasons (-> job :job/last-fenzo-placement-failure edn/read-string
                   fenzo-failures-for-user)}]
    ["The job is now under investigation. Check back in a minute for more details!" {}]))

(defn- get-jobs-by-user-and-state
  "Returns the first `limit` jobs for the given `user` in `state` in `pool-name`,
   submitted within `days-to-look-back`"
  [db user state limit pool-name days-to-look-back]
  (let [end (tc/to-date (t/now))
        start (tc/to-date (t/minus (t/now) (t/days days-to-look-back)))]
    (util/get-jobs-by-user-and-states db user [state] start end
                                      limit (constantly true)
                                      true pool-name)))

(defn check-queue-position
  "IFF the job is not first in the user's queue, returns
  [\"You have x other jobs ahead in the queue\", {:jobs [other job uuids]]}]"
  [conn job running-jobs waiting-jobs]
  (let [db (d/db conn)
        job-uuid (:job/uuid job)
        running-tasks (->> running-jobs
                           (map (fn [j] (->> j
                                             :job/instance
                                             (filter util/instance-running?)
                                             last))))
        pending-tasks (->> waiting-jobs
                           (map util/create-task-ent))
        all-tasks (into running-tasks pending-tasks)
        sorted-tasks (vec (sort (util/same-user-task-comparator) all-tasks))
        queue-pos (or (first
                       (keep-indexed
                        (fn [i instance]
                          (when (= (-> instance :job/_instance :job/uuid) job-uuid) i))
                        sorted-tasks))
                      (count all-tasks))
        tasks-ahead (subvec sorted-tasks 0 (min queue-pos 10))
        message (if (= (count all-tasks) queue-pos)
                  (str "You have at least " queue-pos " other jobs ahead in the queue.")
                  (str "You have " queue-pos " other jobs ahead in the queue."))]
    (when (seq tasks-ahead)
      [message
       {:jobs (->> tasks-ahead
                   (mapv #(-> % :job/_instance :job/uuid str)))}])))

(defn- check-launch-rate-limit
  "Return the appropriate error message if a user's job is unscheduled because they're over the job launch rate limit threshold"
  [{:keys [job/user]}]
  (let [enforcing-job-launch-rate-limit? (ratelimit/enforce? quota/per-user-per-pool-launch-rate-limiter)
        num-ratelimited (->> @cook.tools/pool->user->num-rate-limited-jobs
                             vals
                             (map #(get % user 0))
                             (reduce + 0))
        being-ratelimited? (pos? num-ratelimited)]
    (when (and enforcing-job-launch-rate-limit? being-ratelimited?)
      ["You are currently rate limited on how many jobs you launch per minute." {:num-ratelimited num-ratelimited}])))

(defn- check-plugin-filter
  "Return the appropriate error message if a user's job is unscheduled because the launch plugin
  has blocked the launch."
  [job]
  (let [{:keys [status message]} (launch-plugin/get-filter-status job)]
    (when (= status :deferred)
      ["The launch filter plugin is blocking the job launch."
       {:plugin (str (type launch-plugin/plugin-object))
        :message message}])))

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
      (let [user (:job/user job)
            pool-name (-> job :job/pool :pool/name)
            running-jobs (get-jobs-by-user-and-state db user "running" 1000000 pool-name 30)
            waiting-jobs (get-jobs-by-user-and-state db user "waiting" 100 pool-name 7)]
        (filter some?
                [(check-exhausted-retries db job)
                 (check-exceeds-limit quota/get-quota
                                      "The job would cause you to exceed resource quotas."
                                      db job running-jobs)
                 (check-exceeds-limit share/get-share
                                      "The job would cause you to exceed resource shares."
                                      db job running-jobs)
                 (check-launch-rate-limit job)
                 (check-plugin-filter job)
                 (check-queue-position conn job running-jobs waiting-jobs)
                 (check-fenzo-placement conn job)])))))
