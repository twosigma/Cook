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
(ns cook.mesos.monitor
  (:require [chime :refer [chime-at]]
            [clj-time.core :as time]
            [clj-time.periodic :as periodic]
            [clojure.set :refer (union difference)]
            [clojure.tools.logging :as log]
            [cook.datomic :refer (transact-with-retries)]
            [cook.mesos.share :as share]
            [cook.mesos.util :as util]
            [datomic.api :as d :refer (q)]
            [metatransaction.core :refer (db)]
            [metrics.counters :as counters]
            [riemann.client :as riemann]))

(defn- get-job-stats
  "Query all jobs for the given job state, e.g. :job.state/running or
   :job.state/waiting and produce basic stats per user.

   Return a map from users to their stats where a stats is a map from stats
   types to amounts."
  [db state]
  (let [job-ents (->> (q '[:find ?j
                           :in $ ?state
                           :where
                           [?j :job/state ?state]]
                         db state)
                      (map #(d/entity db (first %))))]
    (->> job-ents
         ;; Produce a list of maps from user's name to his stats.
         (mapv (fn [job-ent]
                 (let [user (:job/user job-ent)
                       stats (-> job-ent
                                 util/job-ent->resources
                                 (select-keys [:cpus :mem])
                                 (assoc :count 1))]
                   {user stats})))
         (reduce (partial merge-with (partial merge-with +)) {}))))

(defn- add-aggregated-stats
  "Given a map from users to their stats, associcate a special user
   \"all\" for the sum of all users stats."
  [db stats]
  (if (seq stats)
    (->> (vals stats)
         (apply merge-with +)
         (assoc stats "all"))
    {"all" (zipmap (conj (util/get-all-resource-types db) :count)
                   (repeat 0.0))}))

(defn- get-starved-job-stats
  "Return a map from starved users ONLY to their stats where a stats is a map
   from stats types to amounts."
  ([db]
   (get-starved-job-stats db
                          (get-job-stats db :job.state/running)
                          (get-job-stats db :job.state/waiting)))
  ([db running-stats waiting-stats]
   (let [promised-resources (fn [user]
                              (share/get-share db user [:cpus :mem]))
         compute-starvation (fn [user]
                              (->> (merge-with - (promised-resources user) (get running-stats user))
                                   (merge-with min (get waiting-stats user))))]
     ;; Loop over waiting users.
     (loop [[user & users] (keys waiting-stats)
            starved-stats {}]
       (if user
         (let [used-resources (get running-stats user)]
           ;; Check if a user is not starved.
           (if (every? true? (map (fn [[resource amount]]
                                    (< (or (resource used-resources) 0.0)
                                       amount))
                                  (promised-resources user)))
             (recur users (assoc starved-stats user (compute-starvation user)))
             (recur users starved-stats)))
         starved-stats)))))

(defn- set-counter
  "Sets the value of the counter to the new value.
   A data race is possible if two threads invoke this function concurrently."
  [counter value]
  (--> counter
       counters/value
       (- value)
       (counters/inc! counter)))

(defn- set-stats-counters
  "Sets counters for jobs with the given state, e.g. running, waiting and starved."
  [db state stats]
  (dorun
    (->> stats
         (add-aggregated-stats db)
         (mapcat (fn [[user stats]]
                   (map (fn [[type amount]]
                          (-> [state user (name type)]
                              counters/counter
                              (set-counter amount)))
                        stats))))))

(defn set-user-counter
  "Given a state (e.g. starved) and a value, sets the corresponding counter."
  [state value]
  (-> [state "users" "count"]
      counters/counter
      (set-counter value)))

(defn- set-user-stats-counters
  "Queries the database for running and waiting jobs per user, and sets
  counters for running, waiting, starved, hungry and satisifed users."
  [db]
  (let [running-stats (get-job-stats db :job.state/running)
        waiting-stats (get-job-stats db :job.state/waiting)
        starved-stats (get-starved-job-stats db running-stats waiting-stats)
        running-users (set (keys running-stats))
        waiting-users (set (keys waiting-stats))
        satisfied-users (difference running-users waiting-users)
        starved-users (set (keys starved-stats))
        hungry-users (difference waiting-users starved-users)]
    (set-stats-counters db "running" running-stats)
    (set-stats-counters db "waiting" waiting-stats)
    (set-stats-counters db "starved" starved-stats)
    (set-user-counter "total" (count (union running-users waiting-users)))
    (set-user-counter "starved" (count starved-users))
    (set-user-counter "hungry" (count hungry-users))
    (set-user-counter "satisfied" (count satisfied-users))))

(defn start-collecting-stats
  "Starts a periodic timer to collect stats about running, waiting, and starved jobs per user.

   Return a function which can be used to stop collecting stats if invoked."
  [mesos-conn & {:keys [interval]
                 :or {interval 20000}}]
  (log/info "Starting stats collection for scheduler.")
  (chime-at (periodic/periodic-seq (time/now) (time/millis interval))
            (fn [_]
              (let [mesos-db (db mesos-conn)]
                (set-user-stats-counters mesos-db)))
            {:error-handler (fn [ex]
                              (log/error ex "Setting user stats counters failed!"))}))
