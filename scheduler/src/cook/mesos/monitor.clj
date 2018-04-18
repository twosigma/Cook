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
            [cook.config :refer (config)]
            [cook.datomic :as datomic]
            [cook.mesos.share :as share]
            [cook.mesos.util :as util]
            [datomic.api :as d :refer (q)]
            [metrics.core :as metrics]
            [metrics.counters :as counters]))

(defn- get-job-stats
  "Query all jobs for the given job state, e.g. :job.state/running or
   :job.state/waiting and produce basic stats per user.

   Return a map from users to their stats where a stats is a map from stats
   types to amounts."
  [db state]
  (let [job-ents (case state
                   :job.state/waiting (util/get-pending-job-ents db)
                   :job.state/running (util/get-running-job-ents db)
                   (throw (ex-info "Encountered unexpected job state" {:state state})))]
    (->> job-ents
         ;; Produce a list of maps from user's name to his stats.
         (mapv (fn [job-ent]
                 (let [user (:job/user job-ent)
                       stats (-> job-ent
                                 util/job-ent->resources
                                 (select-keys [:cpus :mem])
                                 (assoc :jobs 1))]
                   {user stats})))
         (reduce (partial merge-with (partial merge-with +)) {}))))

(defn- add-aggregated-stats
  "Given a map from users to their stats, associcate a special user
   \"all\" for the sum of all users stats."
  [stats]
  (if (seq stats)
    (->> (vals stats)
         (apply merge-with +)
         (assoc stats "all"))
    {"all" {:cpus 0, :mem 0, :jobs 0}}))

(defn- get-starved-job-stats
  "Return a map from starved users ONLY to their stats where a stats is a map
   from stats types to amounts."
  ([db running-stats waiting-stats]
   (let [waiting-users (keys waiting-stats)
         shares (share/get-shares db waiting-users nil [:cpus :mem])
         promised-resources (fn [user] (get shares user))
         compute-starvation (fn [user]
                              (->> (merge-with - (promised-resources user) (get running-stats user))
                                   (merge-with min (get waiting-stats user))))]
     (loop [[user & users] waiting-users
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

(defn- set-counter!
  "Sets the value of the counter to the new value.
   A data race is possible if two threads invoke this function concurrently."
  [counter value]
  (let [amount-to-inc (- (int value) (counters/value counter))]
    (counters/inc! counter amount-to-inc)))

(defn- clear-old-counters!
  "Clears counters that were present on the previous iteration
  but not in the current iteration. This avoids the situation
  where a user's job changes state but the old state's counter
  doesn't reflect the change."
  [state stats state->previous-stats-atom]
  (let [previous-stats (get @state->previous-stats-atom state)
        previous-users (set (keys previous-stats))
        current-users (set (keys stats))
        users-to-clear (difference previous-users current-users)]
    (run! (fn [user]
            (run! (fn [[type _]]
                    (set-counter! (counters/counter [state user (name type)]) 0))
                  (get previous-stats user)))
          users-to-clear)))

(defn- set-user-counters!
  "Sets counters for jobs with the given state, e.g. running, waiting and starved."
  [state stats state->previous-stats-atom]
  (clear-old-counters! state stats state->previous-stats-atom)
  (swap! state->previous-stats-atom #(assoc % state stats))
  (run!
    (fn [[user stats]]
      (run! (fn [[type amount]]
              (-> [state user (name type)]
                  counters/counter
                  (set-counter! amount)))
            stats))
    (add-aggregated-stats stats)))

(defn set-total-counter!
  "Given a state (e.g. starved) and a value, sets the corresponding counter."
  [state value]
  (-> [state "users"]
      counters/counter
      (set-counter! value)))

(defn set-stats-counters!
  "Queries the database for running and waiting jobs per user, and sets
  counters for running, waiting, starved, hungry and satisifed users."
  [db state->previous-stats-atom]
  (log/info "Querying database for running and waiting jobs per user")
  (let [running-stats (get-job-stats db :job.state/running)
        waiting-stats (get-job-stats db :job.state/waiting)
        starved-stats (get-starved-job-stats db running-stats waiting-stats)
        running-users (set (keys running-stats))
        waiting-users (set (keys waiting-stats))
        satisfied-users (difference running-users waiting-users)
        starved-users (set (keys starved-stats))
        hungry-users (difference waiting-users starved-users)
        total-count (count (union running-users waiting-users))
        starved-count (count starved-users)
        hungry-count (count hungry-users)
        satisfied-count (count satisfied-users)]
    (set-user-counters! "running" running-stats state->previous-stats-atom)
    (set-user-counters! "waiting" waiting-stats state->previous-stats-atom)
    (set-user-counters! "starved" starved-stats state->previous-stats-atom)
    (set-total-counter! "total" total-count)
    (set-total-counter! "starved" starved-count)
    (set-total-counter! "hungry" hungry-count)
    (set-total-counter! "satisfied" satisfied-count)
    (log/info "User stats: total" total-count "starved" starved-count
              "hungry" hungry-count "satisfied" satisfied-count)))

(defn start-collecting-stats
  "Starts a periodic timer to collect stats about running, waiting, and starved jobs per user.

   Return a function which can be used to stop collecting stats if invoked."
  []
  (let [interval-seconds (-> config :settings :user-metrics-interval-seconds)]
    (if interval-seconds
      (let [state->previous-stats-atom (atom {})]
        (log/info "Starting user stats collection at intervals of" interval-seconds "seconds")
        (chime-at (periodic/periodic-seq (time/now) (time/seconds interval-seconds))
                  (fn [_]
                    (let [mesos-db (d/db datomic/conn)]
                      (set-stats-counters! mesos-db state->previous-stats-atom)))
                  {:error-handler (fn [ex]
                                    (log/error ex "Setting user stats counters failed!"))}))
      (log/info "User stats collection is disabled"))))
