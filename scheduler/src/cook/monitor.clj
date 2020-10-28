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
(ns cook.monitor
  (:require [chime :refer [chime-at]]
            [clj-time.core :as time]
            [clojure.set :refer [difference union]]
            [clojure.tools.logging :as log]
            [cook.cached-queries :as cached-queries]
            [cook.config :refer [config]]
            [cook.datomic :as datomic]
            [cook.pool :as pool]
            [cook.queries :as queries]
            [cook.scheduler.share :as share]
            [cook.util :as util]
            [cook.tools :as tools]
            [datomic.api :as d :refer [q]]
            [metrics.counters :as counters]))

(defn- get-job-stats
  "Given all jobs for a particular job state, e.g. running or
   waiting, produces basic stats per user.

   Return a map from users to their stats where a stats is a map from stats
   types to amounts."
  [job-ents pool-name]
  (->> job-ents
       (filter #(= pool-name (cached-queries/job->pool-name %)))
       ;; Produce a list of maps from user's name to his stats.
       (mapv (fn [job-ent]
               (let [user (:job/user job-ent)
                     stats (-> job-ent
                               tools/job-ent->resources
                               (select-keys [:cpus :mem])
                               (assoc :jobs 1))]
                 {user stats})))
       (reduce (partial merge-with (partial merge-with +)) {})))

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
  ([db running-stats waiting-stats pool-name]
   (let [waiting-users (keys waiting-stats)
         shares (share/get-shares db waiting-users pool-name [:cpus :mem])
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

(defn set-counter!
  "Sets the value of the counter to the new value.
   A data race is possible if two threads invoke this function concurrently."
  [counter value]
  (let [amount-to-inc (- (long value) (counters/value counter))]
    (counters/inc! counter amount-to-inc)))

(defn- clear-old-counters!
  "Clears counters that were present on the previous iteration
  but not in the current iteration. This avoids the situation
  where a user's job changes state but the old state's counter
  doesn't reflect the change."
  [state stats state->previous-stats-atom pool-name]
  (let [previous-stats (get-in @state->previous-stats-atom [pool-name state])
        previous-users (set (keys previous-stats))
        current-users (set (keys stats))
        users-to-clear (difference previous-users current-users)]
    (run! (fn [user]
            (run! (fn [[type _]]
                    (set-counter! (counters/counter [state user (name type) (str "pool-" pool-name)]) 0))
                  (get previous-stats user)))
          users-to-clear)))

(defn- set-user-counters!
  "Sets counters for jobs with the given state, e.g. running, waiting and starved."
  [state stats state->previous-stats-atom pool-name]
  (clear-old-counters! state stats state->previous-stats-atom pool-name)
  (swap! state->previous-stats-atom #(assoc-in % [pool-name state] stats))
  (run!
    (fn [[user stats]]
      (run! (fn [[type amount]]
              (-> [state user (name type) (str "pool-" pool-name)]
                  counters/counter
                  (set-counter! amount)))
            stats))
    (add-aggregated-stats stats)))

(defn set-total-counter!
  "Given a state (e.g. starved) and a value, sets the corresponding counter."
  [state value pool-name]
  (-> [state "users" (str "pool-" pool-name)]
      counters/counter
      (set-counter! value)))

(defn set-stats-counters!
  "Queries the database for running and waiting jobs per user, and sets
  counters for running, waiting, starved, hungry and satisifed users."
  [db state->previous-stats-atom pending-job-ents running-job-ents pool-name]
  (log/info "Setting stats counters for running and waiting jobs per user for" pool-name "pool")
  (let [running-stats (get-job-stats running-job-ents pool-name)
        waiting-stats (get-job-stats pending-job-ents pool-name)
        starved-stats (get-starved-job-stats db running-stats waiting-stats pool-name)
        running-users (set (keys running-stats))
        waiting-users (set (keys waiting-stats))
        satisfied-users (difference running-users waiting-users)
        starved-users (set (keys starved-stats))
        hungry-users (difference waiting-users starved-users)
        total-count (count (union running-users waiting-users))
        starved-count (count starved-users)
        hungry-count (count hungry-users)
        satisfied-count (count satisfied-users)]
    (set-user-counters! "running" running-stats state->previous-stats-atom pool-name)
    (set-user-counters! "waiting" waiting-stats state->previous-stats-atom pool-name)
    (set-user-counters! "starved" starved-stats state->previous-stats-atom pool-name)
    (set-total-counter! "total" total-count pool-name)
    (set-total-counter! "starved" starved-count pool-name)
    (set-total-counter! "hungry" hungry-count pool-name)
    (set-total-counter! "satisfied" satisfied-count pool-name)
    (log/info "Pool" pool-name "user stats: total" total-count "starved" starved-count
              "hungry" hungry-count "satisfied" satisfied-count)))

(defn start-collecting-stats
  "Starts a periodic timer to collect stats about running, waiting, and starved jobs per user.

   Return a function which can be used to stop collecting stats if invoked."
  []
  (let [interval-seconds (-> config :settings :user-metrics-interval-seconds)]
    (if interval-seconds
      (let [state->previous-stats-atom (atom {})]
        (log/info "Starting user stats collection at intervals of" interval-seconds "seconds")
        (chime-at (util/time-seq (time/now) (time/seconds interval-seconds))
                  (fn [_]
                    (log/info "Querying database for running and waiting jobs")
                    (let [mesos-db (d/db datomic/conn)
                          pending-job-ents (queries/get-pending-job-ents mesos-db)
                          running-job-ents (tools/get-running-job-ents mesos-db)
                          all-pools (pool/all-pools mesos-db)
                          pools (if (seq all-pools) all-pools [{:pool/name "no-pool"}])]
                      (run!
                        (fn [{:keys [pool/name]}]
                          (set-stats-counters! mesos-db state->previous-stats-atom
                                               pending-job-ents running-job-ents name))
                        pools)))
                  {:error-handler (fn [ex]
                                    (log/error ex "Setting user stats counters failed!"))}))
      (log/info "User stats collection is disabled"))))
