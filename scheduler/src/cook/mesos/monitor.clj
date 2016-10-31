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
  (:require [cook.mesos.scheduler :as sched]
            [cook.mesos.share :as share]
            [cook.mesos.util :as util]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [datomic.api :as d :refer (q)]
            [metatransaction.core :refer (db)]
            [riemann.client :as riemann]
            [cook.datomic :refer (transact-with-retries)]
            [clj-time.core :as time]
            [clj-time.periodic :as periodic]
            [clojure.set :refer (union difference)]
            [chime :refer [chime-at]])
  (:import [java.util.concurrent Executors TimeUnit]))

;;; ===========================================================================

;; template of riemann event
(def event
  {:host (.getHostName (java.net.InetAddress/getLocalHost))
   :ttl 180})

(defn get-job-stats
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

(defn add-aggregated-stats
  "Given a map from users to their stats, associcate a special user
   \"all\" for the sum of all users stats."
  [db stats]
  (if (seq stats)
    (->> (vals stats)
         (apply merge-with +)
         (assoc stats "all"))
    {"all" (zipmap (conj (util/get-all-resource-types db) :count)
                   (repeat 0.0))}))

(defn get-starved-job-stats
  "Return a map from starved users ONLY to their stats where a stats is a map
   from stats types to amounts."
  ([db]
   (get-starved-job-stats db
                          (get-job-stats db :job.state/running)
                          (get-job-stats db :job.state/waiting)))
  ([db running-stats waiting-stats]
   (let [promised-resources (share/get-share db "promised")
         compute-starvation (fn [user]
                              (->> (merge-with - promised-resources (get running-stats user))
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
                                  promised-resources))
             (recur users (assoc starved-stats user (compute-starvation user)))
             (recur users starved-stats)))
         starved-stats)))))

(defn make-stats-events
  "Return a list of riemann events for jobs with the given state, e.g. running,
   waiting and starved. These events can be used by (riemann/send-events ...)."
  [db state stats]
  (->> stats
       (add-aggregated-stats db)
       (mapcat (fn [[user stats]]
                 (map (fn [[type amount]]
                        (assoc event
                               :service (apply format "cook scheduler %s %s %s"
                                               (map name [state user type]))
                               :metric amount))
                      stats)))))

(defn make-user-stats-events
  [db]
  (let [running-stats (get-job-stats db :job.state/running)
        waiting-stats (get-job-stats db :job.state/waiting)
        starved-stats (get-starved-job-stats db running-stats waiting-stats)
        running-users (set (keys running-stats))
        waiting-users (set (keys waiting-stats))
        satisfied-users (difference running-users waiting-users)
        starved-users (set (keys starved-stats))
        hungry-users (difference waiting-users starved-users)]
    (conj
      (reduce into
              []
              [(make-stats-events db "running" running-stats)
               (make-stats-events db "waiting" waiting-stats)
               (make-stats-events db "starved" starved-stats)])
      (assoc event
             :service "cook scheduler total users count"
             :metric (count (union running-users waiting-users)))
      (assoc event
             :service "cook scheduler starved users count"
             :metric (count starved-users))
      (assoc event
             :service "cook scheduler hungry users count"
             :metric (count hungry-users))
      (assoc event
             :service "cook scheduler satisfied users count"
             :metric (count satisfied-users)))))

(defn riemann-reporter
  "Send various events to riemann.

   Return a function which can be used to stop sending riemann events if invoked."
  [mesos-conn & {:keys [interval riemann-host riemann-port]
                 :or {interval 20000}}]
  (log/info "Starting riemann reporter for scheduler.")
  (when riemann-host
    (let [riemann-client (-> (riemann/tcp-client :host riemann-host :port riemann-port)
                             (riemann/batch-client 32))]
      (chime-at (periodic/periodic-seq (time/now) (time/millis interval))
                (fn [time]
                  (let [mesos-db (db mesos-conn)
                        events (make-user-stats-events mesos-db)]
                    (log/debug (format "Sending %s monitor events ..." (count events)))
                    (riemann/send-events riemann-client events)))
                {:error-handler (fn [ex]
                                  (log/error ex "Sending riemann events failed!"))}))))
