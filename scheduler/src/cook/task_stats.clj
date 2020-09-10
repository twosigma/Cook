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
(ns cook.task-stats
  (:require [clj-time.core :as t]
            [cook.tools :as util]
            [datomic.api :as d]
            [plumbing.core :as pc]))

(defn- task->task-entry
  "Updates the provided stats map with the data from the provided task entity"
  [task-ent]
  (let [job-ent (:job/_instance task-ent)
        user (:job/user job-ent)
        resources (util/job-ent->resources job-ent)
        run-time (-> task-ent util/task-run-time)
        run-time-seconds (-> run-time .toDuration .getMillis (/ 1000.0))]
    (assoc resources
      :cpu-seconds (* run-time-seconds (:cpus resources))
      :mem-seconds (* run-time-seconds (:mem resources))
      :reason (get-in task-ent [:instance/reason :reason/string])
      :run-time-seconds run-time-seconds
      :user user)))

(defn- get-tasks
  "Gets all tasks that started in the specified time range and with the specified status."
  [db instance-status start end name-filter-fn]
  (let [start-entity-id (d/entid-at db :db.part/user (.toDate (t/minus start (t/hours 1))))
        end-entity-id (d/entid-at db :db.part/user (.toDate (t/plus end (t/hours 1))))
        instance-status-entid (d/entid db instance-status)
        instance-status-attribute-entid (d/entid db :instance/status)
        start-millis (.getTime (.toDate start))
        end-millis (.getTime (.toDate end))
        tasks
        (->> (d/seek-datoms db :avet :instance/status instance-status-entid start-entity-id)
             (take-while #(and
                            (< (.e %) end-entity-id)
                            (= (.a %) instance-status-attribute-entid)
                            (= (.v %) instance-status-entid)))
             (map #(.e %))
             (map (partial d/entity db))
             (filter #(<= start-millis (.getTime (:instance/start-time %))))
             (filter #(< (.getTime (:instance/start-time %)) end-millis)))]
    (cond->> tasks
             name-filter-fn (filter #(name-filter-fn (-> % :job/_instance :job/name))))))

(defn- percentile
  "Calculates the p-th percentile of the values in coll
  (where 0 < p <= 100), using the Nearest Rank method:
  https://en.wikipedia.org/wiki/Percentile#The_Nearest_Rank_method
  Assumes that coll is sorted (see percentiles below for context)"
  [coll p]
  (if (or (empty? coll) (not (number? p)) (<= p 0) (> p 100))
    nil
    (nth coll
         (-> p
             (/ 100)
             (* (count coll))
             (Math/ceil)
             (dec)))))

(defn percentiles
  "Calculates the p-th percentiles of the values in coll for
  each p in p-list (where 0 < p <= 100), and returns a map of
  p -> value"
  [coll & p-list]
  (let [sorted (sort coll)]
    (into {} (map (fn [p] [p (percentile sorted p)]) p-list))))

(defn- generate-stats
  "Generates statistics based on the provided task entries"
  [task-entries]
  (if (pos? (count task-entries))
    (let [stats (fn [xs] {:percentiles (percentiles xs 50 75 95 99 100)
                          :total (reduce + xs)})]
      {:count (count task-entries)
       :cpu-seconds (stats (map :cpu-seconds task-entries))
       :mem-seconds (stats (map :mem-seconds task-entries))
       :run-time-seconds (stats (map :run-time-seconds task-entries))})
    {}))

(defn- generate-all-stats
  "Generates both aggregate and per-user statistics for the provided tasks"
  [tasks]
  (let [stats-by (fn [k ts] (->> ts (group-by k) (pc/map-vals generate-stats)))
        task-entries (map task->task-entry tasks)
        overall-stats (generate-stats task-entries)
        reason-stats (stats-by :reason task-entries)
        user->tasks (group-by :user task-entries)
        user-reason-stats (->> user->tasks (pc/map-vals #(stats-by :reason %)))
        leaders (fn [k]
                  (->> task-entries
                       (stats-by :user)
                       (map (fn [[u m]] [(get-in m [k :total]) u]))
                       (sort-by #(* -1 (first %)))
                       (take 10)
                       (map (fn [[n u]] [u n]))
                       (into {})))]
    {:by-reason reason-stats
     :by-user-and-reason user-reason-stats
     :leaders {:cpu-seconds (leaders :cpu-seconds)
               :mem-seconds (leaders :mem-seconds)}
     :overall overall-stats}))

(defn get-stats
  "Returns a map containing task stats for tasks
  with the given status from the provided time range"
  [conn instance-status start end name-filter-fn]
  (let [tasks (get-tasks (d/db conn) instance-status start end name-filter-fn)]
    (generate-all-stats tasks)))
