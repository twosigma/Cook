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
(ns cook.mesos.dru
  (:require [clj-mesos.scheduler :as mesos]
            [cook.mesos.util :as util]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async]
            [cook.mesos.share :as share]
            [clojure.core.reducers :as r]
            [swiss.arrows :refer :all]
            [clojure.data.priority-map :as pm]))

(defrecord ScoredTask [task dru mem cpus])

(defn init-user->dru-divisors
  "Initializes dru divisors map. This map will contain all users that have a running task or pending job"
  [db running-task-ents pending-job-ents]
  (let [all-running-users (->> running-task-ents
                               (map util/task-ent->user)
                               (into #{}))
        all-pending-users (->> pending-job-ents
                               (map :job/user)
                               (into #{}))
        all-users (clojure.set/union all-running-users all-pending-users)
        user->dru-divisors (->> all-users
                                (map (fn [user]
                                       [user (share/get-share db user)]))
                                (into {}))]
    user->dru-divisors))

(defn accumulate-resources
  "Takes a seq of task resources, returns a seq of accumulated resource usage the nth element is the sum of 0..n"
  [task-resources]
  (reductions (fn [resources-sum resources]
                (merge-with + resources-sum resources))
              task-resources))

(defn compute-task-scored-task-pairs
  "Takes a sorted seq of task entities and dru-divisors, returns a list of [task scored-task], preserving the same order of input tasks"
  [task-ents {mem-divisor :mem cpus-divisor :cpus}]
  (let [task-resources (->> task-ents
                            (map (comp #(select-keys % [:cpus :mem]) util/job-ent->resources :job/_instance)))
        task-drus (->> task-resources
                       (accumulate-resources)
                       (map (fn [{:keys [mem cpus]}]
                              (max (/ mem mem-divisor) (/ cpus cpus-divisor)))))
        scored-tasks (map (fn [task dru {:keys [mem cpus]}]
                            [task (->ScoredTask task dru mem cpus)])
                          task-ents
                          task-drus
                          task-resources)]
    scored-tasks))

(defn scored-task->priority
  "The higher the dru of task, the lower the priority"
  [scored-task]
  (- (:dru scored-task)))

(defn init-task->scored-task
  "Returns a priority-map from task to scored-task sorted by -dru"
  [user->sorted-running-task-ents
   user->dru-divisors]
  (->> user->sorted-running-task-ents
       (r/mapcat (fn [[user task-ents]]
                   (compute-task-scored-task-pairs task-ents (get user->dru-divisors user))))
       (into (pm/priority-map-keyfn scored-task->priority))))

(defn next-task->scored-task
  "Computes the priority-map from task to scored-task sorted by -dru for the next cycle.
   For each user that has changed in the current cycle, replace the scored-task mapping with the updated one"
  [task->scored-task
   user->sorted-running-task-ents
   user->sorted-running-task-ents'
   user->dru-divisors
   changed-users]
  (loop [task->scored-task task->scored-task
         [user & remaining-users] (seq changed-users)]
    (if user
      ;; priority-map doesn't support transients :(
      (recur (-<> task->scored-task
                  (apply dissoc <> (get user->sorted-running-task-ents user))
                  (into <> (compute-task-scored-task-pairs (get user->sorted-running-task-ents' user) (get user->dru-divisors user))))
             remaining-users)
      task->scored-task)))
