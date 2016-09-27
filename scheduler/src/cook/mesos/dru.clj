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
  (:require [cook.mesos.util :as util]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async]
            [cook.mesos.share :as share]
            [clojure.core.reducers :as r]
            [metrics.timers :as timers]
            [swiss.arrows :refer :all]))

(defrecord ScoredTask [task dru mem cpus])

(timers/deftimer [cook-mesos dru init-user->dru-divisors-duration])

(defn init-user->dru-divisors
  "Initializes dru divisors map. This map will contain all users that have a running task or pending job"
  [db running-task-ents pending-job-ents]
  (timers/time!
    init-user->dru-divisors-duration
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
    user->dru-divisors)))

(defn accumulate-resources
  "Takes a seq of task resources, returns a seq of accumulated resource usage the nth element is the sum of 0..n"
  [task-resources]
  (reductions (fn [resources-sum resources]
                (merge-with + resources-sum resources))
              task-resources))

(defn compute-task-scored-task-pairs
  "Takes a sorted seq of task entities and dru-divisors, returns a list of [task scored-task], preserving the same order of input tasks"
  [task-ents {mem-divisor :mem cpus-divisor :cpus}]
  (if (seq task-ents)
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
      scored-tasks)
    '()))

(defn compute-gpu-task-scored-task-pair
  "Takes a sorted seq of task entities and the gpu divisor, returns a list of [task cumulative-gpus], preserving the same order of input tasks"
  [task-ents gpu-divisor]
  (let [task-resources (->> task-ents
                            (map (comp #(select-keys % [:gpus]) util/job-ent->resources :job/_instance)))
        task-cum-gpus (->> task-resources
                           (accumulate-resources)
                           (map (fn [{:keys [gpus]}]
                                  (/ gpus gpu-divisor))))
        scored-tasks (map (fn [task cum-gpus]
                            [task cum-gpus])
                          task-ents
                          task-cum-gpus)]
    scored-tasks))

(defn sorted-merge
  "Accepts a seq-able datastructure `colls` where each item is seq-able.
   Each seq-able in `colls` is assumed to be sorted based on `key-fn`
   Returns a lazy seq containing the sorted items of `colls`

   Initial code from: http://blog.malcolmsparks.com/?p=42"
  ([key-fn ^java.util.Comparator comp-fn colls]
   (letfn [(next-item [[_ colls]]
             (if (nil? colls)
               [:end nil] ; Allow nil items in colls
               (let [[[yield & remaining] & other-colls]
                     (sort-by (comp key-fn first) comp-fn colls)]
                 [yield (if remaining (cons remaining other-colls) other-colls)])))]
     (->> colls
       (vector :begin) ; next-item input is [item colls], need initial item
       (iterate next-item)
       (drop 1) ; Don't care about :begin
       (map first)
       (take-while (partial not= :end)))))
  ([key-fn colls]
   (sorted-merge key-fn compare colls))
  ([colls]
   (sorted-merge identity compare colls)))

(defn gpu-task-scored-task-pairs
  "Takes a sorted seq of task entities and the gpu divisor, returns a list of [task cumulative-gpus], preserving the same order of input tasks"
  [user->sorted-running-task-ents user->dru-divisors]
  (->> user->sorted-running-task-ents
       (map (fn [[user task-ents]]
              (compute-gpu-task-scored-task-pair
                task-ents (get-in user->dru-divisors [user :gpus]))))
       (sorted-merge second)))

(timers/deftimer [cook-mesos dru sorted-task-scored-task-pairs-duration])

(defn sorted-task-scored-task-pairs
  "Returns a lazy sequence of [task,scored-task] pairs sorted by dru in ascending order.
   If jobs have the same dru, any ordering is allowed"
  [user->sorted-running-task-ents user->dru-divisors]
  (timers/time!
    sorted-task-scored-task-pairs-duration
    (->> user->sorted-running-task-ents
       (map (fn [[user task-ents]]
              (compute-task-scored-task-pairs task-ents (get user->dru-divisors user))))
       (sorted-merge (comp :dru second)))))

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
