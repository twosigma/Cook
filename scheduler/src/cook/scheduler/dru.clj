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
(ns cook.scheduler.dru
  (:require [cook.prometheus-metrics :as prom]
            [cook.scheduler.share :as share]
            [cook.tools :as util]
            [metrics.timers :as timers]
            [swiss.arrows :refer :all]))

(defrecord ScoredTask [task dru mem cpus])

(defn metric-title [metric-name pool-name]
  (if pool-name
    ["cook-mesos" "dru" metric-name (str "pool-" pool-name)]
    ["cook-mesos" "dru" metric-name]))

(defn init-user->dru-divisors
  "Initializes dru divisors map. This map will contain all users that have a running task or pending job"
  [db running-task-ents pending-job-ents pool-name]
  (prom/with-duration
    prom/init-user-to-dry-divisors-duration {:pool pool-name}
    (timers/time!
      (timers/timer (metric-title "init-user->dru-divisors-duration" pool-name))
      (let [all-running-users (map util/task-ent->user running-task-ents)
            all-pending-users (map :job/user pending-job-ents)
            all-users-set (-> #{} (into all-running-users) (into all-pending-users))
            user->dru-divisors (share/get-shares db all-users-set pool-name)]
        user->dru-divisors))))

(defn accumulate-resources
  "Takes a seq of task resources, returns a seq of accumulated resource usage the nth element is the sum of 0..n"
  [task-resources]
  (reductions (fn [resources-sum resources]
                (merge-with + resources-sum resources))
              task-resources))

(defn compute-task-scored-task-pairs
  "Takes a sorted seq of task entities and dru-divisors, returns a list of [task scored-task], preserving the same order of input tasks"
  [{mem-divisor :mem cpus-divisor :cpus} task-ents]
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

(defn compute-sorted-task-cumulative-gpu-score-pairs
  "Takes a sorted seq of task entities and the gpu divisor, returns a list of [task cumulative-gpus], preserving the same order of input tasks"
  [gpu-divisor task-ents]
  (if (seq task-ents)
    (let [task-resources (->> task-ents
                              (map (comp #(select-keys % [:gpus]) util/job-ent->resources :job/_instance)))
          task-cum-gpus (->> task-resources
                             (accumulate-resources)
                             (map (fn [{:keys [gpus]}]
                                    (/ gpus gpu-divisor))))
          scored-tasks (map vector task-ents task-cum-gpus)]
      scored-tasks)
    '()))

(defn sorted-merge
  "Accepts a seq-able datastructure `colls` where each item is seq-able.
   Each seq-able in `colls` is assumed to be sorted based on `key-fn`
   Returns a lazy seq containing the sorted items of `colls`

   Initial code from: http://blog.malcolmsparks.com/?p=42"
  ([key-fn ^java.util.Comparator comp-fn colls]
   (letfn [(next-item [[_ colls]]
             (if-not (seq colls)
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

(defn sorted-task-cumulative-gpu-score-pairs
  "Takes a sorted seq of task entities and the gpu divisor, returns a list of [task cumulative-gpus], preserving the same order of input tasks"
  [user->dru-divisors pool-name user->sorted-running-task-ents]
  (->> user->sorted-running-task-ents
       (map (fn [[user task-ents]]
              (compute-sorted-task-cumulative-gpu-score-pairs (-> user user->dru-divisors :gpus) task-ents)))
       (sorted-merge second)))

(defn sorted-task-scored-task-pairs
  "Returns a lazy sequence of [task,scored-task] pairs sorted by dru in ascending order.
   If jobs have the same dru, any ordering is allowed"
  [user->dru-divisors pool-name user->sorted-running-task-ents]
  (prom/with-duration
    prom/generate-sorted-task-scored-task-pairs-duration {:pool pool-name}
    (timers/time!
      (timers/timer (metric-title "sorted-task-scored-task-pairs-duration" pool-name))
      (->> user->sorted-running-task-ents
           (sort-by first) ; Ensure this function is deterministic
           (map (fn [[user task-ents]]
                  (compute-task-scored-task-pairs (user->dru-divisors user) task-ents)))
           (sorted-merge (comp :dru second))))))

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
                  (apply dissoc <> (user->sorted-running-task-ents user))
                  (into <> (compute-task-scored-task-pairs (user->dru-divisors user) (get user->sorted-running-task-ents' user))))
             remaining-users)
      task->scored-task)))
