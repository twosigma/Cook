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
(ns cook.mesos.util
  (:require [clojure.tools.logging :as log]
            [datomic.api :as d :refer (q)]
            [metatransaction.core :refer (db)]
            [metrics.timers :as timers]))

(defn get-all-resource-types
  "Return a list of all supported resources types. Example, :cpus :mem ..."
  [db]
  (->> (q '[:find ?ident
            :where
            [?e :resource.type/mesos-name ?ident]]
          db)
       (map first)))

(defn job-ent->env
  "Take a job entity and return the environment variable map"
  [job-ent]
  (reduce (fn [m env-var]
            (assoc m
                   (:environment/name env-var)
                   (:environment/value env-var)))
          {}
          (:job/environment job-ent)))

(defn job-ent->label
  "Take a job entity and return the label map"
  [job-ent]
  (reduce (fn [m label-var]
            (assoc m
                   (:label/key label-var)
                   (:label/value label-var)))
          {}
          (:job/label job-ent)))

(defn job-ent->resources
  "Take a job entity and return a resource map. NOTE: the keys must be same as mesos resource keys"
  [job-ent]
  (reduce (fn [m r]
            (let [resource (keyword (name (:resource/type r)))]
              (condp contains? resource
                #{:cpus :mem} (assoc m resource (:resource/amount r))
                #{:uri} (update-in m [:uris] (fnil conj [])
                                   {:cache (:resource.uri/cache? r false)
                                    :executable (:resource.uri/executable? r false)
                                    :value (:resource.uri/value r)
                                    :extract (:resource.uri/extract? r false)}))))
          {:ports (:job/port job-ent)}
          (:job/resource job-ent)))

(defn sum-resources-of-jobs
  "Given a collections of job entities, returns the total resources they use
   {:cpus cpu :mem mem}"
  [job-ents]
  (loop [total-cpus 0.0
         total-mem 0.0
         [job-ent & job-ents] job-ents]
    (if job-ent
      (let [{:keys [cpus mem]} (job-ent->resources job-ent)]
        (recur (+ total-cpus cpus)
               (+ total-mem mem)
               job-ents))
      {:cpus total-cpus :mem total-mem})))

(timers/deftimer [cook-mesos scheduler get-pending-jobs-duration])

(defn get-pending-job-ents
  [db]
  (timers/time!
    get-pending-jobs-duration
    (->> (q '[:find ?j
              :in $ [?state ...]
              :where
              [?j :job/state ?state]]
            db [:job.state/waiting])
         (map (fn [[x]] (d/entity db x))))))

(timers/deftimer [cook-mesos scheduler get-running-tasks-duration])

(defn get-running-task-ents
  "Returns all running task entities"
  [db]
  (timers/time!
    get-running-tasks-duration
    (->> (q '[:find ?i
              :in $ [?status ...]
              :where
              [?i :instance/status ?status]]
            db [:instance.status/running :instance.status/unknown])
         (map (fn [[x]] (d/entity db x))))))

(defn create-task-ent
  "Takes a pending job entity and returns a synthetic running task entity for that job"
  [pending-job-ent & {:keys [hostname] :or {hostname nil}}]
  {:job/_instance pending-job-ent
   :instance/status :instance.status/running
   :instance/hostname hostname})

(defn task-ent->user
  [task-ent]
  (get-in task-ent [:job/_instance :job/user]))

(def ^:const default-job-priority 50)

(defn same-user-task-comparator
  "Comparator to order same user's tasks"
  [task1 task2]
  (letfn [(task->feature-vector [task]
            ;; Last two elements are aribitary tie breakers.
            ;; Use :db/id because they guarantee uniqueness for different entities
            ;; (:db/id task) is not sufficient because synthetic task entities don't have :db/id
            ;; This assumes there are at most one synthetic task for a job, otherwise uniqueness invariant will break
            [(- (:job/priority (:job/_instance task) default-job-priority))
             (:instance/start-time task (java.util.Date. Long/MAX_VALUE))
             (:db/id task)
             (:db/id (:job/_instance task))])]
    (compare (task->feature-vector task1) (task->feature-vector task2))))
