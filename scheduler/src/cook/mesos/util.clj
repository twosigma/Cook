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
            [clojure.walk :as walk]
            [datomic.api :as d :refer (q)]
            [metatransaction.core :refer (db)]
            [metrics.timers :as timers]))

(defn get-all-resource-types
  "Return a list of all supported resources types. Example, :cpus :mem :gpus ..."
  [db]
  (->> (q '[:find ?ident
            :where
            [?e :resource.type/mesos-name ?ident]]
          db)
       (map first)))

(defn categorize-job
  "Return the category of the job. Currently jobs can be :normal or :gpu. This
   is used to give separate queues for scarce & non-scarce resources"
  [job]
  (let [resources (:job/resource job)]
    (if (some #(= :resource.type/gpus (:resource/type %)) resources)
      :gpu
      :normal)))

(defn without-ns
  [k]
  (if (keyword? k)
    (keyword (name k))
    k))

(defn deep-transduce-kv
  "Recursively applies the transducer xf over all kvs in the map or
  any nested maps"
  [xf m]
  (walk/postwalk (fn [x]
                   (if (map? x)
                     (into {} xf x)
                     x))
                 m))

(defn job-ent->container
  "Take a job entity and return its container"
  [db job job-ent]
  (when-let [ceid (:db/id (:job/container job-ent))]
    (->> (d/pull db "[*]" ceid)
         (deep-transduce-kv (comp
                             (filter (comp (partial not= :db/id) first))
                             (map (juxt (comp without-ns first) second)))))))

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
                #{:cpus :mem :gpus} (assoc m resource (:resource/amount r))
                #{:uri} (update-in m [:uris] (fnil conj [])
                                   {:cache (:resource.uri/cache? r false)
                                    :executable (:resource.uri/executable? r false)
                                    :value (:resource.uri/value r)
                                    :extract (:resource.uri/extract? r false)}))))
          {:ports (:job/ports job-ent 0)}
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
        (recur (+ total-cpus (or cpus 0))
               (+ total-mem (or mem 0))
               job-ents))
      {:cpus total-cpus :mem total-mem})))

(timers/deftimer [cook-mesos scheduler get-pending-jobs-duration])

(defn get-pending-job-ents
  ([filtered-db]
   (get-pending-job-ents filtered-db filtered-db))
  ([filtered-db unfiltered-db]
   (timers/time!
     get-pending-jobs-duration
     ;; This function can use an unfiltered db when creating entities
     ;; because a job will either be fully committed or fully not committed,
     ;; therefore if the :job/state attribute was committed the full entity
     ;; is committed and we need not worry about checking commit status
     (->> (q '[:find ?j
               :in $ [?state ...]
               :where
               [?j :job/state ?state]]
             filtered-db [:job.state/waiting])
          (map (fn [[x]] (d/entity unfiltered-db x)))))))

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

(defn job-allowed-to-start?
  "Converts the DB function :job/allowed-to-start? into a predicate"
  [db job]
  (try
    (d/invoke db :job/allowed-to-start? db (:db/id job))
    true
    (catch clojure.lang.ExceptionInfo e
      false)))

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


(defn task->feature-vector
  [task]
  "Vector of comparable features of a task.
   Last two elements are aribitary tie breakers.
   Use :db/id because they guarantee uniqueness for different entities
   (:db/id task) is not sufficient because synthetic task entities don't have :db/id
    This assumes there are at most one synthetic task for a job, otherwise uniqueness invariant will break"
  [(- (:job/priority (:job/_instance task) default-job-priority))
   (:instance/start-time task (java.util.Date. Long/MAX_VALUE))
   (:db/id task)
   (:db/id (:job/_instance task))])

(defn same-user-task-comparator
  "Comparator to order same user's tasks"
  []
  (fn [task1 task2]
      (compare (task->feature-vector task1) (task->feature-vector task2))))

(defn same-user-task-comparator-penalize-backfill
  "Same as same-user-task-comparator, but we treat jobs which were backfilled as being the lowest priority"
  []
  (letfn [(comparable-with-backfill
            [task]
            (vec (cons (if (:instance/backfilled? task) 1 0)
                       (task->feature-vector task))))]
    (fn [task1 task2]
      (compare (comparable-with-backfill task1) (comparable-with-backfill task2)))))

(defn retry-job!
  "Sets :job/max-retries to the given value for the given job UUID.
   Throws an exception if there is no job with that UUID."
  [conn uuid retries]
  (try
    (let [eid (-> (d/entity (d/db conn) [:job/uuid uuid])
                  :db/id)]
      @(d/transact conn
                   [
                    [:db/add [:job/uuid uuid]
                     :job/max-retries retries]

                    ;; If the job is in the "completed" state, put it back into
                    ;; "waiting":
                    [:db.fn/cas [:job/uuid uuid]
                     :job/state (d/entid (d/db conn) :job.state/completed) :job.state/waiting]]))
    ;; :db.fn/cas throws an exception if the job is not already in the "completed" state.
    ;; If that happens, that's fine. We just set "retries" only and continue.
    (catch java.util.concurrent.ExecutionException e
      (if-not (.startsWith (.getMessage e)
                           "java.lang.IllegalStateException: :db.error/cas-failed Compare failed:")
        (throw e)
        @(d/transact conn
                     [[:db/add [:job/uuid uuid]
                       :job/max-retries retries]])))))
