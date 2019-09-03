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
(ns cook.task
  (:require
    [datomic.api :as d]
    [cook.compute-cluster :as cc]))

(defn task-entity-id->task-id
  "Given a task entity, what is the task-id associated with it?"
  [db task-entity-id]
  (->> task-entity-id
       (d/entity db)
       :instance/task-id))

(defn task-entity->compute-cluster-name
  "Get the compute-cluster name from a task-entity"
  [task-ent]
  (-> task-ent
      :instance/compute-cluster
      :compute-cluster/cluster-name))

(defn task-entity-id->compute-cluster-name
  "Given a task entity, what is the compute cluser name associated with it?"
  [db task-entity-id]
  (->> task-entity-id
       (d/entity db)
       task-entity->compute-cluster-name))

(defn task-ent->ComputeCluster
  "Given a task entity, return the compute cluster object for it, if it exists. May return nil if that compute cluster on the task
  is no longer in service."
  [task-ent]
  (some-> task-ent
          task-entity->compute-cluster-name
          cc/compute-cluster-name->ComputeCluster))
