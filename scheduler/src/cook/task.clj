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
  (:require [cook.compute-cluster :as cc]
            [cook.config :as config]
            [cook.config-incremental :as config-incremental]
            [datomic.api :as d]
            [plumbing.core :refer [for-map]]))

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

(defn get-compute-cluster-for-task-ent-if-present
  "Like task-ent->ComputeCluster but do not log errors when there is no cluster"
  [task-ent]
  (some-> task-ent
    task-entity->compute-cluster-name
    (@cc/cluster-name->compute-cluster-atom)))

(def progress-meta-env-name
  "Meta environment variable name for declaring the environment variable
   that stores the path of the progress output file."
  "EXECUTOR_PROGRESS_OUTPUT_FILE_ENV")

(def progress-meta-env-value
  "Default name of the the environment variable stores the path of the progress output file,
   used as the default value for the meta-env-var described above."
  "EXECUTOR_PROGRESS_OUTPUT_FILE_NAME")

(def default-progress-env-name
  "Default name of the the environment variable stores the path of the progress output file,
   only used when the meta-env-var is not set. Must correspond to the default file name in the executor."
  "EXECUTOR_PROGRESS_OUTPUT_FILE")

(defn build-executor-environment
  "Build the environment for the job's executor and/or progress monitor."
  [{:keys [:job/uuid] :as job-ent}]
  (let [{:keys [default-progress-regex-string environment log-level max-message-length
                progress-sample-interval-ms incremental-config-environment] :as executor-config} (config/executor-config)
        progress-output-file (:job/progress-output-file job-ent)]
    (cond-> environment
      (seq executor-config)
      (assoc
        "EXECUTOR_LOG_LEVEL" log-level
        "EXECUTOR_MAX_MESSAGE_LENGTH" max-message-length
        "PROGRESS_REGEX_STRING" (:job/progress-regex-string job-ent default-progress-regex-string)
        "PROGRESS_SAMPLE_INTERVAL_MS" progress-sample-interval-ms)
      progress-output-file
      (assoc progress-meta-env-name progress-meta-env-value
             progress-meta-env-value progress-output-file)
      incremental-config-environment
      (merge (for-map [[key value] incremental-config-environment]
               key
               (or (config-incremental/resolve-incremental-config uuid value) ""))))))
