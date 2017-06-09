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
(ns cook.mesos.task
  (:require [cook.mesos.util :as util]
            [plumbing.core :refer (map-vals)])
  (:import com.netflix.fenzo.TaskAssignmentResult))

(defonce custom-executor-name "cook_agent_executor")
(defonce custom-executor-source "cook_scheduler")

(defn job->task-metadata
  "Takes a job entity, returns task metadata"
  [db framework-id job-ent task-id]
  (let [resources (util/job-ent->resources job-ent)
        container (util/job-ent->container db job-ent)
        ;; If the custom-executor attr isn't set, we default to using a custom
        ;; executor in order to support jobs submitted before we added this field
        custom-executor? (:job/custom-executor job-ent true)
        environment (util/job-ent->env job-ent)
        labels (util/job-ent->label job-ent)
        command {:environment environment
                 :uris (:uris resources [])
                 :user (:job/user job-ent)
                 :value (:job/command job-ent)}
        ;; executor-key configure whether this is a command or custom executor
        executor-key (if custom-executor? :executor :command)
        ;;TODO this data is a race-condition
        data (.getBytes
               (pr-str
                 {:instance (str (count (:job/instance job-ent)))})
               "UTF-8")]
    {:command command
     :container container
     :data data
     :environment environment
     :executor-key executor-key
     :framework-id framework-id
     :labels labels
     :name (format "%s_%s_%s" (:job/name job-ent "cookjob") (:job/user job-ent) task-id)
     :num-ports (:ports resources)
     :resources (select-keys resources [:mem :cpus])
     :task-id task-id}))

(defn TaskAssignmentResult->task-metadata
  "Organizes the info Fenzo has already told us about the task we need to run"
  [db framework-id ^TaskAssignmentResult fenzo-result]
  (let [task-request (.getRequest fenzo-result)]
    (merge (job->task-metadata db framework-id (:job task-request) (:task-id task-request))
           {:ports-assigned (.getAssignedPorts fenzo-result)
            :task-request task-request})))

(defmulti combine-like-resources
  (fn [list] (-> list first :type)))

(defmethod combine-like-resources :value-scalar [resources]
  (->> resources (map :scalar) (reduce +)))

(defmethod combine-like-resources :value-ranges [resources]
  (->> resources (map :ranges) (reduce into)))

(defn resources-by-role
  "Given a set of offers, combine all the available resources into a comprehensible
  and easily walkable data strucuture, grouped first by resource name e.g. mem, then
  by role."
  [offers]
  (->> offers
       (map :resources)
       flatten
       (group-by :name)
       (map-vals (fn [resources]
                   (->> resources
                        (group-by :role)
                        (map-vals combine-like-resources))))))

(defn range-contains?
  "true iff val is contained by a mesos-style range e.g. {:begin 1 :end 10}"
  [val mesos-range]
  (and (<= (:begin mesos-range) val)
       (>= (:end mesos-range) val)))

(defn role-containing-port
  "in: {\"*\" [{:begin 201 :end 202}] \"cook\" [{:begin 203 :end 204}]}, 201
  ;; out \"*\""
  [available-ports port]
  (->> available-ports
       seq
       (filter #(some (partial range-contains? port) (val %)))
       ffirst))

(defn take-ports
  "Given a set of available resources (in the format returned by resources-by-role),
  returns a vector of mesos messages that will reserve the specified ports.
  Note: unlike with scalar-resources, available-resources don't need to be
  changed by this function, because Fenzo has already provided us with
  specific ports to use for every task."
  [available-resources ports-needed]
  (mapv (fn [port]
          {:name "ports"
           :type :value-ranges
           :role (role-containing-port available-resources port)
           :ranges [{:begin port :end port}]})
        ports-needed))

(defn add-ports-to-task-info
  "Given a set of tasks and offers that were matched together by Fenzo,
   assigns the specific ports requested by role to each task.
   Returns the input tasks, decorated with :ports-resource-messages"
  [available-resources tasks]
  (let [available-ports (available-resources "ports")]
    (map (fn [task]
           (let [ports (:ports-assigned task)
                 port-env-vars (into {} (map-indexed (fn [i p] [(str "PORT" i) (str p)])
                                                     ports))]
             (-> task
                 (assoc :ports-resource-messages (take-ports available-ports ports))
                 (update-in [:command :environment] merge port-env-vars))))
         tasks)))

(defn take-resources
  "Given a set of available resources (in the format returned by resources-by-role),
  take the specified amount of the specified resource from the pool.
  Return {:remaining-resources (the pool after resources were taken)
          :mesos-messages (the Mesos messages that are necessary to reserve the resources)
          :amount-still-needed (the amount still needed to satisfy amount... if it's not 0, something went wrong)}"
  [available-resources resource-name amount]
  (let [avail (available-resources resource-name)
        ;; to be polite to other frameworks, take from role-specific pool first.
        sorted-roles (sort-by #(= "*" %) (keys avail))
        init-state {:amount-still-needed amount
                    :mesos-messages []
                    :remaining-resources avail}]
    (reduce (fn [{:keys [amount-still-needed mesos-messages remaining-resources]
                  :as state} role-name]
              (let [amount-avail (or (remaining-resources role-name) 0)
                    amount-to-take (min amount-still-needed amount-avail)]
                (if (pos? amount-to-take)
                  {:amount-still-needed (- amount-still-needed amount-to-take)
                   :mesos-messages
                   (conj mesos-messages {:name resource-name
                                         :role role-name
                                         :scalar amount-to-take
                                         :type :value-scalar})
                   :remaining-resources
                   (assoc remaining-resources role-name (- amount-avail amount-to-take))}
                  state)))
            init-state
            sorted-roles)))

(defn take-all-scalar-resources-for-task
  [resources task]
  (reduce
    (fn [{:keys [mesos-messages remaining-resources]} [resource-keyword amount]]
      (let [resource-name (name resource-keyword)
            adjustment (take-resources remaining-resources resource-name amount)]
        {:mesos-messages (into mesos-messages (:mesos-messages adjustment))
         :remaining-resources (assoc remaining-resources resource-name (:remaining-resources adjustment))}))
    {:mesos-messages []
     :remaining-resources resources}
    (.getScalarRequests ^com.netflix.fenzo.TaskRequest (:task-request task))))

(defn add-scalar-resources-to-task-infos
  "Given a set of tasks and offers that were matched together by Fenzo,
   assigns the specific scalar resource requirements by role to each task.
   Returns the input tasks, decorated with :scalar-resource-messages"
  [available-resources tasks]
  (:handled-tasks
    (reduce (fn [{:keys [handled-tasks remaining-resources]} task]
              (let [adjustment (take-all-scalar-resources-for-task remaining-resources task)
                    new-task (assoc task :scalar-resource-messages (:mesos-messages adjustment))]
                {:handled-tasks (conj handled-tasks new-task)
                 :remaining-resources (:remaining-resources adjustment)}))
            {:handled-tasks []
             :remaining-resources available-resources}
            tasks)))

(defn map->mesos-kv
  "Converts a normal clojure map to a format sometimes employed in mesos messages.
  e.g. {:foo :bar}  ->  [{:key :foo :value :bar}]"
  [m key-name]
  (mapv (fn [kv] {key-name (key kv) :value (val kv)})
        (vec m)))

;; TODO: Throw an exception if value is unexpected
(def cook-network->mesomatic-network
  "Converts the string representation of network used in the containerinfo to
   a value mesomatic understands"
  {"HOST" :docker-network-host
   "BRIDGE" :docker-network-bridge
   "NONE" :docker-network-none})

(def cook-volume-mode->mesomatic-volume-mode
  "Converts the string representation of volume mode to a value mesomatic understands"
  {"RW" :volume-rw
   "RO" :volume-ro
   ;; nil volume produces a Mesomatic serializion error
   nil :volume-ro})

(def cook-container-type->mesomatic-container-type
  "Converts the string representation of container type to a value mesomatic understands"
  {"DOCKER" :container-type-docker
   "MESOS" :container-type-mesos})

(defn task-info->mesos-message
  "Given a clojure data structure (based on Cook's internal data format for jobs),
   which has already been decorated with everything we need to know about
   a task, return a Mesos message that will actually launch that task"
  [{:keys [command container data executor-key framework-id labels name ports-resource-messages
           scalar-resource-messages slave-id task-id]}]
  (let [command (update command
                        :environment
                        (fn [env] {:variables (map->mesos-kv env :name)}))
        container (when container
                    (-> container
                        (update :type cook-container-type->mesomatic-container-type)
                        (update :docker
                                (fn [docker]
                                  (if (:network docker)
                                    (update docker :network cook-network->mesomatic-network)
                                    docker)))
                        (update :volumes
                                (fn [volumes]
                                  (map #(update % :mode cook-volume-mode->mesomatic-volume-mode)
                                       volumes)))))]
    (merge {:data (com.google.protobuf.ByteString/copyFrom data)
            :labels {:labels (map->mesos-kv labels :key)}
            :name name
            :resources (into scalar-resource-messages
                             ports-resource-messages)
            :slave-id slave-id
            :task-id {:value task-id}
            executor-key (if (= executor-key :executor)
                           ;; executor-id matches txn code in handle-resource-offer
                           (merge {:command command
                                   :executor-id {:value (str task-id)}
                                   :framework-id framework-id
                                   :name custom-executor-name
                                   :source custom-executor-source}
                                  (when (seq container)
                                    {:container container}))
                           command)}
           (when (and (seq container) (not= executor-key :executor))
             {:container container}))))

(defn compile-mesos-messages
  "Given Mesos offers and partial task-infos created from calling
   TaskAssignmentResult->task-info
   Returns a vector of Mesos messages that can start the tasks
   suggested by the TaskAssignmentResults"
  [offers task-data-maps]
  (let [slave-id (-> offers first :slave-id)
        combined-resource-pool (resources-by-role offers)]
    (->> task-data-maps
         (add-scalar-resources-to-task-infos combined-resource-pool)
         (add-ports-to-task-info combined-resource-pool)
         (map #(assoc % :slave-id slave-id))
         (map task-info->mesos-message))))

