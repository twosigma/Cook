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
  (:require [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [cook.compute-cluster :as cc]
            [cook.config :as config]
            [cook.task :as task]
            [cook.tools :as util]
            [mesomatic.types :as mtypes]
            [plumbing.core :refer (map-vals)])
  (:import com.google.protobuf.ByteString
           com.netflix.fenzo.TaskAssignmentResult))

(def cook-executor-name "cook_executor")
(def cook-executor-source "cook_scheduler_executor")
(defonce custom-executor-name "cook_agent_executor")
(defonce custom-executor-source "cook_scheduler")

(defn- use-custom-executor?
  "Returns true if the job should be scheduled to use the custom executor."
  [job-ent]
  (or (:job/custom-executor job-ent true)
      (= :executor/custom (:job/executor job-ent))))

(defn- cook-executor-candidate?
  "A job is a candidate for execution by the cook-executor if all the following are true:
   a. Cook executor has not been explicitly disabled,
   b. This is going to be the first instance of the job, and
   c. The job UUID hash mod 100 yields less than portion percent."
  [job-ent]
  (let [{:keys [portion retry-limit]} (config/executor-config)]
    (and (nil? (:job/executor job-ent))
         (number? retry-limit)
         (or (= 0 retry-limit) (> retry-limit (count (:job/instance job-ent))))
         (number? portion)
         (> (* portion 100) (-> job-ent :job/uuid hash (mod 100))))))

(defn use-cook-executor?
  "Returns true if the job should be scheduled to use the Cook executor.
   Cook executor is used when the following conditions are true:
   1. The job is not configured to use the custom executor (including backwards compatibility),
   2. The Cook executor command has been configured,
   3. Either :job/executor is explicitly enabled
      Or: the job is a cook-executor candidate (see cook-executor-candidate?)."
  [job-ent]
  (and (not (use-custom-executor? job-ent))
       (:command (config/executor-config))
       (or (= :executor/cook (:job/executor job-ent))
           (cook-executor-candidate? job-ent))))

(defn merge-container-defaults
  "Takes a job container specification and applies any defaults from the config.
   Currently only supports volumes."
  [compute-cluster container]
  (when container
    (let [{:keys [volumes]} (cc/container-defaults compute-cluster)
          get-path (fn [{:keys [container-path host-path]}]
                     (or container-path
                         host-path))]
      (cond-> container
        volumes
        (update :volumes (fn [container-volumes]
                           (let [volumes-to-add (filter (fn [default-volume]
                                                          (not-any? (fn [container-volume]
                                                                      (.startsWith (get-path default-volume)
                                                                                   (get-path container-volume)))
                                                                    container-volumes))
                                                        volumes)]
                             (into (vec container-volumes) volumes-to-add))))))))

(defn job->executor-key
  "Extract the executor key value from the job"
  [job-ent]
  (let [container (util/job-ent->container job-ent)
        ;; If the custom-executor attr isn't set, we default to using a custom
        ;; executor in order to support jobs submitted before we added this field
        custom-executor? (use-custom-executor? job-ent)
        cook-executor? (use-cook-executor? job-ent)]
    ;; executor-key configure whether this is a command or custom executor
    (cond
      (and container cook-executor?) :container-cook-executor
      (and container (not custom-executor?)) :container-command-executor
      (and container custom-executor?) :container-executor
      custom-executor? :custom-executor
      cook-executor? :cook-executor
      ;; use mesos' command executor by default
      :else :command-executor)))

(defn executor-key->executor
  "From the executor key, compute the executor/* parameters."
  [executor-key]
    (case executor-key
      :command-executor :executor/mesos
      :container-command-executor :executor/mesos
      :container-cook-executor :executor/cook
      :cook-executor :executor/cook
      :executor/custom))

(defn job->task-metadata
  "Takes a job entity, returns task metadata"
  [compute-cluster mesos-run-as-user job-ent task-id]
  (let [container (->> job-ent
                       util/job-ent->container
                       (merge-container-defaults compute-cluster))
        cook-executor? (and (cc/use-cook-executor? compute-cluster)
                            (use-cook-executor? job-ent))
        executor-key (job->executor-key job-ent)
        executor (executor-key->executor executor-key)
        resources (util/job-ent->resources job-ent)
        group-uuid (util/job-ent->group-uuid job-ent)
        instance-num (str (count (:job/instance job-ent)))
        environment (cond-> (assoc (util/job-ent->env job-ent)
                              "COOK_INSTANCE_NUM" instance-num
                              "COOK_INSTANCE_UUID" task-id
                              "COOK_JOB_UUID" (-> job-ent :job/uuid str))
                            group-uuid (assoc "COOK_JOB_GROUP_UUID" (str group-uuid))
                            (:cpus resources) (assoc "COOK_JOB_CPUS" (-> resources :cpus str))
                            (:gpus resources) (assoc "COOK_JOB_GPUS" (-> resources :gpus str))
                            (:mem resources) (assoc "COOK_JOB_MEM_MB" (-> resources :mem str))
                            cook-executor? (merge (task/build-executor-environment job-ent)))
        labels (util/job-ent->label job-ent)
        command {:environment environment
                 :uris (cond-> (:uris resources [])
                               (and cook-executor? (get-in (config/executor-config) [:uri :value]))
                               (conj (:uri (config/executor-config))))
                 :user (or mesos-run-as-user (:job/user job-ent))
                 :value (if cook-executor? (:command (config/executor-config)) (:job/command job-ent))}
        ^String data-as-string (if cook-executor?
                                 (json/write-str {"command" (:job/command job-ent)})
                                 (pr-str {:instance instance-num}))
        data (.getBytes data-as-string "UTF-8")]
    (when (and (= :executor/cook (:job/executor job-ent))
               (not= executor-key :cook-executor))
      (log/warn "Task" task-id "requested to use cook executor, but will be executed using" (name executor-key)))
    {:command command
     :container container
     :data data
     :environment environment
     :executor executor
     :executor-key executor-key
     :labels labels
     :name (format "%s_%s_%s" (:job/name job-ent "cookjob") (:job/user job-ent) task-id)
     :num-ports (:ports resources)
     :resources (select-keys resources [:mem :cpus])
     :task-id task-id}))

(defn TaskAssignmentResult->task-metadata
  "Organizes the info Fenzo has already told us about the task we need to run"
  [db mesos-run-as-user compute-cluster ^TaskAssignmentResult task-result]
  (let [{:keys [job task-id] :as task-request} (.getRequest task-result)]
    (merge (job->task-metadata compute-cluster mesos-run-as-user job task-id)
           {:hostname (.getHostname task-result)
            :ports-assigned (vec (sort (.getAssignedPorts task-result)))
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

(defn- assign-port-mappings
  "Assign port mappings from offer. Port n in the cook job should be mapped to the nth
   port taken from the offer."
  [port-mappings ports-assigned]
  (map (fn [{:keys [host-port] :as port-mapping}]
         (if (contains? ports-assigned host-port)
           (assoc port-mapping :host-port (get ports-assigned host-port))
           port-mapping))
       port-mappings))

(defn- cook-mesos-container->mesomatic-mesos-container
  [{:keys [image]}]
  {:image {:type :image-type-docker
           :docker {:name image}}})

(defn task-info->mesos-message
  "Given a clojure data structure (based on Cook's internal data format for jobs),
   which has already been decorated with everything we need to know about
   a task, return a Mesos message that will actually launch that task"
  [framework-id {:keys [command container data executor-key labels name ports-resource-messages
           scalar-resource-messages slave-id task-id ports-assigned]}]
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
                        (update :docker
                                #(set/rename-keys % {:port-mapping :port-mappings}))
                        (update :docker
                                (fn [docker]
                                  (if (:port-mappings docker)
                                    (update docker :port-mappings #(assign-port-mappings % ports-assigned))
                                    docker)))
                        (update :mesos
                                (fn [mesos]
                                  (when mesos
                                    (cook-mesos-container->mesomatic-mesos-container mesos))))
                        (update :volumes
                                (fn [volumes]
                                  (map #(update % :mode cook-volume-mode->mesomatic-volume-mode)
                                       volumes)))))
        executor {:command command
                  :executor-id (mtypes/->ExecutorID (str task-id))
                  :framework-id (mtypes/->FrameworkID framework-id) ; Yes, this is necessary to generate the mesos message.
                  :source custom-executor-source}]
    (cond-> {:data (ByteString/copyFrom data)
             :labels {:labels (map->mesos-kv labels :key)}
             :name name
             :resources (into scalar-resource-messages ports-resource-messages)
             ;; executor-id matches txn code in handle-resource-offer!
             :slave-id slave-id
             :task-id (mtypes/->TaskID (str task-id))}

            (= executor-key :command-executor)
            (assoc :command command)

            (= executor-key :container-command-executor)
            (assoc :command command
                   :container container)

            (= executor-key :container-executor)
            (assoc :executor (assoc executor :container container
                                             :name custom-executor-name))

            (= executor-key :cook-executor)
            (assoc :executor (assoc executor :name cook-executor-name
                                             :source cook-executor-source))

            (= executor-key :container-cook-executor)
            (assoc :executor (assoc executor :name cook-executor-name
                                             :source cook-executor-source
                                             :container container))

            (= executor-key :custom-executor)
            (assoc :executor (assoc executor :name custom-executor-name)))))

(defn compile-mesos-messages
  "Given Mesos offers and partial task-infos created from calling
   TaskAssignmentResult->task-info
   Returns a vector of Mesos messages that can start the tasks
   suggested by the TaskAssignmentResults"
  [framework-id offers task-data-maps]
  (let [slave-id (-> offers first :slave-id)
        combined-resource-pool (resources-by-role offers)]
    (->> task-data-maps
         (add-scalar-resources-to-task-infos combined-resource-pool)
         (add-ports-to-task-info combined-resource-pool)
         (map #(assoc % :slave-id slave-id))
         (map #(task-info->mesos-message framework-id %)))))
