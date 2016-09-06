(ns cook.mesos.task
  (:require [plumbing.core :refer (map-vals)]
            [clojure.tools.logging :as log])
  (import com.netflix.fenzo.TaskAssignmentResult))


(defn TaskAssignmentResult->task-info
  "Organizes the info Fenzo has already told us about the task we need to run"
  [^TaskAssignmentResult fenzo-result]
  (merge (:task-info (.getRequest fenzo-result))
         {:ports-assigned (.getAssignedPorts fenzo-result)
          :task-request (.getRequest fenzo-result)}))


(defmulti combine-like-resources
  (fn [list] (-> list first :type)))

(defmethod combine-like-resources :value-scalar [resources]
  (->> resources (map :scalar) (apply +)))

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
        init-state {:remaining-resources avail
                    :mesos-messages []
                    :amount-still-needed amount}]
    (reduce (fn [{:keys [remaining-resources mesos-messages amount-still-needed]
                  :as state} role-name]
              (let [amount-avail (or (remaining-resources role-name) 0)
                    amount-to-take (min amount-still-needed amount-avail)]
                (if (pos? amount-to-take)
                  {:remaining-resources
                   (assoc remaining-resources role-name (- amount-avail amount-to-take))
                   :mesos-messages
                   (conj mesos-messages {:name resource-name
                                         :type :value-scalar
                                         :role role-name
                                         :scalar amount-to-take})
                   :amount-still-needed (- amount-still-needed amount-to-take)}
                  state)))
            init-state
            sorted-roles)))

(defn take-all-scalar-resources-for-task
  [resources task]
  (reduce
    (fn [{:keys [remaining-resources mesos-messages]} [resource-keyword amount]]
      (let [resource-name (name resource-keyword)
            adjustment (take-resources remaining-resources resource-name amount)]
        {:remaining-resources (assoc remaining-resources resource-name (:remaining-resources adjustment))
         :mesos-messages (into mesos-messages (:mesos-messages adjustment))}))
    {:remaining-resources resources
     :mesos-messages []}
    (.getScalarRequests ^com.netflix.fenzo.TaskRequest (:task-request task))))

(defn add-scalar-resources-to-task-infos
  "Given a set of tasks and offers that were matched together by Fenzo,
   assigns the specific scalar resource requirements by role to each task.
   Returns the input tasks, decorated with :scalar-resource-messages"
  [available-resources tasks]
  (:handled-tasks
   (reduce (fn [{:keys [remaining-resources handled-tasks]} task]
             (let [adjustment (take-all-scalar-resources-for-task remaining-resources task)
                   new-task (assoc task :scalar-resource-messages (:mesos-messages adjustment))]
               {:remaining-resources (:remaining-resources adjustment)
                :handled-tasks (conj handled-tasks new-task)}))
           {:remaining-resources available-resources
            :handled-tasks []}
           tasks)))

(defn map->mesos-kv
  "Converts a normal clojure map to a format sometimes employed in mesos messages.
  e.g. {:foo :bar}  ->  [{:key :foo :value :bar}]"
  [m key-name]
  (mapv (fn [kv] {key-name (key kv) :value (val kv)})
        (vec m)))

(defn task-info->mesos-message
  "Given a clojure data structure (based on Cook's internal data format for jobs),
  which has already been decorated with everything we need to know about
  a task, return a Mesos message that will actually launch that task"
  [t]
  {:name (:name t)
   :slave-id (:slave-id t)
   :task-id {:value (:task-id t)}
   :resources (into (:scalar-resource-messages t)
                    (:ports-resource-messages t))

   :labels {:labels (map->mesos-kv (:labels t) :key)}
   :command (update (:command t)
                    :environment
                    (fn [env] {:variables (map->mesos-kv env :name)}))
   :data (com.google.protobuf.ByteString/copyFrom (:data t))})

(defn compile-mesos-messages
  "Given Mesos offers and Fenzo TaskAssignmentResults,
   Returns a vector of Mesos messages that can start the tasks
   suggested by the TaskAssignmentResults"
  [offers task-assignments]
  (let [slave-id (-> offers first :slave-id)
        combined-resource-pool (resources-by-role offers)]
    (->> task-assignments
         (map TaskAssignmentResult->task-info)
         (add-scalar-resources-to-task-infos combined-resource-pool)
         (add-ports-to-task-info combined-resource-pool)
         (map #(assoc % :slave-id slave-id))
         (map task-info->mesos-message))))
