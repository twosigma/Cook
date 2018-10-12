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
(ns cook.mesos.mesos-mock
  (:require [chime :refer [chime-at chime-ch]]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [cook.mesos.util :as util]
            [datomic.api :refer (q)]
            [mesomatic.scheduler :as mesos]
            [mesomatic.types :as mesos-type]
            [plumbing.core :refer (map-vals map-from-vals)])
  (import org.apache.mesos.Protos$Status
          org.apache.mesos.SchedulerDriver))

(def resource->type {:cpus :scalar
                     :mem :scalar
                     :ports :ranges})

(defn make-offer
  "Takes a `host` which contain a key `:available-resources`
   and generates a list of offers

   Parameters:
   `host` host info, see above for a description of the schema"
  [host]
  (-> host
      (assoc :id {:value (str (java.util.UUID/randomUUID))})
      (update :slave-id (fn [slave-id] {:value slave-id}))
      (dissoc :available-resources)
      (assoc :resources
             (for [[resource-name role->resource] (:available-resources host)
                   [role resource] role->resource]
               {:name (name resource-name)
                :role role
                (resource->type resource-name) resource
                :type (keyword (str "value-" (name (resource->type resource-name))))}))))

(defn clear-out-resources
  "Removes all the resources in a resource map passed in.
   It is assumed this is a called on available-resources

   Parameters:
   `resources` resource map of the form {<resource> {<role> {<quantity}}

   Returns:
   resource map in form {<resource> {}}"
  [resources]
  (into {} (map #(vector % {})) (keys resources)))

(defn prepare-new-offers
  "Prepares new offers to send to the scheduler and returns an updated state
   given the new offers

   Parameters:
   `state` the state of the mock mesos, see above for a description of the schema

   returns [new-offers new-state]"
  [{:keys [slave-id->host] :as state}]
  (let [new-offers (filter (comp seq :resources)
                           (map make-offer (vals slave-id->host)))
        new-offers (filter (fn [{:keys [resources]}]
                             (some #(pos? (get % :scalar 0)) resources))
                           new-offers)]
    [new-offers
     (-> state
         (update :slave-id->host
                 #(map-vals (fn [host]
                              (update host :available-resources clear-out-resources))
                            %))
         (update :offer-id->offer #(into % (map-from-vals (comp :value :id) new-offers))))]))


(defn combine-ranges
  "Takes a list of ranges specified as: {:begin <num> :end <num2>} and
   returns a new list where ranges are consecutive.

   Parameters:
   `ranges` list of ranges {:being <num1> :end <num2>}"
  [ranges]
  (let [ranges (sort-by :begin ranges)]
    (reduce (fn [ranges' new-range]
              (let [last-range (last ranges')]
                (if last-range
                  (if (= (inc (:end last-range)) (:begin new-range))
                    (conj (vec (butlast ranges')) {:begin (:begin last-range)
                                                   :end (:end new-range)})
                    (conj ranges' new-range))
                  (conj ranges' new-range))))
            []
            ranges)))

(defn range-contains?
  "Returns true if range-a contains range-b"
  [range-a range-b]
  (and (<= (:begin range-a) (:begin range-b))
       (>= (:end range-a) (:end range-b))))

(defn ranges-contains?
  "Returns true if ranges contains the considered range"
  [ranges range-b]
  (some #(range-contains? % range-b) ranges))

(defn subtract-range
  "Given range `a` and range `b`, returns a list of ranges {:begin <num> :end <num2>}
   such that the values in `b` are removed from `a`"
  [range-a range-b]
  (when-not (range-contains? range-a range-b)
    (throw (ex-info "Range a must contain range b"
                    {:range-a range-a :range-b range-b})))
  ;; ----------- range-a
  ;;    --       range-b
  ;; ---  ------ out
  ;; KB     KE
  (let [keep-begin {:begin (:begin range-a) :end (dec (:begin range-b))}
        keep-end {:begin (inc (:end range-b)) :end (:end range-a)}]
    (cond
      (and (= (:begin range-a) (:begin range-b))
           (= (:end range-a) (:end range-b)))
      []

      (= (:begin range-a) (:begin range-b))
      [keep-end]

      (= (:end range-a) (:end range-b))
      [keep-begin]

      :else
      [keep-begin keep-end])))

(defn subtract-ranges
  "Given ranges `a` and `b`, each a list of ranges,
   returns a list of ranges such that `b` is removed from `a`"
  [ranges-a ranges-b]
  (when-not (every? (partial ranges-contains? ranges-a) ranges-b)
    (throw (ex-info "ranges-a must contain ranges-b"
                    {:ranges-a ranges-a :ranges-b ranges-b})))
  (reduce (fn [ranges range-to-remove]
            (mapcat
              (fn [r]
                (if (range-contains? r range-to-remove)
                  (subtract-range r range-to-remove)
                  [r]))
              ranges))
          ranges-a
          ranges-b))

(defn combine-resources
  "Given a list of resources in the shape
   {:role <role> :type <resource-type> :name <resource_name> :<resource-type-val> <resource-val>}

   Combine the resources into a map in the shape
   {<resource-name> {<role> <value>}
   ...}"
  ([resources]
   (combine-resources resources {}))
  ([resources initial]
   (reduce (fn [avail {:keys [name role] :as resource-info}]
             (update-in avail
                        [(keyword name) role]
                        (fn [resource-val]
                          (condp = (resource->type (keyword name))
                            :scalar (+ (or resource-val 0) (:scalar resource-info))
                            :ranges (combine-ranges (into (or resource-val [])
                                                          (:ranges resource-info)))))))
           initial
           resources)))


(defn subtract-resources
  "Given two resource maps in the shape
   {<resource-name> {<role> <value>}
   ...}

   Return a single resource map in the same shape where the
   resources of `b` are removed from `a`
   "
  [resource-map-a resource-map-b]
  (when-not (clojure.set/superset? (set (keys resource-map-a)) (set (keys resource-map-b)))
    (throw (ex-info (str "A must contain a superset of resources of B")
                    {:a-resources (keys resource-map-a)
                     :b-resources (keys resource-map-b)})))
  (->> (for [resource (keys resource-map-a)]
         (let [role->resource-a (get resource-map-a resource)
               role->resource-b (get resource-map-b resource)
               _ (when-not (every? #(contains? (set (keys role->resource-a)) %)
                                   (keys role->resource-b))
                   (throw (ex-info "Every role to subtract must exist in `a`"
                                   {:roles-a (keys role->resource-a)
                                    :roles-b (keys role->resource-b)})))
               role->resource' (condp = (resource->type resource)
                                 :scalar (merge-with - role->resource-a role->resource-b)
                                 :ranges (merge-with subtract-ranges role->resource-a role->resource-b))]
           [resource role->resource']))
       (into {})))

(defn complete-task!
  "Marks the task as complete in the state and updates the resources
   Returns the updated state"
  ([state task-id scheduler driver]
   (complete-task! state task-id scheduler driver nil))
  ([state task-id scheduler driver task-state]
   (log/debug "Completing task" {:task-id task-id :state task-state :task (get-in state [:task-id->task task-id])})
   (if-let [task (get-in state [:task-id->task task-id])]
     (let [task->complete-status (get-in state [:config :task->complete-status])
           task-state (or task-state (task->complete-status task))]
       (.statusUpdate scheduler driver (mesos-type/->pb :TaskStatus
                                                        {:task-id {:value task-id}
                                                         :state task-state}))
       (-> state
           (update :task-id->task #(dissoc % task-id))
           (update-in [:slave-id->host (:value (:slave-id task)) :available-resources]
                      #(combine-resources (:resources task) %))))
     state)))

(defn complete-tasks!
  "Removes tasks from state that have completed and calls complete-task!"
  [{:keys [now time-task-id-pairs] :as state} scheduler driver]
  (let [complete? (comp (partial t/after? now) first)
        complete?->pairs (group-by complete? time-task-id-pairs)
        to-complete-pairs (get complete?->pairs true) 
        remaining (get complete?->pairs false)
        to-complete-task-ids (map second to-complete-pairs)
        state' (reduce #(complete-task! %1 %2 scheduler driver) state to-complete-task-ids)]
    (log/debug "Completing" to-complete-pairs)
    (-> state'
        (assoc :time-task-id-pairs remaining))))

(defmulti handle-action!
  "Handles the particular action, likely calling methods on the scheduler
   and returning an updated state

   Parameters:
   `action` the option to dispatch off
   `data` data corresponding to the action and specific to the action
   `state` the current state of 'mesos'
   `driver` the driver used by the scheduler
   `scheduler` the scheduler framework connected to mesos"
  (fn [action _ _ _ _]
    action))

(defmethod handle-action! :decline
  [_ offer-id {:keys [offer-id->offer] :as state} _ _]
  (if-let [offer (get offer-id->offer offer-id)]
    (-> state
        (update :offer-id->offer #(dissoc % offer-id))
        (update-in [:slave-id->host (:value (:slave-id offer)) :available-resources]
                   #(combine-resources (:resources offer) %)))
    (throw (ex-info "Unknown offer-id" {:offer-id offer-id}))))

(defmethod handle-action! :kill-task
  [_ task-id state driver scheduler]
  (log/debug "Killing task " {:task-id task-id})
  (complete-task! state task-id scheduler driver :task-killed))

(defmethod handle-action! :launch
  [_ {:keys [offer-ids tasks] :as in} {:keys [offer-id->offer slave-id->host] :as state} driver scheduler]
  (log/debug "In launch handle-action!" in)
  (let [offers (map offer-id->offer offer-ids)
        slave-id (:value (:slave-id (first tasks)))]
    (when-not (every? #(= slave-id (:value (:slave-id %))) tasks)
      (throw (ex-info "All tasks must have same slave-id"
                      {:slave-ids (map (comp :value :slave-id) tasks)})))
    (when-not (get slave-id->host slave-id)
      (throw (ex-info "Unknown slave-id" {:slave-id slave-id :known-slave-ids (keys slave-id->host)})))
    (when (some nil? offers)
      (throw (ex-info "Unknown offers"
                      {:unknown-offers
                       (remove #(contains? (set (keys offer-id->offer)) %)
                               offer-ids)})))
    (when-not (every? #(= slave-id (:value (:slave-id %))) offers)
      (throw (ex-info "Some offers don't match slave id of tasks"
                      {:expected-slave-id slave-id
                       :mismatched-offers (filter #(not= slave-id (:value (:slave-id %)))
                                                  offers)})))
    (let [host (get slave-id->host slave-id)
          available-resources (combine-resources (mapcat :resources offers) (:available-resources host))
          requested-resources (combine-resources (mapcat :resources tasks))
          resources' (subtract-resources available-resources requested-resources)
          tasks (map #(assoc % :launched-time (t/now)) tasks)
          task->runtime-ms (-> state :config :task->runtime-ms)
          runtime-multiplier (get-in host [:attributes :runtime-multiplier] 1)
          new-time-task-id-pairs (map (fn [{:keys [task-id] :as task}]
                                        [(t/plus (t/now)
                                                 (-> task task->runtime-ms (* runtime-multiplier) t/millis))
                                         (:value task-id)])
                                      tasks)]     
      (log/debug "Resources requested by tasks: " {:requested-resources requested-resources})
      ;; May need to put this in a thread..
      (doseq [task tasks]
        (.statusUpdate scheduler
                       driver
                       (mesos-type/->pb :TaskStatus
                                        {:task-id {:value (:value (:task-id task))}
                                         :state :task-running})))
      (log/debug "Launching tasks " {:tasks tasks})
      (-> state
          (update :task-id->task #(into % (map-from-vals (comp :value :task-id) tasks)))
          (update :time-task-id-pairs #(into % new-time-task-id-pairs))
          (assoc-in [:slave-id->host slave-id :available-resources] resources')
          (update :offer-id->offer #(apply dissoc % offer-ids))))))

(defmethod handle-action! :reconcile
  [_ _ _ _ _]
  ;; TODO: implement
  )

(defn default-task->runtime-ms
  "Takes a task spec and extracts a number as runtime from the command's environment."
  [task]
  (->> task
       :executor
       :command
       :environment
       .getVariablesList
       seq
       (filter #(= "EXECUTION_TIME" (.getName %)))
       first
       .getValue
       read-string))

(defn default-task->complete-status
  "Returns completed successfully"
  [_]
  :task-finished)

(defn mesos-driver-mock
  "Creates an instance of org.apache.mesos.SchedulerDriver which will:
      1. Close the start-chan when start is called
      2. Will try to pull off exit-chan when join is called
      3. Will close the exit-chan when stop is called
      4. Put events on the action chan when all other methods are called"
  [start-chan action-chan exit-chan]
  (reify SchedulerDriver
    (declineOffer [_ offer-id]
      (log/debug "Declining offer" {:offer-id offer-id})
      (async/>!! action-chan [:decline (:value (mesos-type/pb->data offer-id))])
      Protos$Status/DRIVER_RUNNING)
    (join [_]
      (async/<!! exit-chan)
      Protos$Status/DRIVER_RUNNING)
    (stop [_]
      (async/close! exit-chan))
    (killTask [_ task-id]
      (log/debug "In driver killTask" {:task-id task-id :pb->data (mesos-type/pb->data task-id) :value (:value (mesos-type/pb->data task-id))})
      (async/>!! action-chan [:kill-task (:value (mesos-type/pb->data task-id))])
      Protos$Status/DRIVER_RUNNING)
    (^Protos$Status launchTasks [_ ^java.util.Collection offer-ids ^java.util.Collection tasks]
      (log/debug "Launch tasks called" {:tasks tasks :offer-ids offer-ids})
      (async/>!! action-chan [:launch {:offer-ids (map (comp :value mesos-type/pb->data) offer-ids)
                                       :tasks (map mesos-type/pb->data tasks)}])
      Protos$Status/DRIVER_RUNNING)
    (reconcileTasks [_ statuses]
      (async/>!! action-chan [:reconcile statuses])
      Protos$Status/DRIVER_RUNNING)
    (start [_]
      (async/close! start-chan)
      Protos$Status/DRIVER_RUNNING)))

(defn mesos-mock
  "A mock mesos implementation which returns a mesos driver to interact with
   the mock.

   It stores the 'hosts' in the cluster, how much resources are available on
   each host, what jobs are running on each 'host' and how much longer each
   job is expected to run for.

   Parameters:
   `hosts` is a seq of maps that contain a hostname, agent id, resources and attributes
   `offer-trigger-chan` chan that has an item put on it when offers should be sent
   `scheduler` is an implementation of the mesomatic scheduler protocol
   `state-atom` will reset atom after each cycle with updated state -- useful for debugging
   `task->runtime-ms` is a function that takes a task spec and returns the runtime in millis
   `task->complete-status` is a function that takes a task spec and returns a mesos complete status
   `scheduler` is an implementation of the mesomatic scheduler protocol

   Returns a mesos driver"
  ([hosts offer-trigger-chan scheduler & 
    {:keys [task->complete-status task->runtime-ms state-atom
            complete-trigger-chan]
     :or {task->complete-status default-task->complete-status
          task->runtime-ms default-task->runtime-ms
          state-atom (atom nil)
          complete-trigger-chan (chime-ch (util/time-seq (t/now) (t/millis 20)))}}]
   (let [action-chan (async/chan 10) ; This will block calls to the driver. This may be problematic..
         exit-chan (async/chan)
         start-chan (async/chan)
         driver (mesos-driver-mock start-chan action-chan exit-chan)
         hosts (->> hosts
                    (map #(assoc % :available-resources (:resources %)))
                    (map #(update % :slave-id str)))]
     (async/thread
       (try
         (async/<!! start-chan)
         (.registered scheduler
                      driver
                      (mesos-type/->pb :FrameworkID {:value "cool-framework-id"})
                      (mesos-type/->pb :MasterInfo {:id "silly id"
                                                    :ip 127
                                                    :port 5050
                                                    :version "1.0.1"}))
         (loop [state {:config {:task->runtime-ms task->runtime-ms
                                :task->complete-status task->complete-status}
                       :slave-id->host (map-from-vals :slave-id hosts)
                       :offer-id->offer {}
                       :task-id->task {}
                       :time-task-id-pairs []
                       :now (t/now)}]
           (log/debug "State before " state)
           (reset! state-atom state)
           (let [[v ch] (async/alts!! [exit-chan action-chan offer-trigger-chan complete-trigger-chan]
                                      :priority true)
                 _ (log/debug "Picked next decision" {:v v :ch ch})
                 state'
                 (condp = ch
                   exit-chan nil
                   action-chan (let [[action data] v
                                     _ (log/debug "Handling action" {:action action :data data})
                                     state' (handle-action! action data state driver scheduler)]
                                 (log/trace {:action action :data data :state' state'})
                                 state')
                   offer-trigger-chan (let [[new-offers state'] (prepare-new-offers state)]
                                        (log/debug "Sending offers" {:offers new-offers})
                                        (when (seq new-offers)
                                          (.resourceOffers scheduler driver (mapv (partial mesos-type/->pb :Offer) new-offers)))
                                        (util/close-when-ch! v)
                                        state')
                   complete-trigger-chan (let [state' (complete-tasks! (assoc state :now (t/now)) scheduler driver)]
                                           (util/close-when-ch! v)
                                           state'))]
             (when state'
               (recur state'))))
         (catch Exception ex
           (log/fatal ex "Error while simulating mesos"))))
     (mesos/wrap-driver driver))))
