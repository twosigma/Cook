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
(ns cook.scheduler.constraints
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.core.cache :as cache]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [cook.config :as config]
            [cook.scheduler.data-locality :as dl]
            [cook.group :as group]
            [cook.tools :as util]
            [cook.rate-limit :as ratelimit]
            [swiss.arrows :refer :all])
  (:import (com.netflix.fenzo ConstraintEvaluator
                              ConstraintEvaluator$Result
                              TaskRequest
                              TaskTrackerState
                              VirtualMachineCurrentState
                              VirtualMachineLease)
           java.util.Date))

;; Wisdom:
;; * This code expects that attributes COOK_GPU? and HOSTNAME are set for all
;; VMs. This is done by scheduler/get-offer-attr-map , which fetches the mesos
;; attributes from an offer and also adds some other custom attributes that are
;; useful to Cook.  In general, adding custom attributes is great for flagging
;; features of a VM that are hard to figure out during a rebalancing cycle. For
;; example, figuring out if a machine has gpus requires knowing a VM's
;; resources, which are not accessible to the rebalancer (would need to cache
;; resources along with attributes). Instead, we just set the COOK_GPU?
;; attribute in scheduler/get-offer-attr-map/

(defn get-vm-lease-attr-map
  "Returns the attribute map of a Fenzo VirtualMachineLease."
  [^VirtualMachineLease lease]
  (.getAttributeMap lease))

(defn job-needs-gpus?
  "Returns true if the provided job needs GPUs, false otherwise."
  [job]
  (->> (:job/resource job)
       (filter (fn gpu-resource? [res]
                 (and (= (:resource/type res) :resource.type/gpus)
                      (pos? (:resource/amount res)))))
       (seq)
       (boolean)))

;; Job host placement constraints
(defprotocol JobConstraint
  "A placement constraint that is defined only by the job being placed."
  (job-constraint-name [this]
    "The name of this constraint")
  (job-constraint-evaluate [this target-vm-resources target-vm-attrs]
                           [this target-vm-resources target-vm-attrs target-vm-tasks-assigned]
    "Evaluates whether a vm with resources 'target-vm-resources', attributes 'target-vm-attrs'
     and TaskRequests (optional) 'target-vm-tasks-assigned' passes the job's constraint. Must return a
     2-element vector, where the first element is a boolean (whether the constraint was passsed) and
     the second element is a string (empty string if constraint passed, an explanation otherwise)."))

(defn get-class-name
  "Returns the name of a class without package information."
  [object]
  (.getSimpleName (type object)))

(defrecord novel-host-constraint [job previous-hosts]
  JobConstraint
  (job-constraint-name [this] (get-class-name this))
  (job-constraint-evaluate
    [this _ vm-attributes]
    (let [job (:job this)
          target-hostname (get vm-attributes "HOSTNAME")]
      [(not-any? #(= target-hostname %) previous-hosts)
       "Already ran on host"]))
  (job-constraint-evaluate
    [this _ vm-attributes _]
    (job-constraint-evaluate this _ vm-attributes)))

(defn build-novel-host-constraint
  "Constructs a novel-host-constraint.
  The constraint prevents the job from running on hosts it has already run on"
  [job]
  (let [previous-hosts (->> (:job/instance job)
                            (remove #(true? (:instance/preempted? %)))
                            (mapv :instance/hostname))]
    (->novel-host-constraint job previous-hosts)))

(defrecord gpu-host-constraint [job needs-gpus?]
  JobConstraint
  (job-constraint-name [this] (get-class-name this))
  (job-constraint-evaluate
    [this _ vm-attributes]
    (job-constraint-evaluate this nil vm-attributes []))
  (job-constraint-evaluate
    [this _ vm-attributes target-vm-tasks-assigned]
    (let [; Look at attribute and running jobs to determine if vm has gpus
          vm-has-gpus? (or (get vm-attributes "COOK_GPU?") ; Set when putting attributes in cache
                           (some (fn gpu-task? [{:keys [needs-gpus?]}]
                                   needs-gpus?)
                                 target-vm-tasks-assigned))
          job (:job this)
          passes? (or (and needs-gpus? vm-has-gpus?)
                      (and (not needs-gpus?) (not vm-has-gpus?)))]
      [passes? (when-not passes? (if (and needs-gpus? (not vm-has-gpus?))
                                   "Job needs gpus, host does not have gpus."
                                   "Job does not need gpus, host has gpus."))])))

(defn build-gpu-host-constraint
  "Constructs a gpu-host-constraint.
  The constraint prevents a gpu job from running on a non-gpu host (resources should also handle this)
  and a non-gpu job from running on a gpu host because we consider gpus scarce resources."
  [job]
  (let [needs-gpus? (job-needs-gpus? job)]
    (->gpu-host-constraint job needs-gpus?)))

(defrecord rebalancer-reservation-constraint [reserved-hosts]
  JobConstraint
  (job-constraint-name [this] (get-class-name this))
  (job-constraint-evaluate
    [this _ vm-attributes]
    (let [target-hostname (get vm-attributes "HOSTNAME")]
      [(not (contains? reserved-hosts target-hostname))
       "Host is temporarily reserved"]))
  (job-constraint-evaluate
    [this _ vm-attributes _]
    (job-constraint-evaluate this _ vm-attributes)))

(defrecord data-locality-constraint [job launch-wait-seconds]
  JobConstraint
  (job-constraint-name [this] (get-class-name this))
  (job-constraint-evaluate [this _ vm-attributes]
    (let [{:keys [job/submit-time]} job
          datasets (dl/get-dataset-maps job)
          data-locality-costs (dl/get-data-local-costs)
          launch-after-age (t/plus (tc/from-date submit-time) (t/seconds launch-wait-seconds))
          launch-without-data? (t/after? (t/now) launch-after-age)]
      (if (contains? data-locality-costs datasets)
        (let [{:keys [suitable] :or {suitable true}} (get-in data-locality-costs [datasets (get vm-attributes "HOSTNAME")])]
          (if suitable
            [true nil]
            [false "Host is not suitable for datasets"]))
        (if launch-without-data?
          [true nil]
          [false "No data locality costs available"]))))
  (job-constraint-evaluate [this _ vm-attributes _]
    (job-constraint-evaluate this _ vm-attributes)))

(defn build-data-locality-constraint
  "If the job supports data local and the data local fitness calculator is in use,
   returns a data-locality-constraint"
  [{:keys [job/datasets] :as job}]
  (let [{:keys [launch-wait-seconds]} (config/data-local-fitness-config)
        fitness-calculator (config/fitness-calculator-config)]
    (when (and (not (empty? datasets)) (= fitness-calculator dl/data-local-fitness-calculator))
      (->data-locality-constraint job launch-wait-seconds))))

(defrecord user-defined-constraint [constraints]
  JobConstraint
  (job-constraint-name [this] (get-class-name this))
  (job-constraint-evaluate
    [this _ vm-attributes]
    (let [vm-passes-constraint?
          (fn vm-passes-constraint? [{attribute :constraint/attribute
                                      pattern :constraint/pattern
                                      operator :constraint/operator}]
            (let [vm-attribute-value (get vm-attributes attribute)]
              (condp = operator
                :constraint.operator/equals (= pattern vm-attribute-value)
                :else (do
                        (log/error (str "Unknown operator " operator
                                        " api.clj should have prevented this from happening."))
                        true))))
          passes? (every? vm-passes-constraint? constraints)]
      [passes? (when-not passes?
                 "Host doesn't pass at least one user supplied constraint.")]))
  (job-constraint-evaluate
    [this _ vm-attributes _]
    (job-constraint-evaluate this _ vm-attributes)))

(defn build-user-defined-constraint
  "Constructs a user-defined-constraint.
   The constraint asserts that the vm passes the constraints the user supplied as host constraints"
  [job]
  (->user-defined-constraint (:job/constraint job)))

(defrecord estimated-completion-constraint [estimated-end-time host-lifetime-mins]
  JobConstraint
  (job-constraint-name [this] (get-class-name this))
  (job-constraint-evaluate
    [this _ vm-attributes _]
    (job-constraint-evaluate this _ vm-attributes))
  (job-constraint-evaluate
    [this _ vm-attributes]
    (if-let [host-start-time (get vm-attributes "host-start-time")]
      (let [host-death-time (+ (* 1000 (long host-start-time))
                               (* 60 1000 host-lifetime-mins))]
        (if (< estimated-end-time host-death-time)
          [true ""]
          [false "host is expected to shutdown before job completion"]))
      [true ""])))

(defn instance-runtime
  "Returns the runtime, in milliseconds, of the given instance,
  or 0 if the end-time or mesos-start-time is nil"
  [{:keys [^Date instance/end-time ^Date instance/mesos-start-time]}]
  (if (and end-time mesos-start-time)
    (- (.getTime end-time) (.getTime mesos-start-time))
    0))

(defn build-estimated-completion-constraint
  "Returns an instance of estimated-completion-constraint for the job. The estimated end time will be the max of:
   - :job/expected-runtime
   - the runtimes of all failed instances with :mesos-slave-removed as the reason"
  [{:keys [job/expected-runtime job/instance]}]
  (let [{:keys [expected-runtime-multiplier host-lifetime-mins agent-start-grace-period-mins]} (config/estimated-completion-config)]
    (when (and expected-runtime-multiplier host-lifetime-mins)
      (let [agent-removed-failures (filter #(= :mesos-slave-removed (get-in % [:instance/reason :reason/name]))
                                           instance)
            agent-removed-runtimes (map instance-runtime agent-removed-failures)
            scaled-expected-runtime (if expected-runtime
                                      (long (* expected-runtime expected-runtime-multiplier))
                                      0)
            max-expected-runtime (apply max (conj agent-removed-runtimes scaled-expected-runtime))
            ; If a job has run "nearly" the entire lifetime of a host (host-lifetime-mins - agent-start-grace-period-mins)
            ; then accept any host that has been alive for less than agent-start-grace-period-mins
            longest-reasonable-runtime (-> host-lifetime-mins
                                           (- agent-start-grace-period-mins)
                                           (* 1000 60))
            capped-expected-runtime (min max-expected-runtime longest-reasonable-runtime)
            expected-end-time (+ (.getMillis (t/now)) capped-expected-runtime)]
        (when (< 0 max-expected-runtime)
          (->estimated-completion-constraint expected-end-time host-lifetime-mins))))))

(defn build-launch-max-tasks-constraint
  "This returns a Fenzo hard constraint that ensures that we don't match more than a given number of tasks per cycle. Returns nil
  if the constraint is disabled."
  []
  (let [enforcing? (ratelimit/enforce? ratelimit/global-job-launch-rate-limiter)
        max-tasks (ratelimit/get-token-count! ratelimit/global-job-launch-rate-limiter ratelimit/global-job-launch-rate-limiter-key)]
    (when enforcing?
      (reify ConstraintEvaluator
        (getName [_] "launch_max_tasks")
        (evaluate [_ _ _ task-tracker-state]
          (let [num-assigned (-> task-tracker-state .getAllCurrentlyAssignedTasks .size)]
            (ConstraintEvaluator$Result.
              (< num-assigned max-tasks)
              (str "Hit the global rate limit"))))))))

(defn build-max-tasks-per-host-constraint
  "Returns a Fenzo constraint that ensures that we don't
  match more tasks per host than is allowed (configured)"
  []
  (reify ConstraintEvaluator
    (getName [_] "max_tasks_per_host")
    (^ConstraintEvaluator$Result evaluate
      [^ConstraintEvaluator _
       ^TaskRequest _
       ^VirtualMachineCurrentState target-vm
       ^TaskTrackerState _]
      (let [vm-resources (.getCurrAvailableResources target-vm)
            vm-attributes (get-vm-lease-attr-map vm-resources)
            max-per-host (get vm-attributes "COOK_MAX_TASKS_PER_HOST")]
        (if max-per-host
          (let [num-running (get vm-attributes "COOK_NUM_TASKS_ON_HOST")
                num-assigned (-> target-vm .getTasksCurrentlyAssigned count)
                num-total (+ num-running num-assigned)]
            (ConstraintEvaluator$Result.
              (< num-total max-per-host)
              "Host already has the maximum number of tasks"))
          (ConstraintEvaluator$Result.
            true
            ""))))))

(def job-constraint-constructors [build-novel-host-constraint build-gpu-host-constraint build-user-defined-constraint build-estimated-completion-constraint build-data-locality-constraint])

(defn fenzoize-job-constraint
  "Makes the JobConstraint 'constraint' Fenzo-compatible."
  [constraint]
  (reify ConstraintEvaluator
    (getName [_] (job-constraint-name constraint))
    (^ConstraintEvaluator$Result evaluate
      [^ConstraintEvaluator _
       ^TaskRequest task-request
       ^VirtualMachineCurrentState target-vm
       ^TaskTrackerState _]
      (let [vm-resources (.getCurrAvailableResources target-vm)
            vm-attributes (get-vm-lease-attr-map vm-resources)
            [passes? reason] (job-constraint-evaluate constraint vm-resources vm-attributes
                                                      ;; Although concat can be dangerous, in this case it saves a significant
                                                      ;; amount of memory compared to building a vec (around 10% of allocations)
                                                      (concat (.getRunningTasks target-vm)
                                                              (map (fn [^com.netflix.fenzo.TaskAssignmentResult result]
                                                                     (.getRequest result))
                                                                   (.getTasksCurrentlyAssigned target-vm))))]
        (ConstraintEvaluator$Result. passes? reason)))))

(defn make-fenzo-job-constraints
  "Returns a sequence of all the constraints for 'job', in Fenzo-compatible format."
  [job]
  (let [launch-max-tasks-constraint (build-launch-max-tasks-constraint)]
    (cond-> (->> job-constraint-constructors
                 (map (fn [constructor] (constructor job)))
                 (remove nil?)
                 (map fenzoize-job-constraint))
            launch-max-tasks-constraint (conj (build-launch-max-tasks-constraint))
            true (conj (build-max-tasks-per-host-constraint)))))

(defn build-rebalancer-reservation-constraint
  "Constructs a rebalancer-reservation-constraint"
  [reserved-hosts]
  (-> reserved-hosts
      ->rebalancer-reservation-constraint
      fenzoize-job-constraint))

(defn make-rebalancer-job-constraints
  "Returns a sequence of all job constraints for 'job', in rebalancer-compatible (rebalancer.clj)
   format. 'slave-attrs-getter' must be a function that takes a slave-id and returns a map of that
   slave's attributes."
  [job slave-attrs-getter]
  (->> job-constraint-constructors
       (map (fn [constructor] (constructor job)))
       (remove nil?)
       (map (fn [constraint]
              (fn job-constraint [job target-slave-id]
                (first ; evaluate returns [passes? reason], this function only returns passes?
                 (job-constraint-evaluate constraint nil (slave-attrs-getter target-slave-id))))))))

;; Group host placement constraints

(defn- get-running-cotasks-from-cache
  "Looks up the currently running tasks in group from group-uuid->running-cotask-cache
   and loads them from db if missing."
  [db group-uuid->running-cotask-cache group]
  (let [group-uuid (:group/uuid group)]
    (cache/lookup (swap! group-uuid->running-cotask-cache
                         (fn [c]
                           (if (cache/has? c group-uuid)
                             (cache/hit c group-uuid)
                             (cache/miss c group-uuid (group/group->running-task-set db group)))))
                  group-uuid)))

(defn get-cotasks
  [db group job group-uuid->running-cotask-cache]
  "Given a group and a job, finds the set of all the running instances that belong to the jobs in
   'group', but do not belong to 'job'"
  (let [group-uuid (:group/uuid group)
        db-cotasks (get-running-cotasks-from-cache db group-uuid->running-cotask-cache group)]
    (set/difference db-cotasks (set (:job/instance job)))))

(defn get-cotasks-from-tracker-state
  "Returns all the Fenzo TaskTracker.ActiveTask (stored in task-tracker-state) that correspond to
   the provided cotask-ids."
  [task-tracker-state cotask-ids]
  (let [running-cotasks (-> task-tracker-state
                            .getAllRunningTasks
                            (select-keys cotask-ids)
                            vals)
        assigned-cotasks (-> task-tracker-state
                             .getAllCurrentlyAssignedTasks
                             (select-keys cotask-ids)
                             vals)]
    (into (vec running-cotasks) assigned-cotasks)))

(defn get-fenzo-cotasks
  "Given a group, a map of guuids to task-ids, a task-id and a Fenzo task-tracker-state, returns a
   sequence of Fenzo TaskTracker.ActiveTasks, which correspond to ALL the running tasks in group.
   This includes both running tasks and tasks that are being assigned by Fenzo
   in the current cycle."
  [db group task-id same-cycle-task-ids task-tracker-state group-uuid->running-cotask-cache]
  (let [db-cotask-ids (set (map :instance/task-id (get-running-cotasks-from-cache
                                                   db group-uuid->running-cotask-cache group)))
        cotask-ids (disj (set/union
                          db-cotask-ids ; Tasks that are in the database (running or scheduled)
                          same-cycle-task-ids) ; Tasks in same cycle
                         task-id)] ; Do not include this task-id
    ; Return a Fenzo TaskTracker.ActiveTask
    (get-cotasks-from-tracker-state task-tracker-state cotask-ids)))

(defprotocol GroupConstraint
  "A placement constraint that is defined only by a group."
  (group-constraint-name [this]
    "The name of this constraint")
  (group-constraint-type [this]
    "A keyword indicated the type of this constraint.")
  (group-constraint-evaluate [this target-attr-map cohost-attr-maps]
    "Evaluates whether a vm with attributes 'target-attr-map' passes a group constraint, given the
     sequence of attribute maps of all cohosts (hosts running tasks in the same group)
     'cohost-attr-maps'.  Must return a 2-element vector, where the first element is a boolean
     (whether the constraint was passsed) and the second element is a string (empty string if
     constraint passed, an explanation otherwise)."))

(defn get-group-constraint-type
  [group]
  (or (-> group :group/host-placement :host-placement/type)
      :host-placement.type/all))

(defrecord unique-host-placement-group-constraint [group]
  GroupConstraint
  (group-constraint-name [self] (get-class-name self))
  (group-constraint-type [self] (get-group-constraint-type (:group self)))
  (group-constraint-evaluate [this target-attr-map cohost-attr-maps]
    (let [target-hostname (get target-attr-map "HOSTNAME")
          cotask-hostnames (map #(get % "HOSTNAME") cohost-attr-maps)
          passes? (and target-hostname
                       ; cotask-hostnames does not contain target-hostname
                       (not-any? #{target-hostname} cotask-hostnames))
          reason (if passes? "" (format "The hostname %s is being used by other instances in group %s"
                                        target-hostname (:group/uuid (:group this))))]
      [passes? reason])))

(defrecord balanced-host-placement-group-constraint [group]
  GroupConstraint
  (group-constraint-name [self] (get-class-name self))
  (group-constraint-type [self] (get-group-constraint-type (:group self)))
  (group-constraint-evaluate [this target-attr-map cohost-attr-maps]
    (let [group (:group this)
          attr-name (-> group :group/host-placement :host-placement/parameters :host-placement.balanced/attribute)
          minimum-attr-count (-> group :group/host-placement :host-placement/parameters :host-placement.balanced/minimum)
          attr-freq-map (->> cohost-attr-maps
                             (map #(get % attr-name))
                             frequencies)
          attr-freqs (vals attr-freq-map)
          target-attr-val (get target-attr-map attr-name)
          target-freq (get attr-freq-map target-attr-val)
          [minim maxim] (when attr-freqs
                          ; If false enforces the minimum spread
                          [(if (> minimum-attr-count (count attr-freqs)) 0 (apply min attr-freqs))
                           (apply max attr-freqs)])
          passes? (or (empty? attr-freqs)
                      (nil? target-freq)
                      (= minim maxim)
                      (< target-freq maxim))
          reason (if passes? "" (format "Attribute %s=%s would imbalance the distribution of group %s"
                                        attr-name
                                        target-attr-val
                                        (:group/uuid group)))]
      [passes? reason])))

(defrecord attribute-equals-host-placement-group-constraint [group]
  GroupConstraint
  (group-constraint-name [self] (get-class-name self))
  (group-constraint-type [self] (get-group-constraint-type (:group self)))
  (group-constraint-evaluate [this target-attr-map cohost-attr-maps]
    (let [group (:group this)
          attr-name (-> group :group/host-placement :host-placement/parameters :host-placement.attribute-equals/attribute)
          target-attr-val (get target-attr-map attr-name)
          cotask-attr-vals (set (map #(get % attr-name) cohost-attr-maps))
          passes? (or (empty? cotask-attr-vals)
                      (contains? cotask-attr-vals target-attr-val))
          reason (if passes? "" (format "Attribute %s of host %s is different from that of cohosts."
                                        attr-name target-attr-val))]
      (when (> (count cotask-attr-vals) 1)
        (log/warn (format "Attribute-equals constraint broken for group %s, distinct attributes are %s"
                          (:group/uuid group) (pr-str cotask-attr-vals))))
      [passes? reason])))

;; IMPORTANT: Register new group constraints here
(defn constraint-type-to-constraint-constructor
  "Given a constraint-type (as stored in Datomic), returns a constructor for that type of constraint."
  [constraint-type]
  ; This could be automatic if defrecord supported static methods :(
  (case constraint-type
    :host-placement.type/unique ->unique-host-placement-group-constraint
    :host-placement.type/balanced ->balanced-host-placement-group-constraint
    :host-placement.type/attribute-equals ->attribute-equals-host-placement-group-constraint
    :host-placement.type/all nil))

(defn make-fenzo-group-constraint
  "Returns a Fenzo-compatible group host placement constraint for tasks that belong to 'group'. The
   'cycle-task-ids-fn' parameters is a 0-arity function which will return the sequence of task
   ids that will be considered in the cycle where this constraint will be used."
  [db group cycle-task-ids-fn running-task-id-cache]
  (let [constraint-type (or (-> group :group/host-placement :host-placement/type) :host-placement.type/all)
        constraint-constructor (constraint-type-to-constraint-constructor constraint-type)
        constraint (when constraint-constructor (constraint-constructor group))]
    (when constraint
      (reify ConstraintEvaluator
        (getName [_] (group-constraint-name constraint))
        (evaluate [_ task-request target-vm task-tracker-state]
          (let [task-id (.getId task-request)
                target-attr-map (-> target-vm
                                    .getCurrAvailableResources
                                    (get-vm-lease-attr-map))
                cotasks (get-fenzo-cotasks db group task-id (cycle-task-ids-fn) task-tracker-state running-task-id-cache)
                cotask-attr-maps (->> cotasks
                                      (map #(.getTotalLease %))
                                      (map get-vm-lease-attr-map))
                [passes? reason] (group-constraint-evaluate constraint target-attr-map cotask-attr-maps)]
            (ConstraintEvaluator$Result. (boolean passes?) reason)))))))

(defn make-rebalancer-group-constraint
  "Returns a rebalancer-compatible (rebalancer.clj) group host placement constraint for tasks that
   belong to 'group'. 'slave-attrs-getter' must be a function that takes a slave-id and returns the
   attribute map for that slave. 'tasks-preempted-so-far' must be a sequence of tasks that the
   rebalancer has decided to preempt during the current cycle."
  [db group slave-attrs-getter tasks-preempted-so-far cotask-cache]
  (let [constraint-type (get-group-constraint-type group)
        constraint-constructor (constraint-type-to-constraint-constructor constraint-type)
        constraint (when constraint-constructor (constraint-constructor group))]
    (when constraint
      (fn group-constraint [job target-slave-id]
        (let [cotask-slave-ids (->> (get-cotasks db group job cotask-cache)
                                    (map :instance/slave-id)
                                    (into (vec (remove nil? (map :instance/slave-id tasks-preempted-so-far)))))
              target-attr-map (slave-attrs-getter target-slave-id)
              cotask-attr-maps (map slave-attrs-getter cotask-slave-ids)
              [passes? _] (group-constraint-evaluate constraint target-attr-map cotask-attr-maps)]
          (boolean passes?))))))
