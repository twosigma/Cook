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
  (:require [clj-time.core :as t]
            [clojure.core.cache :as cache]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cook.cached-queries :as cached-queries]
            [cook.compute-cluster :as cc]
            [cook.config :as config]
            [cook.config-incremental :as config-incremental]
            [cook.group :as group]
            [cook.regexp-tools :as regexp-tools]
            [cook.tools :as util]
            [swiss.arrows :refer :all])
  (:import (com.netflix.fenzo ConstraintEvaluator ConstraintEvaluator$Result TaskRequest TaskTrackerState VirtualMachineCurrentState VirtualMachineLease)
           (java.util Date)
           (org.joda.time DateTime)))

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
  (let [^Class obj-type (type object)]
    (.getSimpleName obj-type)))

(defrecord novel-host-constraint [job previous-hosts]
  JobConstraint
  (job-constraint-name [this] (get-class-name this))
  (job-constraint-evaluate
    [this _ vm-attributes]
    (let [target-hostname (get vm-attributes "HOSTNAME")]
      [(not (get previous-hosts target-hostname))
       "Already ran on host"]))
  (job-constraint-evaluate
    [this _ vm-attributes _]
    (job-constraint-evaluate this _ vm-attributes)))

(defn job->previous-hosts-to-avoid
  "Given a job, returns the set of hostnames the
  job's instances have run on (except preemptions)."
  [job]
  (->> (:job/instance job)
       (remove #(true? (:instance/preempted? %)))
       (map :instance/hostname)
       set))

(defn build-novel-host-constraint
  "Constructs a novel-host-constraint.
  The constraint prevents the job from running on hosts it has already run on"
  [job]
  (let [previous-hosts (set (job->previous-hosts-to-avoid job))]
    (->novel-host-constraint job previous-hosts)))

(defn job->gpu-model-requested
  "Get GPU model requested from job or use default GPU model on pool"
  [gpu-count job pool-name]
  (let [gpu-model-in-env (-> job util/job-ent->env (get "COOK_GPU_MODEL"))]
    (when (pos? gpu-count)
      (or gpu-model-in-env
          ; lookup the GPU model from the pool defaults defined in config.edn
          (regexp-tools/match-based-on-pool-name (config/valid-gpu-models) pool-name :default-model)))))

(defn job-resources->disk-type
  "Get disk type requested from user or use default disk type on pool"
  [job-resources pool-name]
  ; If user did not specify desired disk type, use the default disk type on the pool
  (let [disk-type-for-job (or (-> job-resources :disk :type)
                              (regexp-tools/match-based-on-pool-name (config/disk) pool-name :default-type))]
    ; Consume the type that the disk-type-requested maps to, which is found in the config
    (or (get (regexp-tools/match-based-on-pool-name (config/disk) pool-name :type-map) disk-type-for-job)
        disk-type-for-job)))

(defn job-resources->disk-request
  "Given resources on job, return the disk-request from the user or the default disk-request in config"
  [job-resources pool-name]
  (or (-> job-resources :disk :request)
      ; if disk config does not have default-request, use default-request of 10GiB
      (regexp-tools/match-based-on-pool-name (config/disk) pool-name :default-request :default-value 10240)))

(defrecord gpu-host-constraint [job-gpu-count-requested job-gpu-model-requested]
  JobConstraint
  (job-constraint-name [this] (get-class-name this))
  (job-constraint-evaluate
    [this _ vm-attributes]
    (job-constraint-evaluate this nil vm-attributes []))
  (job-constraint-evaluate
    [_ _ vm-attributes vm-tasks-assigned]
    (let [k8s-vm? (= (get vm-attributes "compute-cluster-type") "kubernetes")]
          (if k8s-vm?
            (let [vm-gpu-model->count-available (get vm-attributes "gpus")
                  vm-satisfies-constraint? (if (pos? job-gpu-count-requested)
                                             ; If job requests GPUs, require that the VM has the same number of gpus available in the same model as the job requested.
                                             (and (== (get vm-gpu-model->count-available job-gpu-model-requested 0) job-gpu-count-requested)
                                                  ; Only one GPU job can be assigned to each VM
                                                  (-> vm-tasks-assigned count zero?))
                                             ; If job does not request GPUs, require that the VM does not support gpus.
                                             (-> vm-gpu-model->count-available count zero?))]
              [vm-satisfies-constraint? (when-not vm-satisfies-constraint?
                                          (if (not job-gpu-model-requested)
                                            "Job does not need GPUs, kubernetes VM has GPUs."
                                            "Job needs GPUs that are not present on kubernetes VM."))])
            ; Mesos jobs cannot request gpus. If VM is a mesos VM, constraint passes only if job requested 0 gpus.
            (let [vm-satisfies-constraint? (zero? job-gpu-count-requested)]
              [vm-satisfies-constraint? (when-not vm-satisfies-constraint?
                                          "Job needs GPUs, mesos VMs do not support GPU jobs.")])))))

(defn build-gpu-host-constraint
  "Constructs a gpu-host-constraint.
  The constraint prevents a gpu job from running on a host that does not have the correct number and model of gpus
  and a non-gpu job from running on a gpu host because we consider gpus scarce resources."
  [job]
  (let [job-gpu-count-requested (-> job util/job-ent->resources :gpus (or 0))
        job-gpu-model-requested (when (pos? job-gpu-count-requested)
                                  (job->gpu-model-requested job-gpu-count-requested job (cached-queries/job->pool-name job)))]
    (->gpu-host-constraint job-gpu-count-requested job-gpu-model-requested)))

; Cook uses mebibytes (MiB) for disk request and limit.
; Convert bytes from k8s to MiB when passing to disk constraint,
; and MiB back to bytes when submitting to k8s.
(def disk-multiplier (* 1024 1024))

(defrecord disk-host-constraint [job-disk-request job-disk-type]
  JobConstraint
  (job-constraint-name [this] (get-class-name this))
  (job-constraint-evaluate
    [this _ vm-attributes]
    (job-constraint-evaluate this nil vm-attributes []))
  (job-constraint-evaluate
    [_ _ vm-attributes _]
    (let [k8s-vm? (= (get vm-attributes "compute-cluster-type") "kubernetes")]
      (if k8s-vm?
        (let [vm-disk-type->space-available (get vm-attributes "disk")
              ; if disk-host-constraint is being evaluated, job-disk-request should be non-nil,
              ; but we include the following (if ..) check as another safety layer
              vm-satisfies-constraint? (if job-disk-request
                                         (>= (get vm-disk-type->space-available job-disk-type 0) job-disk-request)
                                         ; if job-disk-request is nil, ignore the disk-host-constraint
                                         true)]
          [vm-satisfies-constraint? (when-not vm-satisfies-constraint?
                                      "VM does not have enough disk space of requested disk type")])
        ; Mesos jobs cannot request disk. If VM is a mesos VM, constraint always passes
        [true]))))

(defn build-disk-host-constraint
  "Constructs a disk-host-constraint.
  The constraint prevents a job from running on a host that does not have correct disk type that the job requested
  and prevents a job from running on a host that does not have enough disk space. Use a constraint for disk binpacking instead of
  using Fenzo because disk is not considered a first class resource in Fenzo."
  [job]
  (let [pool-name (cached-queries/job->pool-name job)]
    ; If the pool does not have enable-constraint set to true, return nil
    (when (regexp-tools/match-based-on-pool-name (config/disk) pool-name :enable-constraint? :default-value false)
      (let [; If the user did not specify a disk request, use the default request amount for the pool
            job-disk-request (job-resources->disk-request (util/job-ent->resources job) pool-name)
            job-disk-type (when job-disk-request
                            (job-resources->disk-type (util/job-ent->resources job) pool-name))]
        (->disk-host-constraint job-disk-request job-disk-type)))))

(defn job->last-checkpoint-location
  "Given a job, returns the compute cluster location of the most recent
  instance, if the job had checkpointing enabled and has at least once
  instance. If the previous instance's compute cluster name is not present
  in the dictionary of compute clusters, if the job doesn't have
  checkpointing enabled, or if there simply isn't a previous instance,
  returns nil."
  [{:keys [job/checkpoint job/instance]}]
  (when (and checkpoint instance)
    (let [{{:keys [compute-cluster/cluster-name]} :instance/compute-cluster}
          (->> instance
               (sort-by :instance/start-time)
               last)]
      (-> @cc/cluster-name->compute-cluster-atom
          (get cluster-name)
          cc/compute-cluster->location))))

(defrecord checkpoint-locality-constraint [job-last-checkpoint-location]
  JobConstraint
  (job-constraint-name [this]
    (get-class-name this))

  (job-constraint-evaluate
    [this _ vm-attributes]
    (job-constraint-evaluate this nil vm-attributes []))

  (job-constraint-evaluate
    [_ _ vm-attributes _]
    (if (= (get vm-attributes
                "COOK_COMPUTE_CLUSTER_LOCATION")
           job-last-checkpoint-location)
      [true nil]
      [false "Host is in different location than last checkpoint"])))

(defn build-checkpoint-locality-constraint
  "Given a job, returns a corresponding checkpoint
  locality constraint, or nil if not applicable."
  [job]
  (when-let [location (job->last-checkpoint-location job)]
    (->checkpoint-locality-constraint location)))

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

(defn- resolve-incremental-default-job-constraints
  "Resolve any incremental configurations used for default job constraint patterns"
  [default-job-constraints {:keys [job/uuid]}]
  (->> default-job-constraints
       (map (fn [{:keys [constraint/pattern constraint/pattern-fallback] :as constraint}]
              (if (string? pattern)
                constraint
                (let [[resolved-pattern _] (config-incremental/resolve-incremental-config uuid pattern pattern-fallback)]
                  (-> constraint
                    (dissoc :constraint/pattern-fallback)
                    (assoc :constraint/pattern resolved-pattern))))))))

(defn job->default-constraints
  "Returns the list of default constraints configured for the job's pool"
  [job]
  (resolve-incremental-default-job-constraints
    (regexp-tools/match-based-on-pool-name
      (config/default-job-constraints)
      (cached-queries/job->pool-name job)
      :default-constraints) job))

(def machine-type-constraint-attributes
  #{"cpu-architecture" "node-family" "node-type"})

(defn transform-constraints
  "Given a collection of job constraints and a map from constraint
  attribute to transformation, where transformation has the shape:

  {:new-attribute ... :pattern-transformations ...},

  and :pattern-transformations has the shape:

  [{:match ... :replacement ...}
   {:match ... :replacement ...}
   ...],

  and the match/replacement pairs are fed to str/replace to
  transform the constraint pattern, returns a collection of
  (potentially) transformed job constraints"
  [constraints attribute->transformation]
  (if (seq attribute->transformation)
    (map
      (fn [{:keys [constraint/attribute constraint/pattern] :as constraint}]
        (if-let [{:keys [new-attribute pattern-transformations]}
                 (get attribute->transformation attribute)]
          (assoc constraint
            :constraint/attribute
            (or new-attribute attribute)
            :constraint/pattern
            (loop [new-pattern pattern
                   transformations pattern-transformations]
              (if (seq transformations)
                (let [{:keys [match replacement]} (first transformations)]
                  (recur
                    (str/replace new-pattern match replacement)
                    (rest transformations)))
                new-pattern)))
          constraint))
      constraints)
    constraints))

(defn job->constraints
  "Given a job, returns all job constraints that should be in effect,
  either specified on the job submission or defaulted via configuration"
  [{:keys [job/constraint] :as job}]
  (let [user-specified-constraints constraint

        user-specified-constraint-attributes
        (map :constraint/attribute
             user-specified-constraints)

        user-specified-machine-type-constraint?
        (some
          machine-type-constraint-attributes
          user-specified-constraint-attributes)

        default-constraints
        (->> job
             job->default-constraints
             (remove
               (fn [{:keys [constraint/attribute]}]
                 (or
                   ; Remove any default constraints for which the user
                   ; has specified a constraint on the same attribute
                   (some #{attribute}
                         user-specified-constraint-attributes)

                   ; Remove any default machine-type constraints if
                   ; the user has specified any machine-type constraint
                   ; (otherwise, they could conflict with each other)
                   (and
                     user-specified-machine-type-constraint?
                     (some #{attribute}
                           machine-type-constraint-attributes))))))]

    (-> user-specified-constraints
        (concat default-constraints)
        (transform-constraints (config/constraint-attribute->transformation))
        ; De-lazy the output; Fenzo can call from multiple
        ; threads and we want to avoid contention in LazySeq
        vec)))

(defrecord user-defined-constraint [constraints]
  JobConstraint
  (job-constraint-name [this] (get-class-name this))
  (job-constraint-evaluate
    [_ _ vm-attributes]
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
  (->user-defined-constraint (job->constraints job)))

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
            expected-end-time (+ (.getMillis ^DateTime (t/now)) capped-expected-runtime)]
        (when (< 0 max-expected-runtime)
          (->estimated-completion-constraint expected-end-time host-lifetime-mins))))))

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

; Note that these constraints are used by the rebalancer.
(def job-constraint-constructors [build-novel-host-constraint
                                  build-gpu-host-constraint
                                  build-disk-host-constraint
                                  build-user-defined-constraint
                                  build-estimated-completion-constraint
                                  build-checkpoint-locality-constraint])

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
  "Returns a sequence of all the constraints for 'job', in Fenzo-compatible format for use by Fenzo."
  [job]
  (conj (->> job-constraint-constructors
             (map (fn [constructor] (constructor job)))
             (remove nil?)
             (map fenzoize-job-constraint))
        ; This constraint, which accesses the number of pods on a machine, is not visible to rebalancer.
        (build-max-tasks-per-host-constraint)))

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
