(ns cook.group
  (:require [clj-time.core :as t]
            [cook.tools :as util]
            [datomic.api :as d]))

(defmulti find-stragglers
  "Given a group entity, returns a list of task entities 
   that are considered stragglers"
  (fn [group-ent]
    (-> group-ent :group/straggler-handling :straggler-handling/type util/without-ns)))

(defmethod find-stragglers :none
  [group-ent]
  ;do nothing
  )

(defmethod find-stragglers :quantile-deviation
  ;; Given a group entity, waits for at least `quantile` quantile jobs to complete
  ;; and a straggler is any task that has been running `multiplier` times the run 
  ;; time of the quantile-th job
  [group-ent]
  (let [{:keys [quantile multiplier] :as params} (->> group-ent
                                                      :group/straggler-handling 
                                                      :straggler-handling/parameters 
                                                      (into {})
                                                      (util/deep-transduce-kv 
                                                        (map (juxt (comp util/without-ns first) second))))
        jobs (:group/job group-ent)
        running-tasks (->> jobs
                           (mapcat :job/instance)
                           (filter #(= (:instance/status %) :instance.status/running)))
        successful-tasks (->> jobs
                              (mapcat :job/instance)
                              (filter #(= (:instance/status %) :instance.status/success)))
        ;; decrement jobs by 1 to avoid odd issues when there are few instances
        ;; for example, if there are only 2 jobs, than any quantile will use 
        ;; the first job to complete 
        quantile-job-idx (int (* (dec (count jobs)) quantile))]
    (when (> (count successful-tasks) quantile-job-idx)
      (let [sorted-success-tasks (sort-by (comp t/in-seconds util/task-run-time) successful-tasks)
            target-task (nth sorted-success-tasks quantile-job-idx)
            target-runtime-seconds (t/in-seconds (util/task-run-time target-task))
            max-runtime-seconds (* target-runtime-seconds multiplier)]
        (filter #(> (t/in-seconds (util/task-run-time %)) max-runtime-seconds)
                running-tasks)))))

(defn group->running-task-set
  "Returns all the possibly running (running or unknown, unknown-state instances are in an unknown
   limbo between where mesos has been instructed to run them, but the action has not been confirmed
   by mesos) instances. Returns set to allow set operations (difference, union) with the result of
   datomic queries for job instances (also sets)."
  [db group]
  (->> (d/entity db [:group/uuid (:group/uuid group)])
       :group/job
       (mapcat :job/instance)
       (filter #(#{:instance.status/running :instance.status/unknown} (:instance/status %)))
       set))

(defn group->running-task-id-set
  "A wrapper around group->running-task-id-set that returns a set of task-ids instead of task
   entities. Returns a set so that set operations can be used to manipulate the result in
   conjunction with the result of cotask-getters, which also return sets."
  [db group]
  (->> group
       (group->running-task-set db)
       (map :instance/task-id)
       set))

