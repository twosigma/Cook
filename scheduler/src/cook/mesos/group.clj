(ns cook.mesos.group
  (:require [cook.mesos.util :as util]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]))

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
      (let [sorted-success-tasks (sort-by util/task-run-time successful-tasks)
            target-task (nth sorted-success-tasks quantile-job-idx)
            target-runtime-seconds (t/in-seconds (util/task-run-time target-task))
            max-runtime-seconds (* target-runtime-seconds multiplier)]
        (filter #(> (t/in-seconds (util/task-run-time %)) max-runtime-seconds)
                running-tasks)))))
