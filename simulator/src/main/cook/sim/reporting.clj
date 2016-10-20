(ns cook.sim.reporting
  (:require [clojure.pprint :as pprint]
            [datomic.api :as d]
            [cook.sim.database :as data]
            [cook.sim.util :as u]
            [incanter.core :as ic]
            [incanter.stats :as is]
            [incanter.charts :as chart]))

(defn actions-for-sim
  "Given a simulation id, and a simulation database returns all of the \"actions\"
  (simulant term) In Cook Simulator's case, the only action a user can currently
  take is to schedule a job."
  [sim-db sim-id]
  (d/q '[:find ?jobid ?requested
         :in $ ?simid
         :where [?log :job/uuid ?jobid]
         [?log :job/requested-at ?requested]
         [?log :actionLog/sim ?simid]]
       sim-db sim-id))

(defn wait-time
  "Given a request time and an instance (from Cook database), returns the amount of
  time before the instance was scheduled."
  [requested-at first-cook-instance]
  (if first-cook-instance
    (- (.getTime (:instance/start-time first-cook-instance))
       requested-at)))

(defn first-finish
  "Given a collection of instances (from Cook database), returns the time of the
  instance to finish successfully."
  [instances]
  (let [successes (filter #(= (:instance/status %) :instance.status/success) instances)]
    (if (-> successes count pos?)
      (.getTime (apply min (map :instance/end-time successes))))))

(defn overhead-time
  "Time from scheduling to finish, minus actual execution time of job.
  Intended to provide meaningful insight into the effect of preemption on actually
  finishing jobs."
  [sim-job scheduled-at instances]
  (let [finished-at (first-finish instances)
        execution-time (* (:job/duration sim-job) 1000)]
    (if finished-at
      (- finished-at (+ scheduled-at execution-time)))))

(defn turnaround-time
  "Time from scheduling to finish."
  [sim-job scheduled-at instances]
  (let [finished-at (first-finish instances)]
    (if finished-at
      (- finished-at scheduled-at))))

(defn job-result
  "Given db's from both Cook scheduler and Cook simulator, and a description of
  a job request (job id and time of request), return a data structure
  that contains a summary of everything we want to know about the job and how
  Cook Scheduler handled it (presumably to be used to generate reports)."
  [sim-db cook-db [job-id action-at]]
  (let [sim-job (:actionLog/action (d/entity sim-db [:job/uuid job-id]))
        cook-job (d/entity cook-db [:job/uuid job-id])
        instances (:job/instance cook-job)]
    {:id job-id
     :requested-at action-at
     :name (:job/name sim-job)
     :username (:job/username sim-job)
     :details (d/touch cook-job)
     :wait-time (wait-time action-at (first instances))
     :turnaround (turnaround-time sim-job action-at instances)
     :overhead (overhead-time sim-job action-at instances)
     :instance-count (count instances)}))

(defn warn-about-unscheduled
  "The only effect of this function is to print out to STDOUT (for the cli).
  Given a collection of job-results, prints out a summary of jobs that were never
  scheduled by Cook Scheduler.  If all jobs were scheduled, nothing will be printed."
  [jobs]
  (let [unscheduled-jobs (remove :wait-time jobs)
        unscheduled-count (count unscheduled-jobs)]
    (if (pos? unscheduled-count)
      (do
        (println "Warning!  This simulation had " unscheduled-count
                 " jobs that were never scheduled by Cook.")
        (println "This could be because you are analyzing before the jobs had a chance to finish...")
       (println "or it could mean that there's a problem with Cook,")
       (println "or with the Simulator's ability to connect to Cook.")
       (doseq [job unscheduled-jobs]
         (println "Job" (:id job))
         (println "details from Cook DB:" (:details job)))))))

(defn unfinished-jobs
  "Given a collection of job-results, returns a filtered version that contains only
  jobs which were scheduled but never completed successfully."
  [jobs]
  (->> jobs (filter :wait-time) (remove :turnaround)))

(defn warn-about-unfinished
  "The only effect of this function is to print out to STDOUT (for the cli).
  Given a collection of job-results, prints out a summary of jobs that were
  scheduled by Cook Scheduler but were never finished successfully.
  If no jobs fall into this category, nothing will be printed."
  [jobs]
  (let [unfinished (unfinished-jobs jobs)
        unfinished-count (count unfinished)]
    (if (pos? unfinished-count)
      (do
        (println "Warning!  This simulation had " unfinished-count " jobs that were started but never finished.")
        (println "This makes it so that some creativity must be used in order to present average turnaround/overhead time for the entire sim, or to do job-by-job performance comparisions against another sim.")
        (doseq [job unfinished]
          (println "Job" (:id job))
          ;;(println "details from Cook DB:" (:details job))
          )))))

(defn summarize-preemption
  "This function prints out to STDOUT (for the cli).
  It summarizes the amount of preemption that happened for a collection of jobs.
  The total number of preemptions is also returned."
  [jobs]
  (let [preemption-count (comp count
                               (partial filter :instance/preempted?)
                               :job/instance :details)
        preemption-counts (into (sorted-map) (group-by preemption-count jobs))
        total-preemptions (apply + (map preemption-count jobs))]
    (println "There were" total-preemptions "total preemptions.")
    (doseq [[number js] preemption-counts]
      (println "  " (count js) "jobs preempted" number "times."))
    total-preemptions))

(defn average-of-metric
  "Given a collection of job-results and a metric (function, that accepts a
  job-result as a single argument and returns a number), returns the average
  of the non-nil metric values across all the job-results."
  [jobs metric]
  (let [metrics (remove nil? (map metric jobs))]
    (if (-> metrics count pos?)
      (/ (apply + metrics) (count metrics)))))


(defn show-average-wait
  "The only effect of this function is to print out to STDOUT (for the cli).
  Displays information about the average wait time for a collection of jobs."
  [jobs]
  (let [avg (average-of-metric jobs :wait-time)]
    (if avg
      (println "Average wait time for jobs is" (float (/ avg 1000)) "seconds.")
      (println "No jobs were scheduled; thus, there is no average wait time."))))

(defn show-average-turnaround
  "The only effect of this function is to print out to STDOUT (for the cli).
  Displays information about the average turnaround time for a collection of jobs."
  [jobs]
  (let [avg (average-of-metric jobs :turnaround)]
    (if avg
      (println "Average turnaround for jobs is" (float (/ avg 1000)) "seconds.")
      (println "No jobs were finished; thus, there is no average turnaround."))))

(defn show-average-overhead
  "The only effect of this function is to print out to STDOUT (for the cli).
  Displays information about the average overhead time for a collection of jobs."
  [jobs]
  (let [avg (average-of-metric jobs :overhead)]
    (if avg
      (println "Average overhead for jobs is" (float (/ avg 1000)) "seconds.")
      (println "No jobs were finished; thus, there is no average overhead."))))

(defn job-results
  "Given a snapshot of both the simulation database and the scheduler database,
  and the id of a simulation, returns job-result data structures for each job in
  the simulation."
  [sim-db cook-db sim-id]
  (map (partial job-result sim-db cook-db)
       (actions-for-sim sim-db sim-id)))

(def probs-job-by-job [0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9])

(defn job-by-job-comparison
  "Accepts a collection of job results from a baseline sim run,
  a set of job results from a sim run to be compared to the baseline,
  and a metric to be used to compare job performance between the two sim runs.
  The two sim runs should be from the same workload in order for this to
  return anything useful.
  Metric should be one of the keys returned by (job-result).
  Returns a sequence of comparisions of the performances of each job
  from the runs.  1.0 is identical performance, 2.0 would mean that the
  compared run outperformed the baseline run for this job by double
  (e.g. this job had half the wait time in the compared run)."
  [baseline compared metric]
  (map (fn [compared-job]
         (let [base-job (first (filter #(= (:name %1) (:name compared-job)) baseline))
               base-metric (metric base-job)
               compared-metric (metric compared-job)]
           ;; If a job was never finished/started in one of the runs,
           ;; arbitrarily say the competition
           ;; performed 100x better
           (if (nil? base-metric)
             (if (nil? compared-metric) 1 100)
             (if (nil? compared-metric)
               0.01
               (/ base-metric compared-metric)))))
       compared))

(defn chart-label
  "Convenience function to return the chart label for a given simulation "
  [sim-db sim-id]
  (->> sim-id (d/entity sim-db) :sim/label))


(defn preemption-settings-as-of-sim-start
  "Given a simulation id, returns the rebalancer
  configuration (from scheduler db) at the time of the start of the simulation.
  This is useful for comparing the performance of the Scheduler with the same
  workload and cluster configuration, across various rebalancer configuration
  setting values.  NOTE: it is possible to change the rebalancer configuration
  DURING a simulation run.  This would pollute the results, so make sure it doesn't
  happen."
  [sim-db cook-db sim-id]
  (let [sim-start (u/created-at sim-db sim-id)
        old-db (d/as-of cook-db sim-start)]
    (d/pull old-db "[*]" :rebalancer/config)))

(defn preemption-settings-x-axis
  "Given a \"setting\" (keyword that is a rebalancer config parameter) set of sims,
  returns the values of the named rebalancer config paramater that were in effect
  at the start of each of the sims."
  [sim-db cook-db setting sim-ids]
  (map (fn [sim-id] (setting (preemption-settings-as-of-sim-start
                              sim-db cook-db sim-id)))
       sim-ids))

(defn add-line-for-cook-version
  "For use in a knob-turning chart (see below). Adds a single line for an individual
  version of cook's performance, relative to the baseline version."
  [& {:keys [chart sim-db cook-db baseline-sims sift-pred
             metric knob compared-sims]}]
  (let [performances
        (map (fn [baseline-sim-id compared-sim-id]
               (let [baseline-jobs (filter sift-pred (job-results sim-db cook-db baseline-sim-id))
                     compared-jobs (filter sift-pred (job-results sim-db cook-db compared-sim-id))
                     comparison (job-by-job-comparison baseline-jobs compared-jobs metric)]
                 (/ (->> comparison (filter #(>= % 1.0)) count)
                    (count comparison))))
             baseline-sims compared-sims)]
    (chart/add-categories chart
                          (preemption-settings-x-axis sim-db cook-db knob compared-sims)
                          performances
                          :series-label "comparison")))


(defn knob-turning-chart
  "compared-sim-sets:  a multi-dimensional sequence of sim runs.
  Each element in the sequence should consist of sim ids that were
  run against a specific cook codebase, cluster config & workload,
  but using different values for a given setting (\"knob\").
  E.g. [[master-branch-with-setting-a master-branch-with-setting-a]
        [dev-branch-with-setting-b dev-branch-with-setting-b]]
  X axis will be the values of the \"knob\", Y axis will be the
  percentile of job-by-job-comparison for that value where the compared build
  outperforms the baseline.  (Baseline is first member of each member of compared-sim-sets)"
  [& {:keys [sim-db cook-db metric knob baseline-sims compared-sim-sets
             sift-pred sift-label]
      :or {metric :overhead
           knob :rebalancer.config/min-dru-diff
           sift-pred identity
           sift-label "all"}}]
  (let [baseline-chart
        (chart/line-chart (preemption-settings-x-axis sim-db cook-db
                                                      knob baseline-sims)
                          (repeat (count baseline-sims) 0.5)
                          :title (str (name metric) " with changing "
                                      (name knob) " for " sift-label " jobs")
                          :x-label (name knob)
                          :y-label "Percentile outperforming baseline"
                          :legend true
                          :series-label "baseline")]
    (reduce
     #(add-line-for-cook-version :sim-db sim-db
                                 :cook-db cook-db
                                 :baseline-sims baseline-sims
                                 :sift-pred sift-pred
                                 :metric metric
                                 :knob knob
                                 :chart %1
                                 :compared-sims %2)
     baseline-chart compared-sim-sets)))


(defn add-line-for-sim
  "For use in job-by-job-comparison-chart (see below), which directly compares
  performance percentiles between two or more simulation runs.  Adds a line for a
  single sim."
  [& {:keys [chart sim-db cook-db baseline sift-pred metric compared-sim-id]}]
  (let [compared-jobs (filter sift-pred (job-results sim-db cook-db compared-sim-id))
        comparison (job-by-job-comparison baseline compared-jobs metric)
        quantiles (is/quantile comparison :probs probs-job-by-job)]
    (chart/add-categories chart probs-job-by-job quantiles
                          :series-label (chart-label sim-db compared-sim-id))))


(defn job-by-job-comparison-chart
  "Compares the performance of multple sim runs to each other with respect to a
  specific metric.
  Params:
  * sim-db-val:  a snapshot of the Simulator database
  * cook-db-val: a snapshot of the Scheduler database
  * metric: A keyword, should be a key of job-result data structure (see above),
  to be used as the basis of comparison between jobs
  * sift-pred:  A function, accepts a single argument (which will be a job-result), and returns true iff the job should be included in the comparison.  This can be used to make charts that compare only a subset of jobs in the sims, e.g. only a certain user profile.
  * sift-label:  A description of the sift-pred, will be shown on the chart and has no other effect.
  * baseline-sim-id:  The simulation run that will be used as the baseline, to which other sim runs will be compared.
  * compared-sim-ids:  The other simulation runs whose performance percentiles will be compared to baseline-sim-id on the chart.
  "
  [& {:keys [sim-db-val cook-db-val metric sift-pred sift-label
             baseline-sim-id compared-sim-ids]
      :or {sift-pred identity
           sift-label "all"}}]
  (let [baseline-label (chart-label sim-db-val baseline-sim-id)
        baseline-jobs (filter sift-pred (job-results sim-db-val cook-db-val baseline-sim-id))
        baseline-chart (chart/line-chart probs-job-by-job
                                         (repeat (count probs-job-by-job) 1)
                                         :title (str (name metric)
                                                     " compared to " baseline-label
                                                     " for " sift-label " jobs")
                                         :x-label "quantile"
                                         :y-label "worse <--> better (by what ratio)"
                                         :legend true
                                         :series-label baseline-label)]
    (reduce #(add-line-for-sim :chart %1
                               :sim-db sim-db-val
                               :cook-db cook-db-val
                               :baseline baseline-jobs
                               :sift-pred sift-pred
                               :metric metric
                               :compared-sim-id %2)
            baseline-chart compared-sim-ids)))

(defn compare-sims
  "Top level CLI function that will create a job-by-job comparison chart (see above)
  and save it to a file."
  [{:keys [sim-db cook-db filename sim-ids metric]}]
  (println "Comparing" metric "for sims" (rest sim-ids) "to baseline" (first sim-ids) "...")
  (let [sim-db-val (-> sim-db :conn d/db)
        cook-db-val (-> cook-db :conn d/db)
        chart (job-by-job-comparison-chart sim-db-val cook-db-val (keyword metric) sim-ids)]
    (println "Outputting comparison image to" filename)
    (ic/save chart filename)))

(defn job-results-from-components
  "Convenience function; given the System components corresponding to the two
  relevant databases, and a simulation id, returns a collection of job result data
  structures."
  [sim-db-component cook-db-component sim-id]
  (job-results (-> sim-db-component :conn d/db)
               (-> cook-db-component :conn d/db)
               sim-id))

(defn analyze
  "Overarching function to print out various bits of information about how the
  jobs in the sim ran."
  [settings sim-db cook-db sim-id]
  (println "Analyzing sim " sim-id "...")
  (let [job-results (job-results-from-components sim-db cook-db sim-id)]
    (println (count job-results) "jobs in sim...")
    (warn-about-unscheduled job-results)
    (warn-about-unfinished job-results)
    (show-average-wait job-results)
    (show-average-turnaround job-results)
    (show-average-overhead job-results)
    (summarize-preemption job-results)))

(defn- format-sim-result
  "Prepares a simulation id result from db/pull for output via print-table"
  [sim-db cook-db s]
  (let [id (:db/id s)
        label (:sim/label s)]
    {"ID" id
     "Label" label
     "When" (u/created-at sim-db id)
     "Min-DRU-Diff" (->> s :db/id
                         (preemption-settings-as-of-sim-start sim-db cook-db)
                         :rebalancer.config/min-dru-diff)}))

(defn list-sims
  "A Command line task; prints out info about all sims for specified schedule"
  ([sim-db cook-db sched-id]
   (list-sims sim-db cook-db sched-id #inst "1776"))
  ([sim-db cook-db sched-id after]
   (println "listing simulation runs for test" sched-id "...")
   (let [sim-db-val (-> sim-db :conn d/db)
         cook-db-val (-> cook-db :conn d/db)
         test-entity (d/entity sim-db-val sched-id)
         filtered (filter (fn [s] (> (.getTime (u/created-at sim-db-val (:db/id s)))
                                     (.getTime after)))
                          (:test/sims test-entity))
         sims (map (partial format-sim-result sim-db-val cook-db-val) filtered)]
     (pprint/print-table (sort-by (fn [r] [(r "Min-DRU-Diff") (r "When")]) sims)))))
