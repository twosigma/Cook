(ns cook.sim.reporting.groups
  (:require [clojure.pprint :as pprint]
            [cook.sim.database :as data]
            [cook.sim.util :as u]
            [datomic.api :as d]))


(defn jobs->valid-group->jobs
  "Given a collection of job-results, returns only the jobs with groups,
  in a map keyed by group name."
  [job-results]
  (dissoc (group-by :group job-results) nil))

(defn jobs->first-request-time
  "Given a collection of job results, return the earliest time that any was requested."
  [job-results]
  (->> job-results (map :requested-at) (apply min)))

(defn jobs->sorted-valid-start-times
  "Returns the start times for jobs that HAVE start times, ordered"
  [job-results]
  (->> job-results (map :started-at) (remove nil?) sort))

(defn jobs->wait-for-first
  "Given a collection of job results, return the time between when the first one
  was requested and the first time any of them was scheduled."
  [job-results]
  (let [time (-> job-results jobs->sorted-valid-start-times first)]
    (when time (- time (jobs->first-request-time job-results)))))

(defn jobs->wait-for-last
  "Given a collection of job results, return the time between when the first one
  was requested and the latest time any of them was scheduled."
  [job-results]
  (let [time (-> job-results jobs->sorted-valid-start-times last)]
    (when time (- time (jobs->first-request-time job-results)))))

(defn jobs->runtime
  "Given a collection of job results, return the time between when the first moment
  any of them was started requested and the last time when one finished."
  [job-results]
  (let [first-start (-> job-results jobs->sorted-valid-start-times first)
        last-finish (->> job-results (map :finished-at) (remove nil?) (apply max))]
    (when (and first-start last-finish)
      (- last-finish first-start))))

(defn average-by-group
  "Separates a collection of job-results into groups, performs the given summary
  function on the jobs in each group, and returns the average of these results
  across all groups."
  [summary-fn job-results]
  (let [groups (jobs->valid-group->jobs job-results)
        total-ms (->> groups
                      vals
                      (map summary-fn)
                      (remove nil?)
                      (apply +))]
    (when (seq groups)
      (/ total-ms (count groups)))))

(defn show-wait-for-first
  "If the job-results have groups, print out the average wait for first job
  to be scheduled in each group."
  [job-results]
  (when-let [avg (average-by-group jobs->wait-for-first job-results)]
    (println "Average wait for first job in groups:" (u/seconds avg) "seconds.")))

(defn show-wait-for-last
  "If the job-results have groups, print out the average wait for last job
  to be scheduled in each group."
  [job-results]
  (when-let [avg (average-by-group jobs->wait-for-last job-results)]
    (println "Average wait for last job in groups:" (u/seconds avg) "seconds.")))

(defn show-runtime
  "If the job-results have groups, print out the average difference between
   when the first job starts and when the last job finishes in each group."
  [job-results]
  (when-let [avg (average-by-group jobs->runtime job-results)]
    (println "Average total runtime of groups:" (u/seconds avg) "seconds.")))
