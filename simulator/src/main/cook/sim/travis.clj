(ns cook.sim.travis
  (:require [clojure.string :as string]
            [clojure.tools.cli :refer [parse-opts]]
            [clj-http.client :as http]
            [cook.sim.database :as db]
            [cook.sim.schedule :as schedule]
            [cook.sim.runner :as runner]
            [cook.sim.reporting :as reporting]
            [cook.sim.system :as sys]
            [com.stuartsierra.component :as component]
            [robert.bruce :refer [try-try-again]]))


(defn wait-for-cook
  "Given the app settings, waits for the configured Cook API to become available.
  Simply retries an HTTP connection; as soon as the connection isn't refused stop
  waiting."
  [settings]
  (try-try-again {:sleep 5000 :tries 20
                  :error-hook #(println "Cook API not up yet:" (.getMessage %))}
                 ;; will still throw exception on connection refused.
                 http/get (:cook-api-uri settings) {:throw-exceptions false}))


(defn sim-finished?
  [sim-db cook-db sim-id]
  (let [jobs (reporting/job-results-from-components sim-db cook-db sim-id)
        count-unscheduled (count (remove :wait-time jobs))
        count-unfinished (count (reporting/unfinished-jobs jobs))]
    (println count-unscheduled "unscheduled jobs.")
    (println count-unfinished "unfinished jobs.")
    (and (zero? count-unscheduled) (zero? count-unfinished))))

(defn wait-for-sim-to-finish
  [sim-db cook-db sim-id timeout-seconds]
  (let [sleep-seconds 5]
    (try-try-again {:sleep (* 1000 sleep-seconds)
                    :tries (/ timeout-seconds sleep-seconds)
                    :return? :truthy?
                    :error-hook #(println "Sim not finished yet." %)}
                   sim-finished? sim-db cook-db sim-id)))


(defn perform-ci
  "Top level function (invoked via the cli) that runs a simulation
  with specific parameters designed specifically to serve as a smoke test on
  Travis ci.  Raises an exception if any schedulable jobs in the simulation
  don't get finished within a certain period of time.  Also raises an exception
  if any intentionally unschelable jobs DO get finished.  Such exceptions will
  cause the process to exit with a non-zero code, which in turn will cause the
  Travis build to fail."
  [settings sim-db cook-db]
  (let [timeout-secs (* 5 60)
        file "travis-schedule.edn"
        _ (schedule/generate-job-schedule! settings file)
        schedule-id (schedule/import-schedule! sim-db file)
        _ (wait-for-cook settings)
        sim-id (runner/simulate! settings sim-db schedule-id "Travis run")
        finished? (wait-for-sim-to-finish sim-db cook-db sim-id timeout-secs)]
    (reporting/analyze settings sim-db cook-db sim-id)
    (if (not finished?) (throw (Exception. "Sim never finished.")))))
