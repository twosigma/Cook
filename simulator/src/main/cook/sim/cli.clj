(ns cook.sim.cli
  (:require [clojure.string :as string]
            [clojure.tools.cli :refer [parse-opts]]
            [cook.sim.database :as db]
            [cook.sim.schedule :as schedule]
            [cook.sim.runner :as runner]
            [cook.sim.reporting :as reporting]
            [cook.sim.system :as sys]
            [com.stuartsierra.component :as component])
  (:gen-class))

(def cli-options
  [["-c" "--config FILE" "Specify the config file to use for Cook Simulator."
    :default "config/settings.edn"]
   ["-e" "--entity-id ID" "Specify the test/sim id"
    :parse-fn #(Long/parseLong (string/trim %))]
   ["-h" "--help" "Show help and exit"]])

(defn usage [options-summary]
  (->> ["lein run -c (config_file) task"]
       (string/join \newline)))

(defn error-msg [errors]
  (str "Couldn't interpret your command line:\n\n"
       (string/join \newline errors)))

(defn exit
  ([status] (System/exit status))
  ([status msg]
   (prn msg)
   (System/exit status)))

(defn- start-component
  [config component-function]
  (-> config component-function component/start))

(defn -main [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]

    (cond
      (:help options) (exit 0 (usage summary))
      (not= (count arguments) 1) (exit 1 (usage summary))
      errors (exit 1 (error-msg errors)))

    (let [cfg (-> :config options sys/new-config component/start)
          settings (:settings cfg)
          cmd (first arguments)]

      (if (= cmd "setup-database")
        (db/setup-database! settings)
        (let [sim-db (start-component cfg sys/new-sim-db)
              eid (:entity-id options)]
          (case cmd
            "generate-job-schedule" (schedule/generate-job-schedule! settings sim-db)
            "list-job-schedules" (schedule/list-job-schedules sim-db)
            "simulate" (runner/simulate! settings sim-db eid)
            "list-sims" (reporting/list-sims sim-db eid)
            "report" (let [cook-db  (start-component cfg sys/new-cook-db)]
                       (reporting/analyze settings sim-db cook-db eid))
            (exit 1 (usage summary)))
          ))
      (exit 0)
      )))
