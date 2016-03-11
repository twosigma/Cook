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

(defn parse-integer-list
  [str]
  (map #(Long/parseLong %) (string/split str #",")))

(def cli-options
  [["-c" "--config FILE" "Specify the config file to use for Cook Simulator."
    :default "config/settings.edn"]
   ["-e" "--entity-id ID" "Specify the test/sim id"
    :parse-fn #(Long/parseLong (string/trim %))]
   ["-f" "--file name" "Specify the filename"
    :default "generated-schedule.edn"]
   ["-l" "--label LABEL" "Label the schedule or sim"]
   ["-metric" "--metric METRIC" "Which metric to report on (for compare task)"]
   [nil "--compare IDS"
    "Comma-separated list of ID's of test runs to compare against baseline (only for compare task)"
    :parse-fn parse-integer-list]
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
              cook-db (if (contains?  #{"list-sims" "report" "compare"} cmd)
                        (start-component cfg sys/new-cook-db))
              {:keys [entity-id compare file label metric]} options]
          (case cmd
            "generate-job-schedule" (schedule/generate-job-schedule! settings file)
            "import-job-schedule" (schedule/import-schedule! sim-db file)
            "list-job-schedules" (schedule/list-job-schedules sim-db)
            "simulate" (runner/simulate! settings sim-db entity-id label)
            "list-sims" (reporting/list-sims sim-db cook-db entity-id)
            "report" (reporting/analyze settings sim-db cook-db entity-id)
            "compare" (reporting/compare-sims {:sim-db sim-db
                                               :cook-db cook-db
                                               :filename file
                                               :metric metric
                                               :sim-ids (apply merge [entity-id] compare)})
            (exit 1 (usage summary)))
          ))
      (exit 0)
      )))
