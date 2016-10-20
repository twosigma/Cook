(ns cook.sim.cli
  (:require [clojure.string :as string]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.stacktrace :as stacktrace]
            [cook.sim.database :as db]
            [cook.sim.schedule :as schedule]
            [cook.sim.runner :as runner]
            [cook.sim.reporting :as reporting]
            [cook.sim.travis :as travis]
            [cook.sim.system :as sys]
            [com.stuartsierra.component :as component])
  (:gen-class))

(defn parse-integer-list
  "Accepts a comma-delimited list of ids e.g. 4,5,6;
  Returns a sequence of Longs."
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

(defn usage
  "Prints out rudimentary usage info to stdout."
  [options-summary]
  (->> ["lein run -c (config_file) task"]
       (string/join \newline)))

(defn error-msg
  "Given a set of error messages, nicely prints them all out with an explanatory
  heading."
  [errors]
  (str "Couldn't interpret your command line:\n\n"
       (string/join \newline errors)))

(defn exit
  "Exits the process with a specific code and optional error message."
  ([status] (System/exit status))
  ([status msg]
   (println msg)
   (System/exit status)))

(defn- start-component
  "Convenience function: starts a system component given the configuration and
  function reference"
  [config component-function]
  (-> config component-function component/start))

(defn -main
  "Main command line entry point.  Parses the command line and performs
  various actions depending on its contents."
  [& args]
  (try
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
                cook-db (if (contains?  #{"list-sims" "report" "compare" "travis"} cmd)
                          (start-component cfg sys/new-cook-db))
                {:keys [entity-id compare file label metric]} options]
            (case cmd
              "generate-job-schedule" (schedule/generate-job-schedule! settings file)
              "import-job-schedule" (schedule/import-schedule! sim-db file)
              "list-job-schedules" (schedule/list-job-schedules sim-db)
              "simulate" (runner/simulate! settings sim-db entity-id label)
              "list-sims" (reporting/list-sims sim-db cook-db entity-id)
              "report" (reporting/analyze settings sim-db cook-db entity-id)
              "travis" (travis/perform-ci settings sim-db cook-db)
              "compare" (reporting/compare-sims {:sim-db sim-db
                                                 :cook-db cook-db
                                                 :filename file
                                                 :metric metric
                                                 :sim-ids (apply merge [entity-id] compare)})
              (exit 1 (usage summary))))))
      (exit 0))
    (catch Exception e
      (stacktrace/print-cause-trace e)
      (exit 1 "An exception occurred while handling the command."))))
