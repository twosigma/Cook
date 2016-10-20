(ns cook.sim.repl
  (:require [clojure.java.shell :use sh]
            [clojure.data.generators :as gen]
            [clojure.pprint :as pp]
            [clojure.algo.generic.functor :refer [fmap]]
            [clojure.string :as string]
            [clojure.math.combinatorics :as combo]
            [clojure.math.numeric-tower :as math]
            [datomic.api :as d]
            [cook.sim.cli :as cli]
            [cook.sim.database :as db]
            [cook.sim.schedule :as schedule]
            [cook.sim.runner :as runner]
            [cook.sim.reporting :as report]
            [cook.sim.system :as sys]
            [cook.sim.travis :as travis]
            [cook.sim.util :as u]
            [simulant.util :as simu]
            [com.stuartsierra.component :as component]
            [reloaded.repl :refer [system init start stop go reset reset-all]]
            [incanter.core :as ic]
            [incanter.stats :as is]
            [incanter.charts :as chart]))

(reloaded.repl/set-init! #(sys/system "config/settings.edn"))

(defn settings
  "Shortcut to obtaining the current system settings"
  []
  (-> system :config :settings))

(defn sim-db
  "Shortcut to obtain a reference to the Simulator Database system component"
  []
  (-> system :sim-db))

(defn sim-db-val
  "Shortcut to obtain a reference to the most current value
  of the Simulator Database."
  []
  (-> (sim-db) :conn d/db))

(defn sim-conn
  "Shortcut to acquire a connection to the Simulator Database."
  []
  (:conn (sim-db)))

(defn cook-db
  "Shortcut to obtain a reference to the Scheduler Database system component"
  []
  (-> system :cook-db))

(defn cook-db-val
  "Shortcut to obtain a reference to the most current value
  of the Scheduler Database."
  []
  (-> (cook-db) :conn d/db))

(defn setup-database
  "Shortcut to set up a new Simulator database based on current settings."
  []
  (db/setup-database! (settings)))

(defn generate-schedule
  "Shortcut to generate up a new job schedule based on current settings."
  []
  (schedule/generate-job-schedule! (settings) "schedule.edn"))

(defn import-schedule
  "Shortcut to import a work schedule into Datomic that already exists on the
  filesystem, at the hardcoded location schedule.edn."
  []
  (schedule/import-schedule! (sim-db) "schedule.edn"))

(defn kill-cook-db
  "Shortcut to completely destroy the Cook Scheduler database.  Obviously,
  don't do this to anything resembling a production database!"
  []
  (-> (settings) :cook-db-uri datomic.api/delete-database))

(defn list-scheds
  "Shortcut to list all job schedules currently defined in the Simulator datomic
  database.  In order for a schedule to appear here, it must have been imported
  (see above)."
  []
  (schedule/list-job-schedules (sim-db)))

(defn list-sims
  "Shortcut to print out some info about all  simulations that have been run for
  a given work schedule.  The optional after param allows you to only list sims
  that were started after a specific time."
  ([sched-id]
   (list-sims sched-id #inst "0000"))
  ([sched-id after]
   (report/list-sims (sim-db) (cook-db) sched-id after)))

(defn simulate!
  "Shortcut to run a simulation for a specific schedule given the current
  system configuration.  Label param is mandatory; trust me, once you've run
  a good number of simulations, you'll appreciate having labelled them so you
  can remember which is which!"
  [sched-id label]
  (runner/simulate! (settings) (sim-db) sched-id label))

(defn analyze
  "Shortcut to print out a rough summmary of how jobs performed for a given
  simulation."
  [sim-id]
  (report/analyze (settings) (sim-db) (cook-db) sim-id))

(defn compare-sims
  "Shortcut to write a comparison chart for a set of sim-ids to the hardcoded
  file compare_chart.png, using current system components."
  [sim-ids]
  (report/compare-sims {:sim-db (sim-db)
                        :cook-db (cook-db)
                        :filename "compare_chart.png"
                        :sim-ids sim-ids}))

(defn compare-chart
  "Get a reference to a comparison chart for a given metric between
  specific simulation ids, and a username prefix pattern.  If the username prefix
  parameter is empty, jobs for all usernames will be included in the chart; otherwise,
  only jobs whose username begins with the prefix will be included."
  [metric sim-ids username-prefix]
  (report/job-by-job-comparison-chart
   :sim-db-val (sim-db-val)
   :cook-db-val (cook-db-val)
   :metric metric
   :baseline-sim-id (first sim-ids)
   :compared-sim-ids (rest sim-ids)
   :sift-label (if (empty? username-prefix) "all" username-prefix)
   :sift-pred #(string/starts-with? (:username %) username-prefix)))

(defn view-compare-chart
  "Shortcut to open a Java UI window that shows a comparison chart for a specific
  metric and set of job-ids.  Optionally, a username prefix filter may be included.
  If present only jobs whose username begins with the username filter will be
  represented on the chart."
  ([metric sim-ids]
   (view-compare-chart metric sim-ids ""))
  ([metric sim-ids username-prefix]
   (ic/view (compare-chart metric sim-ids username-prefix))))


(defn knob-chart
  "Gets a reference to a knob-turning chart.  (see reporting.clj).  If
  username-prefix is supplied, only jobs whose username begins with the username
  filter will be represented on the chart."
  [metric sim-ids username-prefix]
  (report/knob-turning-chart :sim-db (sim-db-val)
                             :cook-db (cook-db-val)
                             :metric metric
                             :knob :rebalancer.config/min-dru-diff
                             :baseline-sims (first sim-ids)
                             :compared-sim-sets (rest sim-ids)
                             :sift-label (if (empty? username-prefix) "all" username-prefix)
                             :sift-pred #(string/starts-with? (:username %) username-prefix)))

(defn save-knob-chart
  "Shortcut to generate a knob-turning chart and save it to a specific filename.
  "
  ([sim-ids metric]
   (save-knob-chart metric sim-ids ""))
  ([sim-ids metric username-prefix]
   (ic/save (knob-chart metric sim-ids username-prefix)
            (str (name metric) "_"
                 (if (empty? username-prefix) "all" username-prefix)
                 ".png")
            :width 1200 :height 800)))

(defn view-knob-chart
  ([metric sim-ids]
   (view-knob-chart metric sim-ids ""))
  ([metric sim-ids username-prefix]
   (ic/view (knob-chart metric sim-ids username-prefix))))

(defn save-compare-chart
  ([sim-ids metric]
   (save-compare-chart metric sim-ids ""))
  ([sim-ids metric username-prefix]
   (ic/save (compare-chart metric sim-ids username-prefix)
            (str (name metric) "_"
                 (if (empty? username-prefix) "all" username-prefix)
                 ".png")
            :width 1000 :height 800)))

(defn save-all-compare-graphs
  [sim-ids]
  (map (fn [[metric username-prefix]]
         (save-compare-chart sim-ids metric username-prefix))
       (combo/cartesian-product [:wait-time :overhead :turnaround :instance-count]
                                ["" "sim" "fcat" "spark"])))

(defn save-all-knob-graphs
  [sim-ids]
  (map (fn [[metric username-prefix]]
         (save-knob-chart sim-ids metric username-prefix))
       (combo/cartesian-product [:wait-time :overhead :turnaround :instance-count]
                                ["" "sim" "fcat" "spark"])))


(defn job-results
  [sim-id]
  (report/job-results (sim-db-val) (cook-db-val) sim-id))

(defn backup-cook!
  [{sim-id :entity-id}]
  (let [{:keys [backup-directory cook-db-uri]} settings
        backup-dest (str backup-directory sim-id)]
    (println "backing up cook db up to " backup-dest)
    (println (sh "datomic" "backup-db" cook-db-uri backup-dest))))

(defn pull-entity
  [db eid]
  (d/pull db "[*]" eid))

(def dru-scale (math/expt 10 305))
(defn dru-at-scale
  [dru]
  (* dru dru-scale))

(defn update-min-dru-diff!
  "For testing performance with various levels of pre-emption.
  This will update the real Cook's settings!"
  [scaled-val]
  @(d/transact (:conn (cook-db)) [{:db/id :rebalancer/config
                                   :rebalancer.config/min-dru-diff (/ scaled-val dru-scale)}]))

(defn read-preemption-settings
  "Returns Cook's current pre-emption settings."
  []
  (d/pull (cook-db-val) ["*"] :rebalancer/config))
