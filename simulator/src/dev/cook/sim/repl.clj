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

(defn list-models
  [{conn :conn}]
  (prn "listing models...")
  (let [db (d/db conn)]
    (prn (d/q '[:find [?e ...]
                :where [?e :model/type :model.type/cook]]
              db))))

(defn settings
  []
  (-> system :config :settings))

(defn sim-db
  []
  (-> system :sim-db))

(defn sim-db-val
  []
  (-> (sim-db) :conn d/db))

(defn sim-conn
  []
  (:conn (sim-db)))

(defn cook-db
  []
  (-> system :cook-db))

(defn cook-db-val
  []
  (-> (cook-db) :conn d/db))

(defn setup-database
  []
  (db/setup-database! (settings)))

(defn generate-schedule
  []
  (schedule/generate-job-schedule! (settings) "schedule.edn"))

(defn import-schedule
  []
  (schedule/import-schedule! (sim-db) "schedule.edn"))

(defn kill-cook-db
  []
  (-> (settings) :cook-db-uri datomic.api/delete-database))

(defn list-scheds
  []
  (schedule/list-job-schedules (sim-db)))

(defn list-sims
  ([sched-id]
   (list-sims sched-id #inst "0000"))
  ([sched-id after]
   (report/list-sims (sim-db) (cook-db) sched-id after)))

(defn simulate!
  [sched-id label]
  (runner/simulate! (settings) (sim-db) sched-id label))

(defn analyze
  [sim-id]
  (report/analyze (settings) (sim-db) (cook-db) sim-id))

(defn compare-sims
  [sim-ids]
  (report/compare-sims {:sim-db (sim-db)
                        :cook-db (cook-db)
                        :filename "compare_chart.png"
                        :sim-ids sim-ids}))

(defn compare-chart
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
  ([metric sim-ids]
   (view-compare-chart metric sim-ids ""))
  ([metric sim-ids username-prefix]
   (ic/view (compare-chart metric sim-ids username-prefix))))


(defn knob-chart
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


(comment
  (if backup-after-seconds
    (do
      (println "waiting " backup-after-seconds "seconds before backing up...")
      (Thread/sleep (* 1000 backup-after-seconds))
      (backup-cook! {:entity-id (:db/id cook-sim)}))
    (println "Not configured to do a backup of cook db.")))
