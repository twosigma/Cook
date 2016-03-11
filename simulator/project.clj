(defproject cook/sim "0.1.0-SNAPSHOT"
  :description "Simulation tests for Cook"
  :dependencies   [[org.clojure/clojure "1.7.0"]
                   [clj-time "0.9.0"]
                   [cheshire "5.5.0"]
                   [com.datomic/datomic-free "0.9.5344"
                    :exclusions [org.clojure/clojure joda-time]]
                   [com.datomic/simulant "0.1.8"]
                   [com.stuartsierra/component "0.3.1"]
                   [org.clojure/data.generators "0.1.2"]
                   [org.clojure/tools.cli "0.3.3"]
                   ;; [reloaded.repl "0.2.1"]
                   [clj-http "2.0.1"]]
  :resource-paths ["resources"]
  :main           cook.sim.cli
  :source-paths ["src/main"]
  :profiles {:dev {:source-paths ["src/dev"]
                   :repl-options {:init-ns cook.sim.repl}
                   :dependencies [[reloaded.repl "0.2.1"]]}})
