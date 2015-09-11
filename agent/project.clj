;;
;; Copyright (c) Two Sigma Open Source, LLC
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;  http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(defproject cook-agent "0.0.1-SNAPSHOT"
  :description "Cook Agent"
  :license {:name "Two Sigma Proprietary"}
  :dependencies [[byte-streams "0.1.4"]
                 [byte-transforms "0.1.0"]
                 [cheshire "5.3.1"]
                 [clj-http "2.0.0"]
                 [clj-logging-config "1.9.10"]
                 [clj-mesos "0.20.5"]
                 [clj-time "0.8.0"]
                 [com.draines/postal "1.11.0"]
                 [jarohen/chime "0.1.6"]
                 [me.raynes/conch "0.5.2"]
                 [metrics-clojure "2.3.0"]
                 [nathanmarz/tools.cli "0.2.2"]
                 [org.clojure/algo.generic "0.1.2"]
                 [org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.clojure/data.json "0.2.5"]
                 [org.clojure/math.numeric-tower "0.0.4"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.clojure/tools.namespace "0.2.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.5"]
                 [prismatic/schema "0.2.4"]
                 [riemann-clojure-client "0.2.10"]]

  :repositories {"stuartsierra-releases" "http://stuartsierra.com/maven2"}

  :plugins [[lein-voom "0.1.0-20150822_000839-g763d315"]]

  :profiles {:release {:aot [cook-agent.core]}}

  :aliases {"release-jar" ["do" "clean," "voom" "build-deps," "with-profile" "release" "uberjar"]})
