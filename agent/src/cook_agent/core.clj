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
(ns cook-agent.core
  (:require [cook-agent.mesos :refer (get-mesos-environment)]
            [clj-http.client :as client]
            [cook-agent.util :as util]
            [clj-logging-config.log4j :as log4j-conf]
            [clojure.java.io :as io]
            [cook-agent.common.runner :refer (run-job)]
            [me.raynes.conch.low-level :as sh]
            [clojure.pprint :refer (pprint)]
            [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:use [clojure.tools.cli :only [cli]])
  (:gen-class))

(defn -main
  [& args]
  (log4j-conf/set-loggers! (org.apache.log4j.Logger/getRootLogger)
                           {:out (org.apache.log4j.DailyRollingFileAppender.
                                   (org.apache.log4j.PatternLayout.
                                     "%d{ISO8601} %-5p %c [%t] - %m%n")
                                   (.getPath (io/file (System/getenv "MESOS_DIRECTORY") "cook-agent.log"))
                                   "'.'yyyy-MM-dd")
                            :level :info})
  (util/install-email-on-exception-handler)
  (pprint (into {} (System/getenv)))
  ;;run sim
  (try
    (let [{:keys [output-dir ipc-sink instance finished-success finished-fail] :as environment} (get-mesos-environment)
          process (run-job (assoc environment :commands args))
          exit-code (sh/exit-code process)]
      (if (zero? exit-code) ;; We report our status to Mesos
        (finished-success)
        (finished-fail))
      (Thread/sleep 3000)
      (System/exit exit-code))
    ;; If this finally is reached, it's abnormal termination (an exception somewhere above)
    (catch Throwable t
      (clojure.stacktrace/print-cause-trace t)
      ;; Stacktrace won't be printed without the following println for some reason
      (println)
      (let [stacktrace (util/get-html-stacktrace t (Thread/currentThread))]
        (util/riemann-send {:service "Cook Agent Failure"
                            :state "error"
                            :description stacktrace})))
    (finally (System/exit 1))))
