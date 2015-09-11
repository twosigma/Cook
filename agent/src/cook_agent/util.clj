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
(ns cook-agent.util
  (:refer-clojure :exclude [cast merge sort empty split replace])
  (:require [riemann.client :as riemann]
            [clojure.java.io :as io]
            [clojure.string :refer (join)]
            [clojure.tools.logging :as log]))

(def riemann (delay (riemann/tcp-client :host (System/getProperty "cook.riemann"))))

(defn riemann-send
  "Takes some events, and sends them over the riemann connection!"
  [& events]
  (riemann/send-events @riemann events true))


(defmacro swap!-str-and-log
  [a & args]
  `(let [s# (str ~@args)]
     (log/info s#)
     (swap! ~a str s#)))

(def version
  (try (slurp (io/resource "git-log"))
       (catch Exception e
         "dev")))

(defn get-html-stacktrace
  "Returns a string representation of the exception. The 3 argument form fleshes
   out information about the thread, user, and host."
  ([exception thread]
   (str "thread name: " (.getName thread)
        "\n\n user: " (System/getProperty "user.name")
        "\n\n host: " (.getHostName (java.net.InetAddress/getLocalHost))
        "\n\n commit: " version
        "\n\n" (get-html-stacktrace exception)))
  ([exception]
   (str exception "\n"
        (->> (.getStackTrace exception)
             (map #(str "  " (.getClassName %) "/" (.getMethodName %) "(" (.getFileName %) ":" (.getLineNumber %) ")"))
             (join "\n"))
        (when-let [cause (.getCause exception)]
          (str "\nCaused by:" (get-html-stacktrace cause))))))

(defn install-email-on-exception-handler
  []
  (Thread/setDefaultUncaughtExceptionHandler
    (reify Thread$UncaughtExceptionHandler
      (uncaughtException [_ thread e]
        (log/error e "Uncaught exception on thread" (.getName thread))
        (let [stacktrace (get-html-stacktrace e)]
          (riemann-send {:service "Cook Cloud Uncaught Exceptions"
                         :state "error"
                         :description stacktrace}))))))
