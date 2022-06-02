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

;; This file implements structured logging.

;; A structured log line consists of a human-readable string "Committing Blah to Blah", followed by a metadata dictionary.
;; (structured-log/info "Launching task " {:pool ... ... ...}"

;; This utility's usage is twofold:
;; - adding simple metadata tags (e.g., pool name, compute cluster name) to regular log messages
;; - emitting log metrics with a more complicated dictionary structure (e.g., the match cycle metrics)

;; Within the metadata dictionary, we are standardizing the following key names to be used, where relevant:
; :pool
; :compute-cluster
; :user
; :instance-id
; :job-id

;; Warning: this utility provides the ability to override the default logger namespace of messages.
;; This should be used very sparingly, for specific use cases that require it.

(ns cook.log-structured
  (:require
    [clojure.data.json :as json]
    [clojure.tools.logging :as log]
    [cook.util :as util]))

(defn safe-jsonify
  "Tries to convert the given map to json and return the json string representation.
  If conversion to json fail, returns just the map as-is."
  [log-map]
    (try
      (json/write-str (util/format-map-for-structured-logging log-map))
      (catch Exception e
        (log/error e "Unable to convert to json for structured logging" (str log-map))
        log-map)))

(defmacro logs
  "Logs a structured message at the specified level.
  Supports specifying a custom logger namespace as an optional parameter."
  ([level data metadata]
   `(log/log ~level (safe-jsonify (assoc ~metadata :data ~data))))
  ([level data metadata throwable]
   `(log/log ~level (safe-jsonify (assoc ~metadata :data ~data
                                                   :error (with-out-str (clojure.stacktrace/print-throwable ~throwable))
                                                   :stacktrace (with-out-str (clojure.stacktrace/print-cause-trace ~throwable))))))
  ([level data metadata throwable logger-ns]
   (if (nil? throwable)
     `(log/log ~logger-ns ~level nil (safe-jsonify (assoc ~metadata :data ~data)))
     `(log/log ~logger-ns ~level nil (safe-jsonify (assoc ~metadata :data ~data
                                                                    :error (with-out-str (clojure.stacktrace/print-throwable ~throwable))
                                                                    :stacktrace (with-out-str (clojure.stacktrace/print-cause-trace ~throwable))))))))

(defmacro debug
  "Logs a structured message at the debug level"
  {:arglists '([data metadata] [data metadata throwable] [data metadata throwable logger-ns])}
  [& args]
  `(logs :debug ~@args))

(defmacro info
  "Logs a structured message at the info level"
  {:arglists '([data metadata] [data metadata throwable] [data metadata throwable logger-ns])}
  [& args]
  `(logs :info ~@args))

(defmacro warn
  "Logs a structured message at the warn level"
  {:arglists '([data metadata] [data metadata throwable] [data metadata throwable logger-ns])}
  [& args]
  `(logs :warn ~@args))

(defmacro error
  "Logs a structured message at the error level"
  {:arglists '([data metadata] [data metadata throwable] [data metadata throwable logger-ns])}
  [& args]
  `(logs :error ~@args))
