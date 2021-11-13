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
(ns cook.other.map-mock
  (:require [clojure.string :as str]
            [potemkin.collections :refer [def-map-type]])
  (:import (java.util Arrays)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.concurrent.atomic LongAdder)
           (java.util.function Function)))

(defn ignored? [classname]
  (let [ignored #{"callers" "dbg" "clojure.lang" "swank" "nrepl" "eval"}]
    (some #(re-find (re-pattern %) classname) ignored)))

(defn cook-callers []
  (let [fns (map #(str (.getClassName %) ":" (.getLineNumber %))
                 (-> (Throwable.) .fillInStackTrace .getStackTrace))]
    ;(vec (doall (remove ignored? fns)))
    ;(vec (doall (filter #(str/starts-with? % "cook.") fns)))
    (vec fns)))

(def ^Function make-map-if-absent
  (reify Function (apply [_ _] (ConcurrentHashMap.))))

(def ^Function make-long-adder-if-absent
  (reify Function (apply [_ _] (LongAdder.))))

(def ^ConcurrentHashMap access-map
  (ConcurrentHashMap.))

(declare make-access-logging-map)

(def-map-type
  AccessLoggingMapType [backing-map entity-name]
  (get [_ field-name default-value]
       (let [^ConcurrentHashMap access-map-by-entity (.computeIfAbsent access-map entity-name make-map-if-absent)
             ^ConcurrentHashMap access-map-by-field (.computeIfAbsent access-map-by-entity field-name make-map-if-absent)
             callstack (-> (cook-callers) distinct into-array Arrays/asList)
             ^LongAdder adder (.computeIfAbsent access-map-by-field callstack make-long-adder-if-absent)]
         (.increment adder))
       (let [result (get backing-map field-name default-value)]
         (if (instance? datomic.query.EntityMap result)
           (make-access-logging-map result (keyword (str (name entity-name) "_" (str field-name))))
           result)))
  (assoc [_ k v] (assoc backing-map k v))
  (dissoc [_ k] (dissoc backing-map k))
  (keys [_] (keys backing-map))
  (meta [_] (meta backing-map))
  (empty [_] {})
  (with-meta [_ new-meta] (with-meta backing-map new-meta)))

(defn make-access-logging-map
  [backing-map entity-name]
  (->AccessLoggingMapType backing-map entity-name))