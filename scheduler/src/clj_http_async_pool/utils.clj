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
(ns clj-http-async-pool.utils
  (:require [clojure.tools.logging :as log]))

(defn map-xf
  "Implements the 1-ary version of map that creates a transducer until
  we move to 1.7."
  [f]
  (fn [rf]
    (fn
      ([] (rf))
      ([result] (rf result))
      ([result input]
       (rf result (f input)))
      ([result input & inputs]
       (rf result (apply f input inputs))))))

(defn filter-xf
  "Implements the 1-ary version of filter that creates a transducer
  until we move to 1.7."
  [f]
  (fn [rf]
    (fn
      ([] (rf))
      ([result] (rf result))
      ([result input]
       (when (f input)
         (rf result input))))))

(def log-start-stop? false)
(defmacro log-start
  [name & body]
  `(do
     ~(when log-start-stop? `(log/info ";; Starting:" ~name))
     (let [result# (do ~@body)]
       ~(when log-start-stop? `(log/info ";; Started:" ~name))
       result#)))
(defmacro log-stop
  [name & body]
  `(do
     ~(when log-start-stop? `(log/info ";; Stopping:" ~name))
     (let [result# (do ~@body)]
       ~(when log-start-stop? `(log/info ";; Stopped:" ~name))
       result#)))
