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
(ns cook.util
  (:refer-clojure :exclude [cast merge empty split replace])
  (:require [clojure.data :as data]
            [clojure.java.io :as io :refer [file reader]]
            [clojure.java.shell :refer :all]
            [clojure.pprint :refer :all]
            [clojure.string :refer :all]
            [clojure.tools.logging :as log]
            [clojure.tools.namespace.dependency :refer :all]
            [plumbing.core :as pc]
            [postal.core :as postal]
            [schema.core :as s])
  (:import (java.util.concurrent.atomic AtomicLong)
           (org.joda.time DateTime ReadablePeriod)))
; To avoid circular dependencies, this namespace should depend on no cook namespaces.

(defmacro try-timeout
  "Evaluates an expression in a separate thread and kills it if it takes too long"
  [millis failure & body]
  `(let [result# (promise)
         except# (promise)
         action-fn#
         (fn []
           (deliver result#
                    (try ~@body
                         (catch Exception e#
                           (deliver except# e#)))))
         action-thread# (Thread. action-fn#)]
     (.start action-thread#)
     (.join action-thread# ~millis)
     (cond (realized? except#) (throw @except#)
           (realized? result#) @result#
           :else
           (do (.stop action-thread#)
               ~failure))))

(defonce thread-counter (AtomicLong.))

(defmacro thread
  "Runs the body in a new thread"
  [& body]
  `(.start (Thread. (fn* [] ~@body)
                    (str "cook-util-"
                         (.getAndIncrement
                           ^AtomicLong thread-counter)))))

(defn quantile
  [coll]
  (let [s-coll (sort coll)
        n (count s-coll)]
    (if (zero? n)
      [0 0 0 0 0]
      [(first s-coll)
       (nth s-coll (int (/ n 4)))
       (nth s-coll (int (/ n 2)))
       (nth s-coll (int (* 3 (/ n 4))))
       (last s-coll)])))

(defn min-max
  [coll]
  (let [s-coll (sort coll)]
    [(first s-coll)
     (last s-coll)]))

(defn exponential-seq
  "Return a lazy-seq sequence of
   '(scalar scalar*base scalar*base^2 scalar*base^3 ...)"
  [scalar base]
  (cons scalar (lazy-seq (exponential-seq (* scalar base) base))))

(defn rand-exponential-seq
  "Return a randon exponential sequence of lengh n."
  ([n]
   (rand-exponential-seq n 1 2))
  ([n scalar]
   (rand-exponential-seq n scalar 2))
  ([n scalar base]
   (mapv rand-int (take n (exponential-seq scalar base)))))

(defn- resource
  "Returns the slurped contents of the given resource, or nil"
  [resource-name]
  (try
    (slurp (io/resource resource-name))
    (catch Exception _
      nil)))

(def commit (delay (or (resource "git-log")
                       "dev")))

(def version (delay (or (resource "version")
                        (System/getProperty "cook.version")
                        "version_unknown")))

(defn principal->username
 "Convert a Kerberos-style principal to a Mesos username."
 [principal]
 (-> principal (split #"[/@]" 2) first))

(defn get-html-stacktrace
  "Returns a string representation of the exception. The 3 argument form fleshes
   out information about the thread, user, and host."
  ([exception thread]
   (str "thread name: " (.getName thread)
        "\n\n user: " (System/getProperty "user.name")
        "\n\n host: " (.getHostName (java.net.InetAddress/getLocalHost))
        "\n\n version: " @version
        "\n\n commit: " @commit
        "\n\n" (get-html-stacktrace exception)))
  ([exception]
   (str exception "\n"
        (->> (.getStackTrace exception)
             (map #(str "  " (.getClassName %) "/" (.getMethodName %) "(" (.getFileName %) ":" (.getLineNumber %) ")"))
             (join "\n"))
        (when-let [cause (.getCause exception)]
          (str "\nCaused by:" (get-html-stacktrace cause))))))

(defn install-email-on-exception-handler
  "When an exception isn't caught anywhere, this installs a global handler.
   It will log the exception at the given log-level (:error is recommended),
   and it will send an email to the address specified by the email-config.

   email-config comes from the postal library. Usually, you'll specify
   a map like:
   {:to [\"admin@example.com\"]
    :from \"cook@example.com\"
    :subject \"unhandled exception in cook\"}"
  [log-level email-config]
  (Thread/setDefaultUncaughtExceptionHandler
    (reify Thread$UncaughtExceptionHandler
      (uncaughtException [_ thread e]
        (log/logp log-level e "Uncaught exception on thread" (.getName thread))
        (when email-config
          (let [stacktrace (get-html-stacktrace e)]
            (postal/send-message (-> email-config
                                     (update-in [:subject] str " on thread " (.getName thread))
                                     (assoc :body stacktrace)))))))))

(defn lazy-load-var
  "Takes a symbol name of a var, requires the ns if not yet required, and returns the var."
  [var-sym]
  (let [ns (namespace var-sym)]
    (when-not ns
      (throw (ex-info "Can only load vars that are ns-qualified!" {})))
    ; BUG: Require is not thread safe. Suspect this leads to some problems when laxy-load-var is used
    ; from multiple threads. See https://ask.clojure.org/index.php/9893/require-is-not-thread-safe
    (require (symbol ns))
    (let [resolved (resolve var-sym)]
      (if resolved
        resolved
        (throw (ex-info "Unable to resolve var, is it valid?" {:var-sym var-sym}))))))

(def lazy-load-var-memo
  (memoize lazy-load-var))

(def ZeroInt
  (s/both s/Int (s/pred zero? 'zero?)))

(s/defschema PosNum
  "Positive number (float or int)"
  (s/both s/Num (s/pred pos? 'pos?)))

(s/defschema NonNegNum
  "Non-negative number (float or int)"
  (s/both s/Num (s/pred (comp not neg?) 'non-negative?)))

(def PosInt
  (s/both s/Int (s/pred pos? 'pos?)))

(def NonNegInt
  (s/both s/Int (s/pred (comp not neg?) 'non-negative?)))

(def PosDouble
  (s/both double (s/pred pos? 'pos?)))

(def UserName
  (s/both s/Str (s/pred #(re-matches #"\A[a-z][a-z0-9_-]{0,62}[a-z0-9]\z" %) 'lowercase-alphanum?)))

(def NonEmptyString
  (s/both s/Str (s/pred #(not (zero? (count %))) 'not-empty-string)))

(defn diff-map-keys
  "Return triple of keys from two maps: [only in left, only in right, in both]"
  [left right]
  (data/diff (set (keys left)) (set (keys right))))

(defn time-seq
  "Returns a sequence of date-time values growing over specific period.
   Takes as input the starting value and the growing value, returning a lazy infinite sequence."
  [start ^ReadablePeriod period]
  (iterate (fn [^DateTime t] (.plus t period)) start))

(defn deep-merge-with
  "Like merge-with, but merges maps recursively, applying the given fn
  only when there's a non-map at a particular level.
  (deep-merge-with + {:a {:b {:c 1 :d {:x 1 :y 2}} :e 3} :f 4}
               {:a {:b {:c 2 :d {:z 9} :z 3} :e 100}})
  -> {:a {:b {:z 3, :c 3, :d {:z 9, :x 1, :y 2}}, :e 103}, :f 4}"
  [f & maps]
  (apply
    (fn merge
      [& args]
      (try
        (if (every? map? args)
          (apply merge-with merge args)
          (apply f args))
        (catch Exception e
          (log/error e "Encountered exception while merging" {:args args})
          (throw e))))
    maps))

(defn set-atom!
  "Atomically set the atom to the new value, return the old val"
  [atom newval]
  (loop []
    (let [old-val @atom
          swap-happened (compare-and-set! atom old-val newval)]
      (if swap-happened old-val (recur)))))

(defn format-resource-map
  "Given a map with resource amount values,
   formats the amount values for logging"
  [resource-map]
  (pc/map-vals #(if (float? %)
                  (format "%.3f" %)
                  (str %))
               resource-map))

(defn format-map-for-structured-logging
  "Given a map with different types of values, formats the amount values as a number,
   and everything else as string for structured logging.
   Traverses nested maps to format appropriately."
  [resource-map]
  (pc/map-vals #(cond
                  ; keep numbers as numbers
                  (number? %) %
                  ; for nested maps, traverse the map and format its values
                  (map? %) (format-map-for-structured-logging %)
                  ; everything else is a string
                  :else (str %))
               resource-map))
