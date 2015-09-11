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
(ns cook-agent.retry
  "A module for retrying a preferably idempotent operation.  Supports
  multiple backoff and defeat policies."
  (:require [clj-time.core :as time]
            [clojure.tools.logging :as log]))

(defn exponential-seq
  "Return a lazy-seq sequence of
   '(scalar scalar*base scalar*base^2 scalar*base^3 ...)"
  [scalar base]
  (cons scalar (lazy-seq (exponential-seq (* scalar base) base))))


(defprotocol DefeatAdmitter
  "Decides when to give up retrying."
  (admit? [this retry-elapsed retry-count]
    "Returns true iff we should stop retrying, given that we've spent
    retry-elapsed total time on retry-count retries already."))

;;; Gives up on retries after stop-after-secs seconds, but never gives
;;; up before retrying at least once, in case the job to retry itself
;;; takes a long time.
(defrecord TimeoutAdmitter [stop-after-secs]
  DefeatAdmitter
  (admit? [this retry-elapsed retry-count]
    (and (> retry-count 1)
         (> (time/in-seconds retry-elapsed) stop-after-secs))))

(def default-elapsed-time-fraction
  "Fraction of the total job's running time during which we will
  attempt to retry the operation.  For example, if the total job takes
  30 minutes, an elapsed-retry-fraction of 1/3 means we'll spend up to
  an additional 10 minutes retrying the operation."
  (/ 1.0 3.0))

(def default-min-retry-time-minutes
  "Minimum amount of time we'll spend retrying finish commands, no
  matter how long we spent running the job."
  5)

(def default-max-retry-time-minutes
  "Maximum amount of time we'll spend retrying finish commands, no
  matter how long we spent running the job."
  (* 12 60))

(defn fractional-effort-admitter
  "A policy for spending at most a constant fraction of the total
  job's time on retries, with clamping at min and max amounts.

  The intuition here comes from finish commands for sims: If the sim
  itself took a long time to run, then it's worthwhile to spend
  a (somewhat less) long time retrying the finish commands, since
  failure at that point means we'd have to re-run the whole sim.
  However, if the sim was cheap to run, we don't want to waste that
  much time retrying the finish commands, because we're wasting
  resources while we're retrying.  Generally, we want the amount of
  time spent retrying finish commands to be proportional to the cost
  of re-running the job.

  The defaults here were arbitrarily chosen for finish commands.  For
  general retry usage, they may not be suitable."
  ([job-interval]
   (fractional-effort-admitter job-interval
                               default-elapsed-time-fraction
                               default-min-retry-time-minutes
                               default-max-retry-time-minutes))
  ([job-interval elapsed-time-fraction min-retry-time-minutes max-retry-time-minutes]
   (->TimeoutAdmitter (-> job-interval
                          time/in-minutes
                          (* elapsed-time-fraction)
                          (max min-retry-time-minutes)
                          (min max-retry-time-minutes)
                          (* 60)))))

(defn default-gen-delay-seconds
  "Yields a lazy seq of numbers of seconds to wait between job
  retries.

  Begins as an exponential sequence, then continues as a constant
  sequence.  Where we truncate is configurable.

  By default, produces an exponential sequence up to a 20-minute
  delay, then repeatedly gives 20 minute delays."
  ([] (default-gen-delay-seconds (* 20 60)))
  ([constant-val]
   (concat
    (->> (exponential-seq 1 2)
         (take-while (partial >= constant-val)))
    (repeat constant-val))))

(defn retry-loop
  "Attempts running the given op until it either succeeds (according
  to success-checker) or we decide to stop retrying.

  Returns a map containing either the value of the last call to op or
  the exception thrown, together with the number of retries and
  interval of time spent retrying.  Will call op with the retry number
  we're on.

  Example:

      (retry-loop
        {:success-checker zero?
         :admitter (fractional-effort-admitter (time/interval begin end))
         :delay-seq (default-gen-delay-seconds)}
        (fn [retry-count] ...))
  "
  [{:keys [success-checker admitter delay-seq] :as opts} op]
  {:pre [(every? second opts)]}
  (let [retries-begin (time/now)]
    (loop [retry-count 0
           retry-delay-seconds (first delay-seq)
           delay-seq' (rest delay-seq)]
      (let [result (try
                     (let [ret (op retry-count)]
                       {:return ret})
                     (catch Exception e
                       {:exception e}))]
        (if (or (some-> result :return success-checker)
                (admit? admitter
                        (time/interval retries-begin (-> retry-delay-seconds time/seconds time/from-now))
                        retry-count))
          (assoc result
                 :retry-count retry-count
                 :retry-interval (time/interval retries-begin (time/now)))
          (do
            (Thread/sleep (* 1000 retry-delay-seconds))
            (recur (inc retry-count) (first delay-seq') (rest delay-seq'))))))))
