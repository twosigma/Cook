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
(ns cook.rate-limit.token-bucket-filter)

(defn create-tbf
  "Make a token bucket filter (tbf), with a given refill rate (units per timestep), a maximum token capacity, and give it the
  current timestamp.

  This token bucket filter is a bit unusual, instead of earning tokens that you can later spend, we allow the current
  tokens to be negative. Thus, you can (in one request) spend an unbounded number of tokens. However, you will be
  in debt until your token balance is at least 0. This design allows the tokens in a single request to exceed the bucket-size
  in the tbf. (If we didn't do it this way, bucket-size is an implicit maximum limit on request size.) This way
  we can independently pick the burst size in the tbf and the maximum request size.

  This library does not handle blocking or other operations. Typical workflows are to:

  ** Admission control **
  This is good when you don't know how big a request will be until afterwards.

  Use in-debt? as part of request admission control, rejecting a request if it returns true. When a request completes (or is aborted),
  take the tokens that it used, affecting the next request.

  ** Delaying requests **
  If you want to delay requests instead of rejecting. Take the tokens the request needs, pause for the returned pause time.
  (E.g., schedule the message to be sent at that particular pause time.
  "
  ([^double token-rate ^long bucket-size ^long current-time ^long current-tokens]
   {:pre [(> token-rate 0)
          (>= bucket-size 0)]}
   {:token-rate token-rate :bucket-size bucket-size :last-update current-time :current-tokens current-tokens})
  ([^double token-rate ^long bucket-size ^long current-time]
   (create-tbf token-rate bucket-size current-time 0)))

(defn- update-tbf
  "Add tokens (if there are any). If less than a whole token added, keep the current state. Returns the current state.
  Enforces bucket-size limit. Allows tokens to be removed or added. If no timestamp supplied, just alters the token count."
  [{:keys [current-tokens bucket-size] :as tbf} ^long tokens-to-dispense ^long current-time]
  (if (zero? tokens-to-dispense)
    ; Only dispense when we have at least a whole token to add on.
    tbf
    (-> tbf
        (assoc :last-update current-time)
        (update :current-tokens #(-> % (+ tokens-to-dispense) (min bucket-size))))))

(defn earn-tokens
  "Earn tokens. Helper function, takes the current timestamp and adds in any newly earned tokens. It is numerically
  robust to the timestamp being out of order."
  [{:keys [last-update token-rate] :as tbf} ^long current-time]
  (let [current-time (max current-time last-update)]
    (cond-> tbf
      (not (= current-time last-update))
      (update-tbf
        (-> current-time
            (- last-update)
            (* token-rate)
            long)
        current-time))))

(defn time-until
  "Assumes tokens have been dispensed. Reports how many time units until the balance is positive and a request can be granted."
  [{:keys [current-tokens token-rate] :as tbf}]
  (-> current-tokens
      -
      (/ token-rate)
      Math/ceil
      long
      (max 0)))

(defn spend-tokens
  "Returns a new TBF. Takes the tokens from the result; the new TBF reflects the tokens being earned and taken.
  This does not block and will always succeed, even if the balance is negative."
  [tbf ^long current-time ^long wanted-tokens]
  {:pre [(>= wanted-tokens 0)]}
  (let [{:keys [last-update] :as tbf} (earn-tokens tbf current-time)
        tbf (update-tbf tbf (- wanted-tokens) last-update)]
    tbf))

(defn in-debt?
  "Reports if the tbf is currently in debt."
  [{:keys [current-tokens]}]
  (< current-tokens 0))

(defn get-token-count
  "Reports the number of tokens in the tbf."
  [{:keys [current-tokens]}]
  current-tokens)
