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
(ns cook.rate-limit.generic
  (:require [clj-time.core]
            [clj-time.coerce]
            [cook.rate-limit.token-bucket-filter :as tbf])
  (:import (com.google.common.cache CacheBuilder Cache)
           (java.util.concurrent TimeUnit)))

(defprotocol RateLimiter
  (spend! [this key resources]
    "Request this number of resources. Does not block (or otherwise indicate we're in debt)")
  (time-until-out-of-debt-millis! [this key]
    "Time, in milliseconds, until this rate limiter is out of debt for this key. Earns resources first.")
  (enforce? [this]
    "Are we enforcing this policy?"))

(defn current-time-in-millis
  []
  (clj-time.coerce/to-long (clj-time.core/now)))

(defn get-key
  "Guava cache key cannot be nil/null"
  [key]
  (or key "*NULL_TBF_KEY*"))

(defn get-token-bucket-filter
  "Get or create a TBF for the requested key. Buckets start off full, so users whose first request is a lot of
  fast requests don't get throttled immediately. This doesn't let users get much above their quota ---
  The fact that we don't have a TBF says that we are either just starting up (not likely) or haven't
  gotten anything from them in a long time. (Assuming the TTL is set at least as high as
  (:bucket-size/:tokens-replenished-per-minute))."
  [limiter key]
  (let [{:keys [tokens-replenished-per-minute max-tokens]} limiter
        supplier (proxy [Callable] []
                   (call []
                     (tbf/create-tbf (/ tokens-replenished-per-minute 60000.)
                                     max-tokens
                                     (current-time-in-millis)
                                     max-tokens)))]
    (.get (:cache limiter) key supplier)))

(defn earn-tokens!
  "Account for any earned tokens for the requested key."
  [{:keys [cache] :as this} key]
  (let [key (get-key key)]
    (locking cache
      (let [tbf (tbf/earn-tokens (get-token-bucket-filter this key) (current-time-in-millis))]
        (.put cache key tbf)))))

(defrecord TokenBucketFilterRateLimiter
  [^long max-tokens ^double tokens-replenished-per-minute cache ^Boolean enforce?]
  RateLimiter
  (spend!
    [this key tokens]
    (let [key (get-key key)]
      (locking cache
        (let [tbf (tbf/spend-tokens (get-token-bucket-filter this key) (current-time-in-millis) tokens)]
          (.put cache key tbf)
          nil))))

  (time-until-out-of-debt-millis!
    [this key]
    (let [key (get-key key)]
      (earn-tokens! this key)
      (tbf/time-until (get-token-bucket-filter this key))))

  (enforce?
    [_]
    enforce?))

(defn ^RateLimiter make-token-bucket-filter
  [^long max-tokens ^double tokens-replenished-per-minute ^long bucket-expire-minutes enforce?]
  {:pre [(> max-tokens 0)
         (> tokens-replenished-per-minute 0)
         (> bucket-expire-minutes 0)
         (or (true? enforce?) (false? enforce?))
         ; The bucket-expiration-minutes is used for GC'ing old buckets. It should be more than
         ; max-tokens/tokens-replenished-per-minute. Otherwise, we might expire non-full buckets and a user could get
         ; extra tokens via expiration. (New token-bucket-filter's are born with a full bucket).
         (> bucket-expire-minutes (/ max-tokens tokens-replenished-per-minute))]}
  (->TokenBucketFilterRateLimiter max-tokens tokens-replenished-per-minute
                     (-> (CacheBuilder/newBuilder)
                         (.expireAfterAccess bucket-expire-minutes (TimeUnit/MINUTES))
                         (.build))
                     enforce?))


(def AllowAllRateLimiter
  "A noop rate limiter that doesn't put a limit on anything. Has {:enforce? false} as the policy key."
  (reify
    RateLimiter
    (spend! [_ _ _] 0)
    (time-until-out-of-debt-millis! [_ _] 0)
    (enforce? [_] false)))
