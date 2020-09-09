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
  (:require [clj-time.coerce]
            [clj-time.core]
            [cook.rate-limit.token-bucket-filter :as tbf])
  (:import (com.google.common.cache LoadingCache CacheLoader CacheBuilder)
           (java.util.concurrent TimeUnit)))

(defprotocol RateLimiter
  (spend! [this key resources]
    "Request this number of resources. Does not block (or otherwise indicate we're in debt)")
  (get-token-count! [this key]
    "Get the number of tokens in the token bucket filter under this name. Also earns any tokens first.")
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

(defn ->tbf
  "Given a tocken bucket parameters and size, create an empty tbf that has the target parameters"
  [tokens-replenished-per-minute bucket-size]
    (tbf/create-tbf (/ tokens-replenished-per-minute 60000.)
                    bucket-size
                    (current-time-in-millis)
                    bucket-size))

(defn config->tbf
  "Given a configuration dictionary with a token bucket paremters, create an empty tbf that has those parameters."
  [{:keys [tokens-replenished-per-minute bucket-size]}]
  (->tbf tokens-replenished-per-minute bucket-size))

(defn make-tbf-fn-ignore-key
  "Return a function from a key to a token bucket filter. Ignores the key key, creating the same config for all tbf's."
  [config]
  (fn [key]
    (config->tbf config)))

(defn fn->CacheLoader
  "Given a function from a single argument, make it a CacheLoader"
  [make-tbf-fn]
  (proxy [CacheLoader] []
    (load [key]
      (make-tbf-fn key))))

(defn get-token-bucket-filter
  "Get or create a TBF for the requested key. Buckets start off full, so users whose first request is a lot of
  fast requests don't get throttled immediately. This doesn't let users get much above their quota ---
  The fact that we don't have a TBF says that we are either just starting up (not likely) or haven't
  gotten anything from them in a long time. (Assuming the TTL is set at least as high as
  (:bucket-size/:tokens-replenished-per-minute))."
  [{:keys [^LoadingCache cache] :as ratelimiter} key]
    (.get cache key))

(defn earn-tokens!
  "Account for any earned tokens for the requested key."
  [{:keys [cache] :as this} key]
  (let [key (get-key key)]
    (locking cache
      (let [tbf (tbf/earn-tokens (get-token-bucket-filter this key) (current-time-in-millis))]
        (.put cache key tbf)))))

(defrecord TokenBucketFilterRateLimiter
  [config cache ^Boolean enforce?]
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

  (get-token-count!
    [this key]
    (let [key (get-key key)]
      (earn-tokens! this key)
      (tbf/get-token-count (get-token-bucket-filter this key))))

  (enforce?
    [_]
    enforce?))

(defn ^RateLimiter make-generic-token-bucket-filter
  [{:keys [bucket-expire-minutes enforce?] :as config} make-tbf-fn]
  {:pre [(> bucket-expire-minutes 0)
         (or (true? enforce?) (false? enforce?))]}

  (->TokenBucketFilterRateLimiter config
                                  (-> (CacheBuilder/newBuilder)
                                      (.expireAfterAccess bucket-expire-minutes (TimeUnit/MINUTES))
                                      (.build (fn->CacheLoader make-tbf-fn)))
                                  enforce?))

(defn ^RateLimiter make-token-bucket-filter
  [^long bucket-size ^double tokens-replenished-per-minute ^long bucket-expire-minutes enforce?]
  {:pre [(> bucket-size 0)
         (> tokens-replenished-per-minute 0.0)
         (> bucket-expire-minutes 0)
         (or (true? enforce?) (false? enforce?))
         ; The bucket-expiration-minutes is used for GC'ing old buckets. It should be more than
         ; bucket-size/tokens-replenished-per-minute. Otherwise, we might expire non-full buckets and a user could get
         ; extra tokens via expiration. (New token-bucket-filter's are born with a full bucket).
         (> bucket-expire-minutes (/ bucket-size tokens-replenished-per-minute))]}
  (let [config {:bucket-size bucket-size
                :tokens-replenished-per-minute tokens-replenished-per-minute
                :bucket-expire-minutes bucket-expire-minutes :enforce? enforce?}]
    (make-generic-token-bucket-filter config (make-tbf-fn-ignore-key config))))


(def AllowAllRateLimiter
  "A noop rate limiter that doesn't put a limit on anything. Has {:enforce? false} as the policy key."
  (reify
    RateLimiter
    (spend! [_ _ _] 0)
    (time-until-out-of-debt-millis! [_ _] 0)
    (get-token-count! [_ _] 100000000)
    (enforce? [_] false)))
