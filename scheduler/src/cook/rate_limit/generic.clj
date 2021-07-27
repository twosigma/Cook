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
            [clojure.tools.logging :as log]
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
  (flush! [this key]
    "Flush all entries with this key, causing a rate limit to be reset. nil means flush all keys")
  (enforce? [this]
    "Are we enforcing this policy?"))

(defn current-time-in-millis
  []
  (clj-time.coerce/to-long (clj-time.core/now)))

(defn get-key
  "Guava cache key cannot be nil/null"
  [key]
  (or key "*NULL_TBF_KEY*"))

(defn make-token-bucket-filter
  "Given a token bucket parameters and size, create an empty tbf that has the target parameters."
  [tokens-replenished-per-minute bucket-size]
  {:pre [(> bucket-size 0)
         (> tokens-replenished-per-minute 0.0)]}
  (tbf/create-tbf (/ tokens-replenished-per-minute 60000.)
                  bucket-size
                  (current-time-in-millis)
                  bucket-size))

(defn config->token-bucket-filter
  "Given a configuration dictionary with a token bucket paremters, create an empty tbf that has those parameters."
  [{:keys [tokens-replenished-per-minute bucket-size]}]
  (make-token-bucket-filter tokens-replenished-per-minute bucket-size))

(defn make-tbf-fn->CacheLoader
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
  {:pre [(not (nil? key))
         (not (nil? cache))]}
  (.get cache key))

(defn earn-tokens!
  "Account for any earned tokens for the requested key."
  [{:keys [cache] :as this} key]
  (let [key (get-key key)]
    (locking cache
      (let [tbf (tbf/earn-tokens (get-token-bucket-filter this key) (current-time-in-millis))]
        (.put ^LoadingCache cache key tbf)))))

(defrecord TokenBucketFilterRateLimiter
  [config ^LoadingCache cache ^Boolean enforce?]
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

  (flush!
    [this key]
    (locking cache
      (if (nil? key)
        (.invalidateAll cache)
        (.invalidate cache key))))

  (enforce?
    [_]
    enforce?))

(defn ^RateLimiter make-generic-tbf-rate-limiter
  "Make a token bucket filter based rate limiter where make-tbf-fn is used to configure the default rate limit for a particular
  key, instead of having a global configuration for each key."
  [{:keys [expire-minutes enforce?] :as config} make-tbf-fn]
  {:pre [(> expire-minutes 0)
         (or (true? enforce?) (false? enforce?))]}

  (->TokenBucketFilterRateLimiter config
                                  (-> (CacheBuilder/newBuilder)
                                      (.expireAfterAccess expire-minutes (TimeUnit/MINUTES))
                                      (.build (make-tbf-fn->CacheLoader make-tbf-fn)))
                                  enforce?))

(defn ^RateLimiter make-tbf-rate-limiter
  "Make a token bucket filter rate limiter where all keys have the same rate limit, as given in the config."
  [{:keys [bucket-size tokens-replenished-per-minute expire-minutes enforce?] :as config}]
  {:pre [(> bucket-size 0)
         (> tokens-replenished-per-minute 0.0)
         (> expire-minutes 0)
         (or (true? enforce?) (false? enforce?))
         ; The bucket-expiration-minutes is used for GC'ing old buckets. It should be more than
         ; bucket-size/tokens-replenished-per-minute. Otherwise, we might expire non-full buckets and a user could get
         ; extra tokens via expiration. (New token-bucket-filter's are born with a full bucket).
         (> expire-minutes (/ bucket-size tokens-replenished-per-minute))]}
    (make-generic-tbf-rate-limiter config (fn [_] (config->token-bucket-filter config))))


(defrecord AllowAllRateLimiterSingleton
  []
  RateLimiter
  (spend! [_ _ _] 0)
  (time-until-out-of-debt-millis! [_ _] 0)
  (get-token-count! [_ _] 100000000)
  (flush! [_ _])
  (enforce? [_] false))


(def AllowAllRateLimiter
  "A noop rate limiter that doesn't put a limit on anything. Has {:enforce? false} as the policy key."
  (->AllowAllRateLimiterSingleton))