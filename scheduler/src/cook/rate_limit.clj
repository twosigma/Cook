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
(ns cook.rate-limit
  (:require [cook.config :refer [config]]
            [cook.rate-limit.generic :as rtg]
            [cook.compute-cluster :as cc]
            [cook.regexp-tools :as regexp-tools]
            [mount.core :as mount]))

; Import from cook.rate-limit.generic some relevant functions.
(def spend! rtg/spend!)
(def time-until-out-of-debt-millis! rtg/time-until-out-of-debt-millis!)
(def get-token-count! rtg/get-token-count!)
(def enforce? rtg/enforce?)
(def AllowAllRateLimiter rtg/AllowAllRateLimiter)

(defn initialize-rate-limit-based-on-key
  "Method to help tocken-bucket-filter pick a default rate limit based a regexp match through a series of patterns.

  Given a match-list of [{:<field> <regexp> :tbf-config {:tokens-replenished-per-minute ...}}] and a key, return a
  token bucket filter"
  [regexp-name {match-list :matches}]
  (fn [key]
    (if-let [tbf-config-dict (regexp-tools/match-based-on-regexp regexp-name :tbf-config match-list key)]
      (rtg/config->token-bucket-filter tbf-config-dict)
      (throw (ex-info "Unable to match in matchlist." {:key key :match-list match-list})))))

(defn create-job-submission-rate-limiter
  "From the configuration map, extract the keys that setup the job-submission rate limiter and return
  the constructed object. If the configuration map is not found, the AllowAllRateLimiter is returned."
  [config]
  (let [{:keys [settings]} config
        {:keys [rate-limit]} settings
        {:keys [expire-minutes job-submission]} rate-limit]
    (if (seq job-submission)
      (rtg/make-tbf-rate-limiter (assoc job-submission :expire-minutes expire-minutes))
      AllowAllRateLimiter)))

(mount/defstate job-submission-rate-limiter
  :start (create-job-submission-rate-limiter config))

(defn create-job-launch-rate-limiter
  "From the configuration map, extract the keys that setup the job-launch rate limiter and return
  the constructed object. If the configuration map is not found, the AllowAllRateLimiter is returned."
  [config]
  (let [{:keys [settings]} config
        {:keys [rate-limit]} settings
        {:keys [expire-minutes job-launch]} rate-limit]
    (if (seq job-launch)
      (rtg/make-tbf-rate-limiter (assoc job-launch :expire-minutes expire-minutes))
      AllowAllRateLimiter)))

(mount/defstate job-launch-rate-limiter
  :start (create-job-launch-rate-limiter config))

(defn create-global-job-launch-rate-limiter
  "From the configuration map, extract the keys that setup the job-launch rate limiter and return
  the constructed object. If the configuration map is not found, the AllowAllRateLimiter is returned."
  [config]
  (let [ratelimit-config (some-> config :settings :compute-cluster-launch-rate-limit)]
    (if (seq ratelimit-config)
      (rtg/make-generic-tbf-rate-limiter
        ratelimit-config
        (initialize-rate-limit-based-on-key :compute-cluster-regex ratelimit-config))
      AllowAllRateLimiter)))

(mount/defstate global-job-launch-rate-limiter
  :start (create-global-job-launch-rate-limiter config))
