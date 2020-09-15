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
            [mount.core :as mount]))

; Import from cook.rate-limit.generic some relevant functions.
(def spend! rtg/spend!)
(def time-until-out-of-debt-millis! rtg/time-until-out-of-debt-millis!)
(def get-token-count! rtg/get-token-count!)
(def enforce? rtg/enforce?)
(def AllowAllRateLimiter rtg/AllowAllRateLimiter)

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
  (let [{:keys [settings]} config
        {:keys [rate-limit]} settings
        {:keys [expire-minutes global-job-launch]} rate-limit]
    (if (seq global-job-launch)
      (rtg/make-tbf-rate-limiter (assoc global-job-launch :expire-minutes expire-minutes))
      AllowAllRateLimiter)))

(mount/defstate global-job-launch-rate-limiter
  :start (create-global-job-launch-rate-limiter config))

(def global-job-launch-rate-limiter-key "*DEF*")