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
  (:require [clojure.tools.logging :as log]
            [cook.config :refer [config]]
            [mount.core :as mount]
            [cook.rate-limit.generic :as rt]))

(defn create-job-submission-rate-limiter
  "From the configuration map, extract the keys that setup the job-submission rate limiter and return
  the constructed object. If the configuration map is not found, the AllowAllRateLimiter is returned."
  [config]
  (let [{:keys [settings]} config
        {:keys [rate-limit]} settings
        {:keys [expire-minutes job-submission]} rate-limit]
    (if (seq job-submission)
      (let [{:keys [bucket-size enforce? tokens-replenished-per-minute]} job-submission]
        (rt/make-token-bucket-filter bucket-size tokens-replenished-per-minute expire-minutes enforce?))
      rt/AllowAllRateLimiter)))

(mount/defstate job-submission-rate-limiter
  :start (create-job-submission-rate-limiter config))
