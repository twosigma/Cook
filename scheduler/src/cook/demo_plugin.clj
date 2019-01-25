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
(ns cook.demo-plugin
  (:require [clj-time.core :as t]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cook.hooks.definitions :as chd]))

(def uuid-seen-counts (atom {}))

(defn- generate-result
       [result message]
       {:status result :message message :cache-expires-at (-> 1 t/seconds t/from-now)})

; Demo validation plugin, implements SchedulerHooks, pinging the status service on the two given URL's.
(defrecord DemoValidateSubmission []
  chd/JobSubmissionValidator
  (chd/check-job-submission
    [this {:keys [name] :as job-map}]
    (if (and name (str/starts-with? name "submit_fail"))
      (generate-result :rejected "Message1")
      (generate-result :accepted "Message2"))))

(defrecord DemoFilterLaunch []
  chd/JobLaunchFilter
  (chd/check-job-launch
    [this {:keys [:job/name :job/uuid] :as job-map}]
    (let [newdict (swap! uuid-seen-counts update-in [uuid] (fnil inc 0))
          seen (get newdict uuid)]
      (if (and name
               (str/starts-with? name "launch_defer")
               (<= seen 3))
        (generate-result :deferred "Message3")
        (generate-result :accepted "Message4")))))

(defn launch-factory
  "Factory method for the launch-hook to be used in config.edn"
  [{:keys [launch-url]}] (->DemoFilterLaunch))

(defn submission-factory
  "Factory method for the submission hook to be used in config.edn"
  [{:keys [submit-url]}] (->DemoValidateSubmission))
