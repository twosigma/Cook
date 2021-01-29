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
(ns cook.plugins.demo-plugin
  (:require [clj-time.core :as t]
            [clojure.string :as str]
            [cook.plugins.definitions :as chd]))

(def uuid-seen-counts (atom {}))

(defn- generate-result
  [result message]
  {:status result :message message :cache-expires-at (-> 1 t/seconds t/from-now)})

; Demo validation plugin, designed to match with the integration tests.
(defrecord DemoValidateSubmission []
  chd/JobSubmissionValidator
  (chd/check-job-submission
    [this {:keys [name] :as job-map} _]
    (if (and name (str/starts-with? name "plugin_test.submit_fail"))
      (generate-result :rejected "Message1- Fail to submit")
      (generate-result :accepted "Message2"))))

(defrecord DemoValidateSubmission2 []
  chd/JobSubmissionValidator
  (chd/check-job-submission [this {:keys [name]} _]
    (if (and name (str/starts-with? name "plugin_test.submit_fail2"))
      (generate-result :rejected "Message5- Plugin2 failed")
      (generate-result :accepted "Message6"))))

(defrecord DemoFilterLaunch []
  chd/JobLaunchFilter
  (chd/check-job-launch
    [this {:keys [:job/name :job/uuid] :as job-map}]
    (let [newdict (swap! uuid-seen-counts update-in [uuid] (fnil inc 0))
          seen (get newdict uuid)]
      (if (and name
               (str/starts-with? name "plugin_test.launch_defer")
               (<= seen 3))
        (generate-result :deferred "Message3")
        (generate-result :accepted "Message4")))))

(defn launch-factory
  "Factory method for the launch-plugin to be used in config.edn"
  []
  (->DemoFilterLaunch))

(defn submission-factory
  "Factory method for the submission plugin to be used in config.edn"
  []
  (->DemoValidateSubmission))

(defn submission-factory2
  "Factory method for the second submission plugin to be used in config.edn"
  []
  (->DemoValidateSubmission2))
