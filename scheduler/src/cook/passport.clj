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
(ns cook.passport
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [cook.config :as config]))

(defn log-event
  "Log event to cook-passport log file"
  [{:keys [event-type] :as log-data}]
  (when (:enabled? (config/passport))
    (log/log config/passport-logger-ns :info nil (json/write-str
                                                   (assoc
                                                     log-data
                                                     :source :cook-scheduler
                                                     :event-type (str "cook-scheduler/" (name event-type)))))))

(def checkpoint-volume-mounts-key-selected :checkpoint-volume-mounts-key-selected)
(def default-image-selected :default-image-selected)
(def init-container-image-selected :init-container-image-selected)
(def job-created :job-created)
(def job-submitted :job-submitted)
(def pod-completed :pod-completed)
(def pod-submission-succeeded :pod-submission-succeeded)
(def sidecar-image-selected :sidecar-image-selected)
(def synthetic-pod-submission-succeeded :synthetic-pod-submission-succeeded)
(def pod-submission-failed :pod-submission-failed)
(def synthetic-pod-submission-failed :synthetic-pod-submission-failed)
