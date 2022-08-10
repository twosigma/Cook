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

(ns cook.plugins.job-submission-modifier
  (:require [clojure.tools.logging :as log]
            [cook.config :as config]
            [cook.plugins.definitions :refer [modify-job JobSubmissionModifier]]
            [cook.plugins.util]
            [mount.core :as mount]))

(defrecord IdentityJobSubmissionModifier []
  JobSubmissionModifier
  ;The IdentityJobSubmissionModifier doesn't make any changes to what users submit
  (modify-job [this job]
    job))

(defn create-plugin-object
  "Returns the configured JobSubmissionModifier, or a IdentityJobSubmissionModifier if none is defined."
  [config]
  (let [factory-fn (get-in config [:settings :plugins :job-submission-modifier :factory-fn])]
    (if factory-fn
      (do
        (log/info "Creating job submission modifier plugin with" factory-fn)
        (if-let [resolved-fn (cook.plugins.util/resolve-symbol (symbol factory-fn))]
          (resolved-fn config)
          (throw (ex-info (str "Unable to resolve factory fn " factory-fn) {}))))
      (IdentityJobSubmissionModifier.))))

(mount/defstate plugin
                :start (create-plugin-object config/config))

(defn apply-job-submission-modifier-plugins
  "Modify a user-submitted job before passing it further down the submission pipeline."
  [raw-job]
  (modify-job plugin raw-job))