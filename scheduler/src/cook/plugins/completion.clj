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
(ns cook.plugins.completion
  (:require [clojure.tools.logging :as log]
            [cook.config :as config]
            [cook.plugins.definitions :refer [InstanceCompletionHandler]]
            [cook.plugins.util]
            [mount.core :as mount]))

(def no-op
  (reify InstanceCompletionHandler
    (on-instance-completion [_ _ _])))

(defn create-default-plugin-object
  "Returns the configured InstanceCompletionHandler, or a no-op if none is defined."
  [config]
  (let [factory-fn (get-in config [:settings :plugins :instance-completion :factory-fn])]
    (if factory-fn
      (do
        (log/info "Creating instance completion plugin with" factory-fn)
        (if-let [resolved-fn (cook.plugins.util/resolve-symbol (symbol factory-fn))]
          (resolved-fn config)
          (throw (ex-info (str "Unable to resolve factory fn " factory-fn)))))
      no-op)))

(mount/defstate plugin
  :start (create-default-plugin-object config/config))
