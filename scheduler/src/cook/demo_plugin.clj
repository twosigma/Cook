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
  (:require [clj-http.client :as http]
            [clj-time.core :as t]
            [cook.cache :as ccache]
            [cook.hook-definitions :as chd]))


;; Cook hooks is a plugin API that lets job submission and job launch be controlled via a
;; plugin. cook.demo-plugin is a demo plugin that queries a remote service to ask about the
;; submission and launch status of a job. We use it in integration tests.

(def http-timeout-millis 2000)
(def reqdict {:socket-timeout http-timeout-millis :conn-timeout http-timeout-millis
               :as :json-string-keys :content-type :json})

(defn- generate-result
       [result message]
       {:status result :message message :cache-expires-at (-> 1 t/seconds t/from-now)})

; Demo validation plugin, implements SchedulerHooks, pinging the status service on the two given URL's.
(defrecord DemoValidate [submit-url launch-url]
  chd/SchedulerHooks
  (chd/check-job-submission
    [this job-map]
    (let [{:keys [body] http-status :status :as response} (http/get submit-url reqdict)]

      (case http-status
        200 (let [status (get body "status")
                  message (or (get body "message") "No message sent.")]
              (case status
                "accepted" (generate-result :accepted message)
                "rejected" (generate-result :rejected message)
                (generate-result :rejected (str "Bad contents[1], illegal status message " body))))

        404 (generate-result :rejected  (str "Got 404 accessing " submit-url))
        :rejected  (str "Got nothing[1] " response))))
  (chd/check-job-launch
    [this job-map]
    (let [{:keys [body] http-status :status :as response} (http/get launch-url reqdict)]

      (case http-status
        200 (let [status (get body "status")
                  message (or (get body "message") "No message sent.")]
              (case status
                "accepted" (generate-result :accepted message)
                "deferred" (generate-result :deferred message)
                (generate-result :accepted (str "Bad contents[2], illegal status message " body))))

        404 (generate-result :rejected  (str "Got 404 accessing " launch-url))
        (generate-result :rejected (str "Got nothing[2] " response))))))

(defn factory
  "Factory method for these hooks to be used in config.edn"
  [{:keys [launch-url submit-url]}] (->DemoValidate submit-url launch-url))
