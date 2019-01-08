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
(ns cook.check-image
  (:require [cook.hooks :as hooks]
            [clj-http.client :as http]
            [clj-time.core :as t]
            [cook.cache :as ccache]
            [cook.hooks-definitions :refer [SchedulerHooks]])
  (:import (com.google.common.cache CacheLoader Cache LoadingCache CacheBuilder))
  (:import (com.google.common.base Function)
           (java.util.concurrent TimeUnit)))

(def image-validity-check-http-timeout-millis 2000)
(def image-deployment-check-http-timeout-millis 30000)
(def bad-cache-timeout (t/seconds 30)) ; How long ot store a 'image is bad' status.
(def unknown-cache-timeout (t/seconds 60)) ; Time to defer when we have a
(def odd-result-cache-timeout (t/seconds 30))

(defn failed-image-validity-check [docker-image timeout]
  {:status :rejected
   :message (str "Problem with docker image '" docker-image "'")
   :cache-expires-at (t/plus (t/now) timeout)})

(defn do-image-status-query-http
  "Is the image valid? Do the HTTP (with a 2 second timeout) and return the response."
  [url timeout]
  (let [reqdict {:socket-timeout timeout :conn-timeout timeout
                 :as :json-string-keys :content-type :json}]
    (http/get url reqdict)))


(defn calculate-expiration
  "Given a body dictionary from a response with an 'expires-timestamp' key, extract the key in the
  form of a seconds-after-1970 value."
  [body]
  (when-let [expire-timestamp (get body "expires-timestamp")]
    (->> expire-timestamp t/seconds (t/plus (t/epoch)))))

(defn generate-url-from-image
  [docker-image]
  "TODO")

(defn process-response-for-image-validity-cache
  "Determine if an image is valid from the http response body.
    Process a response containing a json body with a map with two keys:
    'built' and 'deployed' and true/valse as values."
  [docker-image {:keys [body] http-status :status :as response}]
  (case http-status
    200 {:status :accepted
         :cache-expires-at (calculate-expiration body)}
    404 (failed-image-validity-check docker-image bad-cache-timeout)
    ; Weird outputs: Default fail.
    (failed-image-validity-check docker-image odd-result-cache-timeout)))

(defn process-response-for-image-deployment-cache
  "Process a response containing a json body with a map with two keys:
    'built' and 'deployed' and true/valse as values."
  [docker-image {:keys [status body]}]
  (let [now (t/now)
        built? ("built" body)
        deployed? ("deployed" body)
        ready? (and built? deployed?)]
    (cond
      (and (= 200 status) ready?) {:status :accepted
                                   :cache-expires-at (calculate-expiration body)}
      (and (= 200 status) (not ready?)) {:status :deferred
                                         :cache-expires-at (t/plus now unknown-cache-timeout)}
      ; This can't happen, but if it does, lets flush the job out by executing it.
      (= 404 status) {:status :accepted
                      :cache-expires-at (t/plus now bad-cache-timeout)}
      ; Try again later.
      :else {:status :deferred
             :cache-expires-at (t/plus now odd-result-cache-timeout)})))

(defn image-deployment-miss
  "Is the image deployed? Miss handler if we don't have an entry in the cache."
  [docker-image]
  (let [now (t/now)]
    (try
      (let [url (generate-url-from-image docker-image)
            response (do-image-status-query-http url image-deployment-check-http-timeout-millis)]
        (process-response-for-image-validity-cache docker-image response))
      (catch Exception e
        {:status :deferred
         :cache-expires-at (t/plus now odd-result-cache-timeout)}))))

(def ^LoadingCache image-deployment-cache (-> (CacheBuilder/newBuilder)
                                              (.maximumSize 200000)
                                              (.expireAfterAccess 72 TimeUnit/HOURS)
                                              (.build (CacheLoader/from
                                                        (reify Function
                                                          (apply [_ docker-image]
                                                            (image-deployment-miss docker-image)))))))

(defn image-validity-miss [docker-image]
  (try
    (let [url (generate-url-from-image docker-image)
          response (do-image-status-query-http url image-validity-check-http-timeout-millis)
          validity (process-response-for-image-validity-cache docker-image response)
          {:keys [status] :as deployment} (process-response-for-image-deployment-cache docker-image response)]
      ; Opportunistically put the status into the image-deployment-cache if its good.
      (when (= :ok status)
        (.put image-deployment-cache docker-image deployment))
      validity)
    (catch Exception e
      (failed-image-validity-check docker-image odd-result-cache-timeout))))

(def ^LoadingCache image-validity-cache
  (-> (CacheBuilder/newBuilder)
      (.maximumSize 200000)
      (.expireAfterAccess 72 TimeUnit/HOURS)
      (.build (CacheLoader/from
                (reify Function
                  (apply [_ docker-image]
                    (image-validity-miss docker-image)))))))

(defrecord DockerValidate []
  hooks/ScheduleHooks
  (hooks/check-job-submission
    [this {:keys [docker-image] :as job-map}]
    (let [now (t/now)]
      ; If we have a status return it, else, dispatch sync work to refresh image status.
      (ccache/lookup-cache-with-expiration! image-validity-cache identity image-validity-miss docker-image)))

  (hooks/check-job-invocation
    [this {:keys [docker-image] :as job-map}]
    (let [now (t/now)]
      ; Expire the deployment status if it should be expired.
      (ccache/expire-key! image-deployment-cache identity docker-image)
      ; If we have a status return it, else, dispatch async work to refresh image status.
      (if-let [result (ccache/get-if-present image-deployment-cache identity docker-image)]
        result
        (do
          (future
            (ccache/lookup-cache! image-deployment-cache identity image-deployment-miss docker-image))
          {:status :later :cache-expires-at (t/plus now unknown-cache-timeout)})))))
