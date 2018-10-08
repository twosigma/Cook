(ns cook.check-image
  (:require [cook.hooks :as hooks]
            [clj-http.client :as http]
            [clj-time.core :as t]
            [cook.cache :as ccache]
            [cook.hooks :as hooks]
            )
  (:import (com.google.common.cache CacheLoader Cache LoadingCache CacheBuilder))
  (:import (com.google.common.base Function)
           (java.util.concurrent TimeUnit ForkJoinPool ArrayBlockingQueue)))

(def image-validity-check-http-timeout-millis 2000)
(def image-deployment-check-http-timeout-millis 30000)

(def good-cache-timeout (t/minutes 240)) ; How long store a good image status
(def bad-cache-timeout (t/seconds 30)) ; How long ot store a 'image is bad' status.
(def unknown-cache-timeout (t/seconds 60)) ; Time to defer when we have a
(def odd-result-cache-timeout (t/seconds 30))


(defn failed-image-validity-check [docker-image]
  {:status :error
   :message (str "Image " docker-image " not found")
   :cache-expire-at (t/plus (t/now) bad-cache-timeout)})

(defn do-image-status-query-http
  "Is the image valid? Do the HTTP (with a 2 second timeout) and return the response."
  [url timeout]
  (let [reqdict {:socket-timeout timeout :conn-timeout timeout
                 :as :json-string-keys :content-type :json}]
    (http/get url reqdict)))

(defn process-response-for-image-validity-cache
  "Determine if an image is valid from the http response body.
    Process a response containing a json body with a map with two keys:
    'built' and 'deployed' and true/valse as values."
  [docker-image {:keys [status body]}]
  (let [now (t/now)]
    (cond
      (= 200 status) {:status :ok
                      :cache-expire-at (t/plus now good-cache-timeout) }
      (= 404 status) {:status :error
                      :message (str "Image " docker-image " not found")
                      :cache-expire-at (t/plus now bad-cache-timeout)}
      ; TODO: What to return on other outputs? XXXX thinks we should default fail.
      :else {:status :ok
             :cache-expire-at (t/plus now odd-result-cache-timeout) })))

(defn process-response-for-image-deployment-cache
  "Process a response containing a json body with a map with two keys:
    'built' and 'deployed' and true/valse as values."
  [docker-image {:keys [docker-image status body]}]
  (let [now (t/now)
        built? ("built" body)
        deployed? ("deployed" body)
        ready? (and built? deployed?)]
    (cond
      (and (= 200 status) ready?) {:status :ok
                                   :cache-expire-at (t/plus now good-cache-timeout) }
      (and (= 200 status) (not ready?)) {:status :later
                                         :cache-expire-at (t/plus now unknown-cache-timeout) }
      (= 404 status) {:status :error
                      :message (str "Image " docker-image " not found")
                      :cache-expire-at (t/plus now bad-cache-timeout)}
      ; TODO: What to return on other outputs? XXXX thinks we should default fail.
      :else {:status :ok
             :cache-expire-at (t/plus now odd-result-cache-timeout) })))

(defn generate-url-from-image
  [docker-image]
  "TODO")

(defn image-deployment-miss
  "Is the image deployed? Do the HTTP and return the response."
  [docker-image]
  (let [now (t/now)]
    (try
      (let [url (generate-url-from-image docker-image)
            response (do-image-status-query-http url image-deployment-check-http-timeout-millis)]
        (process-response-for-image-validity-cache docker-image response))
      (catch Exception e
        {:status :later
         :cache-expire-at (t/plus now odd-result-cache-timeout)}))))

(def ^LoadingCache image-deployment-cache (-> (CacheBuilder/newBuilder)
                                              (.maximumSize 200000)
                                              (.expireAfterAccess 72 TimeUnit/HOURS)
                                              (.build (CacheLoader/from
                                                        (reify Function
                                                          (apply [_ docker-image]
                                                            (image-deployment-miss docker-image)))))))

(defn image-validity-miss [docker-image]
  (try
    (let [now (t/now)
          url (generate-url-from-image docker-image)
          response (do-image-status-query-http url image-validity-check-http-timeout-millis)
          validity (process-response-for-image-validity-cache docker-image response)
          {:keys [status] :as deployment} (process-response-for-image-deployment-cache docker-image response)]
      ; Opportunistically put the status into the image-deployment-cache if its good.
      (when (= :ok status)
        (.put image-deployment-cache docker-image deployment))
      validity)
    (catch Exception e
      (failed-image-validity-check))))

(def ^LoadingCache image-validity-cache (-> (CacheBuilder/newBuilder)
                                            (.maximumSize 200000)
                                            (.expireAfterAccess 72 TimeUnit/HOURS)
                                            (.build (CacheLoader/from
                                                      (reify Function
                                                        (apply [_ docker-image]
                                                          (image-validity-miss docker-image)))))))


; Avoid the set of deferred tasks from diverging to infinity. Sole purpose of this is to avoid an infinite queue if the
; service is misbehaving.
(def' max-deferred-tasks 1000)

; Configured for at most 5 async requests.
(def ^ThreadPoolExecutor async-pool (ThreadPoolExecutor. 1 5 10 TimeUnit/MINUTES
                                                         (ArrayBlockingQueue. (+ queue-size 100))))

(defrecord Foo []
  hooks/ScheduleHooks
  (hooks/check-job-submission
    [this {:keys [docker-image] :as job-map}]
    (let [now (t/now)]
      ; Expire the deployment status if it should be expired.
      (ccache/expire-key! image-validity-cache identity docker-image)
      ; If we have a status return it, else, dispatch sync work to refresh image status.
      (ccache/lookup-cache! image-validity-cache identity image-validity-miss docker-image)))

  (hooks/check-job-invocation
    [this {:keys [docker-image] :as job-map}]
    (let [now (t/now)]
      ; Expire the deployment status if it should be expired.
      (ccache/expire-key! image-deployment-cache identity docker-image)
      ; If we have a status return it, else, dispatch async work to refresh image status.
      (if-let [result (.getIfPresent image-deployment-cache docker-image)]
        result
        ; Dispatch async work to update with to avoid a queue explosion. Its OK to drop this; we'll requeue next time the
        ; scheduler tries to schedule it.
        (when (< (.size (.getQueue async-pool)) max-deferred-tasks)
          (.submit async-pool
                   (reify Runnable
                     (run [_]
                       (ccache/lookup-cache! image-deployment-cache identity image-deployment-miss docker-image))))
          {:status :later :cache-expire-at (t/plus now unknown-cache-timeout)})))))
