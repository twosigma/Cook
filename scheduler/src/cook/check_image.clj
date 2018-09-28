(ns cook.check-image
  (:require [cook.hooks :as hooks]
            [clj-http.client :as http]
            [clj-time.core :as t]
            [cook.cache :as ccache]
            [cook.hooks :as hooks]
            )
  (:import (com.google.common.cache CacheLoader Cache LoadingCache CacheBuilder))
  (:import (com.google.common.base Function)
           (java.util.concurrent TimeUnit)))


(def hit-timeout (t/minutes 30)) ; How long store a good image status
(def miss-timeout (t/seconds 30)) ; How long ot store a 'image is bad' status.
(def load-timeout (t/seconds 60)) ; Time to handle a load
(def odd-result-timeout (t/seconds 30))

(defn do-image-valid-http
  "Is the image valid? Do the HTTP (with a 2 second timeout) and return the response."
  [imagename]
  ; TODO
  200)

(defn do-image-deployed-http
  "Is the image deployed? Do the HTTP (with a 30 second timeout) and return the response."
  [imagename]
  ; TODO
  200)

(defn image-validity-miss [image-name]
  (let [now (t/now)
        http-response (do-image-valid-http image-name)]
    (cond
      (= 200 http-response) {:status :ok
                             :cache-expire-at (t/plus now hit-timeout) }
      (= 404 http-response) {:status :error
                             :message (str "Image " image-name " not found")
                             :cache-expire-at (t/plus now miss-timeout)}
      :else {:status :ok
             :cache-expire-at (t/plus now odd-result-timeout) })))

(def ^LoadingCache image-validity-cache (-> (CacheBuilder/newBuilder)
                                            (.maximumSize 200000)
                                            (.expireAfterAccess 48 TimeUnit/HOURS)
                                            (.build (CacheLoader/from
                                                      (reify Function
                                                        (apply [_ image-name]
                                                          (image-validity-miss image-name)))))))



(defn is-image-available-everywhere-we-want
  [http-response]
  true ; TODO
  )


(defn image-deployment-status-miss [image-name]
  (let [now (t/now)
        http-response (do-image-deployed-http image-name)
        is-deployed-everywhere (is-image-available-everywhere-we-want http-response)
        ]
    (cond
      is-deployed-everywhere {:status :ok
                              :cache-expire-at (t/plus now hit-timeout) }
      (= 404 http-response) {:status :error
                             :message (str "Image " image-name " not found")
                             :cache-expire-at (t/plus now miss-timeout)}
      :else {:status :ok
             :cache-expire-at (t/plus now odd-result-timeout) })))


(def ^Cache image-deployment-status-cache (-> (CacheBuilder/newBuilder)
                                                     (.maximumSize 200000)
                                                     (.expireAfterAccess 48 TimeUnit/HOURS)
                                                     (.build)))



(def extract-image-from-job-map
  [job-map])

(defrecord Foo []
  hooks/ScheduleHooks
  (hooks/check-job-submission
    [this job-map]
    (let [now (t/now)
          image-name (extract-image-from-job-map job-map)]
      ; Expire the deployment status if it should be expired.
      (ccache/expire-key! image-validity-cache identity image-name)
      ; If we have a status return it, else, dispatch async work to refresh image status.
      (ccache/lookup-cache! image-validity-cache identity image-validity-miss image-name)))

  (hooks/check-job-invocation
    [this job-map]
    (let [now (t/now)
          image-name (extract-image-from-job-map job-map)]
      ; Expire the deployment status if it should be expired.
      (ccache/expire-key! image-deployment-status-cache identity image-name)
      ; If we have a status return it, else, dispatch async work to refresh image status.
      (if-let [result (.getIfPresent image-deployment-status-cache image-name)]
        result
        (do
          ; TODO: async dispatch request to load status
          {:status :later :cache-expire-at (t/plus now load-timeout)})))))

(def do-image-check)
(CacheLoader/from
  (reify Function
    (apply [_ image-name]
      (image-deployment-status-miss image-name))))

