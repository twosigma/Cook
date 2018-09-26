(ns cook.check-image
  [clj-http.client :as http]
  [cook.hooks :as hooks]
  (:import (com.google.common.cache CacheLoader Cache LoadingCache)))


(defstate validation-uri "TODO:%imageName%")



(defstate status-map)
;; Stores a cache from a job UUID to a set of status maps, that can be one of the following:
;{:status :ok }
;{:status :error :message "Message" }
;{:status :later :message "Message" :expire-at <DateTime to try again>}

IDEA: We use expire-at on :status :later to automatically skip it until it is auto-removed later on. No additional effort needed on our part. :)


(defprotocol ScheduleHooks
  (default-job-submisison-response []
    "What to return in a job submission response if the check-job-submission does not return in time")
  (check-job-submission [this job-map deadline]
    "Check a job submission for correctness at the time of submission. Returns with one of two possibilities:
      {:status :ok :expire-at <DateTime>}
      {:status :error :message "<SOME MESSAGE>" :expire-at <DateTime>}

      This check is run synchronously with jobs submisison and MUST respond within 2 seconds, and should ideally return within
      100ms, or less. Furthermore, if multiple jobs are submitted in a batch (which may contain tens to hundreds to
      thousands of jobs), the whole batch of responeses MUST complete within 30 seconds. Note that this API is called on a
      best-effort basis, and not be called at all or may be called after hook-check-job-before-invocation.

      Deadline indicates when the response must return. If the current time is later than the deadline, the plugin
      MUST return a value immediately with no chance of blocking.")
  (check-job-invocation [job-maps]
    "Check a job submission for if we can run it now. Returns a map with one of three possibilities:
      {:status :ok :expire-at <DateTime>}
      {:status :error :message \"Message\" :expire-at <DateTime>}
      {:status :later :message \"Message\" :retry-at <DateTime to relaunch>}

      On a rerun (when the response was previously :later) this will be automatically re-invoked.

      This check is run just before a job is about to launch, and MUST return within milliseconds, without blocking
      (If you don't have have a definitive result, return a retry a few tens of milliseconds later)

      If the return value is :status :ok, the job is considered ready to launch right now.
      If the return value is :status :error, the job is considered bad and should be failed with the given message.

      If the return value is :status :later, the job execution should be deferred until the given datetime. Note that you cannot
      delay a job indefinitely, after a certain time (set via XXXX in the config.edn), a job will be killed automatically.

      In addition, certain rate limits apply for re-invocation."))


;; BROKEN: Unused.
'(defn get-with-expiration
  [^LoadingCache cache key]
  "Applies to a LoadingCache. expires elements that are older than expire-at."
  (let [current-state (.getIfPresent cache key)
        {:keys [status expire-at] current-state}
        now (t/now)]
    (when (< now expire-at)  (.remove state-map key))
    (.get state-map key)))

;; BROKEN: Unused.
'(def ^LoadingCache loading-cache-from-load-fn [load-fn]
  (-> (CacheBuilder/newBuilder)
      (.maximumSize 10000)
      (.expireAfterAccess 24 TimeUnit/HOURS)
      (.build
        (CacheLoader/from
          (reify [Function]
            (load [self key]
              (load-fn key)))))))



(def do-http-miss-check [job-map]
    (let [now (t/now)]
      ;; TODO: Should return :status OK :status :error :status, with expirations set appropriately depending on if it
      ;; is a positive or negative hit. Should timeout any request after 2 seconds or so, and if a timeout occurs,
      ;; set a very short :cache-expires-at, even as soon as t/now.
  ))

(def do-http-image-ready-check [job-map]
  (let [now (t/now)]
    ;; TODO: Does a HTTP request, returns :status :later with a try again. then submits an async http request.
    ;; When the response comes back from the request, it asynchronously updates the cache put-cache! with the result, postive or negative.

    ;; TODO: Should return :status :ok or :status :error or :status :later, with expirations set appropriately depending on if it
    ;; is a positive or negative hit. Should timeout any request after 2 seconds or so, and if a timeout occurs,
    ;; set a very short :cache-expires-at, even as soon as t/now.
    ))


(defonce image-available-cache [])

(load-docker-available)

