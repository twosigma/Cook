(ns cook.caches
  (:require [chime]
            [cook.cache :as ccache]
            [cook.config :as config]
            [mount.core :as mount])
  (:import (com.google.common.cache Cache CacheBuilder)
           (java.util.concurrent TimeUnit)))

(defn new-cache [config]
  "Build a new cache"
  (-> (CacheBuilder/newBuilder)
      (.maximumSize (get-in config [:settings :cache-working-set-size]))
      ;; if its not been accessed in 2 hours, whatever is going on, its not being visted by the
      ;; scheduler loop anymore. E.g., its probably failed/done and won't be needed. So,
      ;; lets kick it out to keep cache small.
      (.expireAfterAccess 2 TimeUnit/HOURS)
      (.build)))

(defn passport-cache [config]
  "Build a new passport-related cache"
  (-> (CacheBuilder/newBuilder)
    (.maximumSize (get-in config [:settings :passport :job-cache-set-size]))
    (.expireAfterAccess (get-in config [:settings :passport :job-cache-expiry-time-hours]) TimeUnit/HOURS)
    (.build)))

(defn lookup-cache-datomic-entity!
  "Specialized function for caching where datomic entities are the key.
  Extracts :db/id so that we don't keep the entity alive in the cache."
  [cache miss-fn entity]
  (ccache/lookup-cache! cache :db/id miss-fn entity))

(mount/defstate ^Cache job-ent->resources-cache :start (new-cache config/config))
(mount/defstate ^Cache job-ent->pool-cache :start (new-cache config/config))
(mount/defstate ^Cache task-ent->user-cache :start (new-cache config/config))
(mount/defstate ^Cache job-ent->user-cache :start (new-cache config/config))
(mount/defstate ^Cache task->feature-vector-cache :start (new-cache config/config))
(mount/defstate ^Cache user->group-ids-cache :start (new-cache config/config))
(mount/defstate ^Cache recent-synthetic-pod-job-uuids :start
  (-> (CacheBuilder/newBuilder)
      (.maximumSize (:synthetic-pod-recency-size (config/kubernetes)))
      ; We blocklist a given job from being autoscaled soon after a prior autoscaling.
      (.expireAfterWrite (:synthetic-pod-recency-seconds (config/kubernetes)) TimeUnit/SECONDS)
      (.build)))
(mount/defstate ^Cache pool-name->exists?-cache :start (new-cache config/config))
(mount/defstate ^Cache pool-name->accepts-submissions?-cache :start (new-cache config/config))
(mount/defstate ^Cache pool-name->db-id-cache :start (new-cache config/config))
(mount/defstate ^Cache user-and-pool-name->quota :start (new-cache config/config))
(mount/defstate ^Cache instance-uuid->job-uuid :start (passport-cache config/config))
(mount/defstate ^Cache job-uuid->job-map :start (passport-cache config/config))