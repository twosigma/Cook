(ns cook.cache
  (:require [clj-time.core :as t])
  (:import (com.google.common.cache Cache)))


(defn get-if-present
  "Generic cache. Caches under a key (extracted from the item with extract-key-fn. Uses miss-fn to fill
  any misses. Caches only positive hits where both functions return non-nil"
  [^Cache cache extract-key-fn item]
  (if-let [key (extract-key-fn item)]
    (.getIfPresent cache key)))


(defn put-cache!
  "Generic cache. Caches under a key (extracted from the item with extract-key-fn. Uses miss-fn to fill
  any misses. Caches only positive hits where both functions return non-nil"
  [^Cache cache extract-key-fn item new-result]
  (if-let [key (extract-key-fn item)]
    (do
      ; Only cache non-nil
      (when new-result
        (.put cache key new-result))
      new-result)))

(defn lookup-cache!
  "Generic cache. Caches under a key (extracted from the item with extract-key-fn. Uses miss-fn to fill
  any misses. Caches only positive hits where both functions return non-nil"
  [^Cache cache extract-key-fn miss-fn item]
  (if-let [key (extract-key-fn item)]
    (if-let [result (.getIfPresent cache key)]
      result ; we got a hit.
      (let [new-result (miss-fn item)]
        ; Only cache non-nil
        (when new-result
          (.put cache key new-result))
        new-result))
    (miss-fn item)))

(defn expire-key!
  "Generic cache. Caches under a key (extracted from the item with extract-key-fn. Uses miss-fn to fill
  any misses. Caches only positive hits where both functions return non-nil. Also handles expiration if the value
  is a map with the key :cache-expires-at."
  [^Cache cache extract-key-fn item]
  (if-let [key (extract-key-fn item)]
    (locking cache ; TOOD: Consider lock striping based on hashcode of the key to allow concurrent loads.
      ; If it has a timed expiration, expire it.
      (when-let [result (.getIfPresent cache key)]
        (let [{:keys [cache-expires-at]} result]
          (when (and cache-expires-at (t/after? (t/now) cache-expires-at))  (.invalidate cache key)))))))


(defn lookup-cache-with-expiration!
  "Generic cache. Caches under a key (extracted from the item with extract-key-fn. Uses miss-fn to fill
  any misses. Caches only positive hits where both functions return non-nil. Also handles expiration if the value
  is a map with the key :cache-expires-at."
  [^Cache cache extract-key-fn miss-fn item]
  (if-let [key (extract-key-fn item)]
    (locking cache ; TOOD: Consider lock striping based on hashcode of the key to allow concurrent loads.
      ; If it has a timed expiration, expire it.
      (when-let [result (.getIfPresent cache key)]
        (let [{:keys [cache-expires-at]} result]
          (when (and cache-expires-at (t/after? (t/now) cache-expires-at))  (.invalidate cache key))))
      ; And then fetch it.
      (if-let [result (.getIfPresent cache key)]
        result ; we got a hit.
        (let [new-result (miss-fn item)]
          ; Only cache non-nil
          (when new-result
            (.put cache key new-result))
          new-result)))
    (miss-fn item)))
