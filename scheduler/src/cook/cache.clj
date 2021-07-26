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
      (locking cache ; TODO: Consider lock striping based on hashcode of the key to allow concurrent loads.
        (let [new-result (miss-fn item)]
          ; Only cache non-nil
          (when new-result
            (.put cache key new-result))
          new-result)))
    (miss-fn item)))

(defn- expire-key-helper
  "Helper function for expiring a key explicitly."
  [^Cache cache key]
  (when-let [result (.getIfPresent cache key)]
    (let [{:keys [cache-expires-at]} result]
      (when (and cache-expires-at (t/after? (t/now) cache-expires-at))  (.invalidate cache key)))))

(defn expire-key!
  "Generic cache. Caches under a key (extracted from the item with extract-key-fn). Uses miss-fn to fill
  any misses. Caches only positive hits where both functions return non-nil. Also handles expiration if the value
  is a map with the key :cache-expires-at."
  [^Cache cache extract-key-fn item]
  (if-let [key (extract-key-fn item)]
    (locking cache ; TODO: Consider lock striping based on hashcode of the key to allow concurrent loads.
      ; If it has a timed expiration, expire it.
      (expire-key-helper cache key))))

(defn lookup-cache-with-expiration!
  "Generic cache. Caches under a key (extracted from the item with extract-key-fn. Uses miss-fn to fill
  any misses. Caches only positive hits where both functions return non-nil. Also handles expiration if the value
  is a map with the key :cache-expires-at."
  [^Cache cache extract-key-fn miss-fn item]
  (if-let [key (extract-key-fn item)]
    (locking cache ; TODO: Consider lock striping based on hashcode of the key to allow concurrent loads.
      ; If it has a timed expiration, expire it.
      (expire-key-helper cache key)
      ; And then fetch it.
      (if-let [result (.getIfPresent cache key)]
        result ; we got a hit.
        (let [new-result (miss-fn item)]
          ; Only cache non-nil
          (when new-result
            (.put cache key new-result))
          new-result)))
    (miss-fn item)))
