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
(ns cook.test.cache
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [cook.cache :as ccache])
  (:import (com.google.common.cache Cache CacheBuilder)
           (java.util.concurrent TimeUnit)))

(defn new-cache []
  "Build a new cache"
  (-> (CacheBuilder/newBuilder)
      (.maximumSize 100)
      (.expireAfterAccess 2 TimeUnit/HOURS)
      (.build)))

(deftest test-cache
  (let [^Cache cache (new-cache)
        extract-fn #(if (odd? %) nil %)
        miss-fn #(if (> % 100) nil %)]
    ;; u1 has 2 jobs running
    (is (= 1 (ccache/lookup-cache! cache extract-fn miss-fn 1))) ; Should not be cached. Nil from extractor.
    (is (= 2 (ccache/lookup-cache! cache extract-fn miss-fn 2))) ; Should be cached.
    (is (= nil (.getIfPresent cache 1)))
    (is (= 2 (.getIfPresent cache 2)))
    (is (= nil (ccache/lookup-cache! cache extract-fn miss-fn 101))) ; Should not be cached. Nil from miss function
    (is (= nil (ccache/lookup-cache! cache extract-fn miss-fn 102))) ; Should not be cached. Nil from miss function
    (is (= nil (.getIfPresent cache 101)))
    (is (= nil (.getIfPresent cache 102)))
    (is (= 4 (ccache/lookup-cache! cache extract-fn miss-fn 4))) ; Should be cached.
    (is (= 2 (.getIfPresent cache 2)))
    (is (= 4 (.getIfPresent cache 4)))))

(deftest test-cache-expire-explicit
  (let [^Cache cache (new-cache)
        epoch (t/epoch)
        extract-fn identity
        make-fn (fn [key offset]
                  {:val (* key 2) :cache-expires-at (->> offset t/millis (t/plus epoch))})
        miss-fn (fn [key] (make-fn key (-> key (* 1000))))
        miss-fn2 (fn [key] (make-fn key (-> key (* 1000) (+ 10000))))]
    ;; Should expire at 1000, 2000, and 3000
    (with-redefs [t/now (fn [] epoch)]
      (ccache/lookup-cache-with-expiration! cache identity miss-fn 1)
      (ccache/lookup-cache-with-expiration! cache identity miss-fn 2)
      (ccache/lookup-cache-with-expiration! cache identity miss-fn 3))

    ;; None of these should be expired.
    (with-redefs [t/now (fn [] (t/plus epoch (t/millis 999)))]
      (is (= (make-fn 1 1000) (ccache/lookup-cache-with-expiration! cache identity miss-fn 1)))
      (is (= (make-fn 2 2000)  (ccache/lookup-cache-with-expiration! cache identity miss-fn 2)))
      (is (= (make-fn 3 3000)  (ccache/lookup-cache-with-expiration! cache identity miss-fn 3))))

    ;; This should not expire
    (with-redefs [t/now (fn [] (t/plus epoch (t/millis 999)))]
      (ccache/expire-key! cache identity 1)
      (ccache/expire-key! cache identity 2)
      (ccache/expire-key! cache identity 3)

      (is (= (make-fn 1 1000) (.getIfPresent cache 1)))
      (is (= (make-fn 2 2000) (.getIfPresent cache 2)))
      (is (= (make-fn 3 3000) (.getIfPresent cache 3))))

    ;; This should expire
    (with-redefs [t/now (fn [] (t/plus epoch (t/millis 1001)))]
      (ccache/expire-key! cache identity 1)
      (ccache/expire-key! cache identity 2)
      (ccache/expire-key! cache identity 3)

      (is (= nil (.getIfPresent cache 1)))
      (is (= (make-fn 2 2000) (.getIfPresent cache 2)))
      (is (= (make-fn 3 3000) (.getIfPresent cache 3))))

    ;; This should expire
    (with-redefs [t/now (fn [] (t/plus epoch (t/millis 2001)))]
      (ccache/expire-key! cache identity 1)
      (ccache/expire-key! cache identity 2)
      (ccache/expire-key! cache identity 3)

      (is (= nil (.getIfPresent cache 1)))
      (is (= nil (.getIfPresent cache 2)))
      (is (= (make-fn 3 3000) (.getIfPresent cache 3))))))

(deftest test-cache-expiration
  (let [^Cache cache (new-cache)
        epoch (t/epoch)
        make-fn (fn [key offset]
                  {:val (* key 2) :cache-expires-at (->> offset t/millis (t/plus epoch))})
        miss-fn (fn [key] (make-fn key (-> key (* 1000))))
        miss-fn2 (fn [key] (make-fn key (-> key (* 1000) (+ 10000))))]
    ;; Should expire at 1000, 2000, and 3000
    (with-redefs [t/now (fn [] epoch)]
      (ccache/lookup-cache-with-expiration! cache identity miss-fn 1)
      (ccache/lookup-cache-with-expiration! cache identity miss-fn 2)
      (ccache/lookup-cache-with-expiration! cache identity miss-fn 3))

    ;; None of these should be expired.
    (with-redefs [t/now (fn [] (t/plus epoch (t/millis 999)))]
      (is (= (make-fn 1 1000) (ccache/lookup-cache-with-expiration! cache identity miss-fn2 1)))
      (is (= (make-fn 2 2000) (ccache/lookup-cache-with-expiration! cache identity miss-fn2 2)))
      (is (= (make-fn 3 3000) (ccache/lookup-cache-with-expiration! cache identity miss-fn2 3))))

    ;; This should expire and be replaced
    (with-redefs [t/now (fn [] (t/plus epoch (t/millis 1001)))]
      (is (= (make-fn 1 11000) (ccache/lookup-cache-with-expiration! cache identity miss-fn2 1)))
      (is (= (make-fn 2 2000) (ccache/lookup-cache-with-expiration! cache identity miss-fn2 2)))
      (is (= (make-fn 3 3000) (ccache/lookup-cache-with-expiration! cache identity miss-fn2 3))))

    ;; This should expire and be replaced
    (with-redefs [t/now (fn [] (t/plus epoch (t/millis 2001)))]
      (is (= (make-fn 1 11000) (ccache/lookup-cache-with-expiration! cache identity miss-fn2 1)))
      (is (= (make-fn 2 12000) (ccache/lookup-cache-with-expiration! cache identity miss-fn2 2)))
      (is (= (make-fn 3 3000) (ccache/lookup-cache-with-expiration! cache identity miss-fn2 3))))))


(deftest test-cache-not-expiring
  (let [^Cache cache (new-cache)
        epoch (t/epoch)
        make-fn (fn [key _]
                  {:val (* key 2)})
        miss-fn (fn [key] {:val (* key 2)})]
    ;; These do not have the :cache-expires-at and should not expire.
    (with-redefs [t/now (fn [] epoch)]
      (ccache/lookup-cache-with-expiration! cache identity miss-fn 1)
      (ccache/lookup-cache-with-expiration! cache identity miss-fn 2)
      (ccache/lookup-cache-with-expiration! cache identity miss-fn 3))

    ;; None of these should be expired.
    (with-redefs [t/now (fn [] (t/plus epoch (t/millis 999)))]
      (is (= (make-fn 1 1000) (ccache/lookup-cache-with-expiration! cache identity miss-fn 1)))
      (is (= (make-fn 2 2000) (ccache/lookup-cache-with-expiration! cache identity miss-fn 2)))
      (is (= (make-fn 3 3000) (ccache/lookup-cache-with-expiration! cache identity miss-fn 3))))

    ;; None of these should be expired.
    (with-redefs [t/now (fn [] (t/plus epoch (t/millis 2000001)))]
      (is (= (make-fn 1 11000) (ccache/lookup-cache-with-expiration! cache identity miss-fn 1)))
      (is (= (make-fn 2 12000) (ccache/lookup-cache-with-expiration! cache identity miss-fn 2)))
      (is (= (make-fn 3 3000) (ccache/lookup-cache-with-expiration! cache identity miss-fn 3))))))
