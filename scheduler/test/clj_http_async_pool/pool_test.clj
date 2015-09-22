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
(ns clj-http-async-pool.pool-test
  (:require [clj-http.client] ; clj-http.fake depends on this being loaded, *sigh*
            [clj-http.fake :as http-fake]
            [clojure.core.async :as async]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clj-http-async-pool.client :as c]
            [clj-http-async-pool.pool :as pool]
            [clj-http-async-pool.test-utils :refer :all]))

(deftest test-batching
  (testing "When there are more concurrent requests than threads, we batch appropriately."
    (let [req-time (multiply-duration 25)]
      (http-fake/with-fake-routes-in-isolation
        {"http://google.com/apps" (fn [req]
                                    (Thread/sleep req-time)
                                    {:status 200 :headers {} :body "fake-apps"})}
        (doall
         (for [priming [true false]     ; need to run through the test
                                        ; once without checking timing
                                        ; in order to reduce variance.
                                        ; probably jit nonsense.
               threads (range 1 6)
               requests (range 1 11)]
           (with-lifecycle [pool (pool/make-connection-pool (test-pool-opts {:nthreads threads}))]
             (let [{:keys [elapsed result]}
                   (timed
                    (let [response-chans (map (fn [_] (c/get pool "http://google.com/apps" {}))
                                              (range requests))
                          responses (async/into [] (async/merge response-chans))]
                      (async/<!! responses)))]
               (when-not priming
                 (doseq [response result]
                   (is (= (:body response) "fake-apps")))
                 (is (= (quot elapsed req-time)
                        (+ (quot requests threads)
                           (min 1 (rem requests threads))))))))))))))
