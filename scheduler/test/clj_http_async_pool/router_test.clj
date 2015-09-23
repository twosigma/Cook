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
(ns clj-http-async-pool.router-test
  (:require [clj-http.client] ; clj-http.fake depends on this being loaded, *sigh*
            [clj-http.fake :as http-fake]
            [clojure.core.async :as async]
            [clojure.test :refer :all]
            [clj-http-async-pool.core :refer :all]
            [clj-http-async-pool.client :as c]
            [clj-http-async-pool.router :as router]
            [clj-http-async-pool.test-utils :refer :all]))

(deftest ^:integration test-routing
  (testing "Separate hosts have separate threadpools"
    (let [req-time (multiply-duration 25)
          goog-req-mult 1
          aapl-req-mult 2
          msft-req-mult 3]
      (http-fake/with-fake-routes-in-isolation
        {"http://google.com/apps" (fn [req]
                                    (Thread/sleep (* goog-req-mult req-time))
                                    {:status 200 :headers {} :body "fake-apps"})
         "http://apple.com/iphone" (fn [req]
                                     (Thread/sleep (* aapl-req-mult req-time))
                                     {:status 200 :headers {} :body "fake-iphone"})
         "http://microsoft.com/" (fn [req]
                                   (Thread/sleep (* msft-req-mult req-time))
                                   {:status 200 :headers {} :body "fake-msft"})}
        (doall
         (for [threads (range 1 3)
               goog-requests (range 1 3)
               aapl-requests (range 1 5)
               msft-requests (range 0 2)
               router-base [(router/make-router (test-router-opts {:nthreads threads
                                                                   :hosts #{"google.com:80" "apple.com:80"}}))
                            (router/make-router (test-router-opts {:nthreads threads
                                                                   :hosts #{#"google\.com" #"apple\.com"}}))]]
           (with-lifecycle [router router-base]
             (let [{:keys [elapsed result]}
                   (timed
                    (let [urls (concat (repeatedly goog-requests (constantly "http://google.com/apps"))
                                       (repeatedly aapl-requests (constantly "http://apple.com/iphone"))
                                       (repeatedly msft-requests (constantly "http://microsoft.com/")))
                          responses (->> urls
                                         (map #(c/get router % {}))
                                         (async/merge)
                                         (async/into []))]
                      (async/<!! responses)))]
               (doseq [response result]
                 (is (contains? #{"fake-apps" "fake-iphone" "fake-msft"}
                                (:body response))))
               (is (= (quot elapsed req-time)
                      (max (* goog-req-mult
                              (+ (quot goog-requests threads)
                                 (min 1 (rem goog-requests threads))))
                           (* aapl-req-mult
                              (+ (quot aapl-requests threads)
                                 (min 1 (rem aapl-requests threads))))
                           (* msft-req-mult
                              (+ (quot msft-requests threads)
                                 (min 1 (rem msft-requests threads)))))))))))))))

(deftest ^:integration test-timeouts
  (testing "A slow server can't block client requests"
    (let [req-time (multiply-duration 200)
          timeout (multiply-duration 50)]
      (http-fake/with-fake-routes-in-isolation
        {"http://google.com/apps" (fn [req]
                                    (Thread/sleep req-time)
                                    {:status 200 :headers {} :body "fake-apps"})}
        (doall
         (for [threads (range 1 4)
               requests (range 1 20)]
           (with-lifecycle [router (router/make-router (test-router-opts {:nthreads threads
                                                                          :response-timeout-ms timeout
                                                                          :hosts #{"google.com:80"}}))]
             (let [{:keys [elapsed result]}
                   (timed
                    (let [response-chans (repeatedly requests #(c/get router "http://google.com/apps" {}))
                          responses (async/into [] (async/merge response-chans))]
                      (async/<!! responses)))]
               (doseq [response result]
                 (is (= :timeout response)))
               ;; twice the timeout was cutting it a little too close
               ;; for "lein test" on the shell, smells like startup
               ;; variance
               (is (> (* 3 timeout) elapsed))))))))))

(deftest ^:integration test-circuit-breaker-stats
  (testing "We can snoop on the circuit-breaker stats properly"
    (let [req-time (multiply-duration 200)
          timeout (multiply-duration 100)]
      (http-fake/with-fake-routes-in-isolation
        {"http://google.com/apps" (fn [req]
                                    (Thread/sleep req-time)
                                    {:status 200 :headers {} :body "fake-apps"})}
        (doall
         (for [threads (range 1 5)
               requests (range 1 10)]
           (with-lifecycle [router (router/make-router (test-router-opts {:nthreads threads
                                                                          :response-timeout-ms timeout
                                                                          :hosts #{"google.com:80"}}))]
             (let [circuit-breakers (:circuit-breakers router)
                   response-chans (repeatedly requests #(c/get router "http://google.com/apps" {}))
                   responses (async/into [] (async/merge response-chans))]
               (letfn [(grab-stats []
                         (-> (get circuit-breakers "google.com:80")
                             :stats-chan
                             async/<!!
                             (dissoc :mode)))]
                 (async/<!! (async/timeout (multiply-duration 25)))

                 (let [stats (grab-stats)]
                   (are [k val] (= val (get stats k))
                        :failed 0
                        :passed 0
                        :pending requests
                        :dropped 0))

                 (async/<!! (async/timeout (multiply-duration 150)))

                 (let [stats (grab-stats)]
                   (are [k val] (= val (get stats k))
                        :failed requests
                        :passed 0
                        :pending 0
                        :dropped 0)))

               (is (empty? (async/<!! responses)))))))))))
