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
(ns clj-http-async-pool.circuit-breaker-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clj-http-async-pool.circuit-breaker :as cb]
            [clj-http-async-pool.test-utils :refer :all]))

(defn- service
  []
  (let [request-chan (async/chan)]
    (async/go-loop []
      (when-let [{:keys [fail delay-ms val response-chan]} (async/<! request-chan)]
        (when-not fail
          (async/go
            (when delay-ms
              (async/<! (async/timeout delay-ms)))
            (async/>! response-chan val)
            (async/close! response-chan)))
        (recur)))
    request-chan))

(defn- double-<!!
  "When we check stats-chan, we may need to \"flush out\" some stale
  state, so we pull out of stats-chan twice."
  [chan]
  (async/<!! chan)
  (async/<!! chan))

(deftest responsive-test
  (testing "behavior when server is responsive"
    (let [breaker (cb/make-circuit-breaker (test-circuit-breaker-opts {:response-timeout-ms (multiply-duration 25)
                                                                       :lifetime-ms (multiply-duration 10)
                                                                       :failure-threshold 0.95})
                                           (service))
          service (:request-chan breaker)
          stats-chan (:stats-chan breaker)
          failure-logger (:failure-logger breaker)]
      (letfn [(dotest [trials concurrent-requests]
                (dotimes [i trials]
                  (let [response-chans (async/merge
                                        (repeatedly concurrent-requests
                                                    (fn []
                                                      (let [response-chan (async/chan)]
                                                        (is (true? (async/>!! service {:val i :response-chan response-chan})))
                                                        response-chan))))
                        responses (async/<!! (async/into [] response-chans))]
                    (is (= concurrent-requests (count responses)))
                    (doseq [response responses]
                      (is (= i response)))
                    (is (= :closed (:mode (double-<!! stats-chan)))))))]
        (testing "one request at a time"
          (dotest 1000 1))
        (testing "10 requests at a time"
          (dotest 100 10))
        (testing "1000 requests at a time"
          (dotest 2 1000)))
      (async/close! service))))

(deftest unresponsive-test
  (testing "behavior when server never works right"
    (let [breaker (cb/make-circuit-breaker (test-circuit-breaker-opts {:response-timeout-ms (multiply-duration 5)
                                                                       :failure-threshold 0
                                                                       :reset-timeout-ms (multiply-duration 100)})
                                           (service))
          service (:request-chan breaker)
          stats-chan (:stats-chan breaker)]
      (letfn [(dotest [trials concurrent-requests]
                (dotimes [i trials]
                  (let [response-chans (async/merge
                                        (repeatedly concurrent-requests
                                                    (fn []
                                                      (let [response-chan (async/chan)]
                                                        (is (true? (async/>!! service {:fail true :response-chan response-chan})))
                                                        response-chan))))
                        responses (async/<!! (async/into [] response-chans))]
                    (is (= 0 (count responses))))))]
        (testing "one request at a time"
          (dotest 1000 1))
        (testing "10 requests at a time"
          (dotest 100 10))
        (testing "1000 requests at a time"
          (dotest 2 1000)))
      (async/close! service))))

(deftest recovery-test
  (testing "circuit breaker recovers after reset-timeout-ms"
    (let [reset-timeout-ms (multiply-duration 200)
          recovery-time-ms (+ reset-timeout-ms (/ reset-timeout-ms 10))
          breaker (cb/make-circuit-breaker (test-circuit-breaker-opts {:response-timeout-ms (multiply-duration 5)
                                                                       :lifetime-ms (multiply-duration 10)
                                                                       :failure-threshold 0
                                                                       :reset-timeout-ms reset-timeout-ms})
                                           (service))
          service (:request-chan breaker)
          stats-chan (:stats-chan breaker)]
      (letfn [(dotest [trials concurrent-requests]
                (dotimes [i trials]
                  (let [response-chans (async/merge
                                        (repeatedly concurrent-requests
                                                    (fn []
                                                      (let [response-chan (async/chan)]
                                                        (is (true? (async/>!! service {:fail true :response-chan response-chan})))
                                                        response-chan))))
                        responses (async/<!! (async/into [] response-chans))]
                    (is (= 0 (count responses)))))
                (is (= :open (:mode (double-<!! stats-chan)))
                    "many failures break the circuit")
                (async/<!! (async/timeout recovery-time-ms))
                (is (= :half-open (:mode (double-<!! stats-chan)))
                    "waiting allows the circuit to return to half-open")
                (let [response-chan (async/chan)]
                  (is (true? (async/>!! service {:fail true :response-chan response-chan})))
                  (is (nil? (async/<!! response-chan))))
                (is (= :open (:mode (double-<!! stats-chan)))
                    "when half-open, failing once breaks the circuit again")
                (async/<!! (async/timeout recovery-time-ms))
                (is (= :half-open (:mode (double-<!! stats-chan)))
                    "waiting allows the circuit to return to half-open")
                (let [response-chan (async/chan)]
                  (is (true? (async/>!! service {:val 1 :response-chan response-chan})))
                  (is (= 1 (async/<!! response-chan))))
                (is (= :closed (:mode (double-<!! stats-chan)))
                    "when half-open, passing once closes the circuit"))]
        (testing "20 requests at a time"
          (dotest 100 20))
        (testing "200 requests at a time"
          (dotest 10 200))
        (testing "500 requests at a time"
          (dotest 2 500)))
      (async/close! service))))

(deftest timeout-test
  (testing "timeout is accurate"
    (let [response-timeout-ms (multiply-duration 50)
          pass-delay (/ response-timeout-ms 2)
          fail-delay (+ response-timeout-ms
                        (/ response-timeout-ms 2))
          concurrent-requests 10]
      (dotimes [i 10]
        (testing "responding faster than the timeout counts as passing"
          (let [breaker (cb/make-circuit-breaker (test-circuit-breaker-opts {:response-timeout-ms response-timeout-ms
                                                                             :failure-threshold 0
                                                                             :reset-timeout-ms (multiply-duration 200)})
                                                 (service))
                service (:request-chan breaker)
                stats-chan (:stats-chan breaker)
                response-chans (async/merge
                                (repeatedly concurrent-requests
                                            (fn []
                                              (let [response-chan (async/chan)]
                                                (is (true? (async/>!! service {:val i :delay-ms pass-delay
                                                                               :response-chan response-chan})))
                                                response-chan))))
                responses (async/<!! (async/into [] response-chans))]
            (is (= concurrent-requests (count responses)))
            (async/close! service)))
        (testing "responding slower than the timeout counts as failing"
          (let [breaker (cb/make-circuit-breaker (test-circuit-breaker-opts {:response-timeout-ms response-timeout-ms
                                                                             :failure-threshold 0
                                                                             :reset-timeout-ms (multiply-duration 200)})
                                                 (service))
                service (:request-chan breaker)
                stats-chan (:stats-chan breaker)
                response-chans (async/merge
                                (repeatedly concurrent-requests
                                            (fn []
                                              (let [response-chan (async/chan)]
                                                (is (true? (async/>!! service {:val i :delay-ms fail-delay
                                                                               :response-chan response-chan})))
                                                response-chan))))
                responses (async/<!! (async/into [] response-chans))]
            (is (= 0 (count responses)))
            (async/close! service)))
        (testing "mixing concurrent passes and fails does the right thing"
          (let [breaker (cb/make-circuit-breaker (test-circuit-breaker-opts {:response-timeout-ms response-timeout-ms
                                                                             :failure-threshold 0
                                                                             :reset-timeout-ms (multiply-duration 200)})
                                                 (service))
                service (:request-chan breaker)
                stats-chan (:stats-chan breaker)
                passes (/ concurrent-requests 2)
                fails (- concurrent-requests passes)
                response-chans (async/merge
                                (concat
                                 (repeatedly passes
                                             (fn []
                                               (let [response-chan (async/chan)]
                                                 (is (true? (async/>!! service {:val i :delay-ms pass-delay
                                                                                :response-chan response-chan})))
                                                 response-chan)))
                                 (repeatedly fails
                                             (fn []
                                               (let [response-chan (async/chan)]
                                                 (is (true? (async/>!! service {:val i :delay-ms fail-delay
                                                                                :response-chan response-chan})))
                                                 response-chan)))))
                responses (async/<!! (async/into [] response-chans))]
            (is (= passes (count responses)))
            (async/close! service)))))))

(deftest threshold-test
  (testing "we honor the failure threshold properly"
    (doall
     (for [passes (range 15 25)
           fails (range 1 15)
           :let [failure-threshold (- 1 (* 2 (/ fails (+ passes fails))))]]
       (let [breaker (cb/make-circuit-breaker (test-circuit-breaker-opts {:response-timeout-ms (multiply-duration 5)
                                                                          :lifetime-ms (multiply-duration 50)
                                                                          :failure-threshold failure-threshold
                                                                          :reset-timeout-ms (multiply-duration 200)})
                                              (service))
             service (:request-chan breaker)
             stats-chan (:stats-chan breaker)]
         (dotimes [i passes]
           (let [response-chan (async/chan)]
             (is (true? (async/>!! service {:val i :response-chan response-chan})))
             (is (= i (async/<!! response-chan)))
             (is (= :closed (:mode (double-<!! stats-chan)))
                 "circuit stays closed when we've done nothing but pass")))
         (dotimes [_ (dec fails)]
           (let [response-chan (async/chan)]
             (is (true? (async/>!! service {:fail true :response-chan response-chan})))
             (is (nil? (async/<!! response-chan)))))
         (dotimes [_ 3]
           (let [response-chan (async/chan)]
             (is (true? (async/>!! service {:fail true :response-chan response-chan})))
             (is (nil? (async/<!! response-chan)))))
         (let [full-state (double-<!! stats-chan)]
           (is (= :open (:mode full-state))
               (str "we break the circuit after going past the threshold; passes = " passes " fails = " fails " threshold = " failure-threshold " state = " full-state)))
         (async/close! service))))))

(deftest behavior-change-test
  (testing "when the service goes down, we detect it quickly, even if we have a long positive history"
    (doall
     (for [passes (range 50 200 25)
           :let [fails-without-noticing 5
                 additional-fails 10]]
       (let [delay-ms (multiply-duration 5)
             lifetime-ms (multiply-duration 1000)
             response-timeout-ms (multiply-duration 25)
             ;; TODO: figure out why these constants tend to work, and
             ;; make something more robust.
             failure-threshold 0.5
             breaker (cb/make-circuit-breaker (test-circuit-breaker-opts {:response-timeout-ms response-timeout-ms
                                                                          :lifetime-ms lifetime-ms
                                                                          :failure-threshold failure-threshold})
                                              (service))
             service (:request-chan breaker)
             stats-chan (:stats-chan breaker)]
         (testing (str "passes " passes " fails-without-noticing " fails-without-noticing " failure-threshold " failure-threshold)
           (dotimes [i passes]
             (let [response-chan (async/chan)]
               (is (true? (async/>!! service {:val i :delay-ms delay-ms :response-chan response-chan})))
               (is (= i (async/<!! response-chan)))
               (is (= :closed (:mode (double-<!! stats-chan)))
                   "circuit stays closed when we've done nothing but pass")))
           (dotimes [i fails-without-noticing]
             (let [response-chan (async/chan)]
               (is (true? (async/>!! service {:fail true :response-chan response-chan})))
               (is (nil? (async/<!! response-chan)))
               (is (= :closed (:mode (double-<!! stats-chan)))
                   (str "noticed failure on try " i))))
           (dotimes [_ additional-fails]
             (let [response-chan (async/chan)]
               (is (true? (async/>!! service {:fail true :response-chan response-chan})))
               (is (nil? (async/<!! response-chan)))))
           (is (= :open (:mode (double-<!! stats-chan)))
               "we break the circuit after many failures suddenly occur")))))))
