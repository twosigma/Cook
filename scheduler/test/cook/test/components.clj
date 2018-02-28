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
(ns cook.test.components
  (:require [clojure.test :refer :all]
            [cook.components :as components]))

(deftest test-health-check-middleware
  (let [handler (fn [_] "Called handler!")
        debug-request {:uri "/debug"
                       :request-method :get}]

    (testing "always returns 200 if leader-reports-unhealthy is false"
      (let [leadership-atom (atom false)
            middleware (components/health-check-middleware handler
                                                           leadership-atom
                                                           false)]
        (is (= 200 (:status (middleware debug-request))))
        (swap! leadership-atom (constantly true))
        (is (= 200 (:status (middleware debug-request))))))

    (testing "returns 503 when leader"
      (let [leadership-atom (atom false)
            middleware (components/health-check-middleware handler
                                                           leadership-atom
                                                           true)]
        (is (= 200 (:status (middleware debug-request))))
        (swap! leadership-atom (constantly true))
        (is (= 503 (:status (middleware debug-request))))))

    (testing "passes other requests to handler"
      (let [leadership-atom (atom false)
            middleware (components/health-check-middleware handler
                                                           leadership-atom
                                                           true)]
        (is (= "Called handler!" (middleware {:uri "/real-request"})))
        (swap! leadership-atom (constantly true))
        (is (= "Called handler!" (middleware {:uri "/real-request"})))))))

