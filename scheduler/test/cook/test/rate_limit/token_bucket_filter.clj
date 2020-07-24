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
(ns cook.test.rate-limit.token-bucket-filter
  (:require [clojure.test :refer :all]
            [cook.rate-limit.token-bucket-filter :as tbf]))

(defn approx=
  "Approximately equal"
  ([^Double a ^Double b ^Double err]
   (or (= a b) (< (/ (Math/abs (- b a)) (+ (Math/abs a) (Math/abs b))) err)))
  ([^Double a ^Double b]
   (approx= a b 0.01)))

(deftest approx
  (is (approx= 0 0))
  (is (approx= 1 1))
  (is (approx= -1 -1))
  (is (approx= 1.001 1.002))
  (is (approx= 1000.001 1000.002))
  (is (approx= -1.001 -1.002))
  (is (approx= -1000.001 -1000.002))
  (is (not (approx= 1.1 1.2)))
  (is (not (approx= 1.001 -1.002)))
  (is (not (approx= -1.001 1.002))))

(deftest test-earn-tokens
  (let [tbf (tbf/create-tbf 10.0 100 0)] ; 10/time unit, 100 max total, and the current time is 0
    (testing "Initial state not in debt"
      (let [tbf (tbf/earn-tokens tbf 0)]
        (is (not (tbf/in-debt? tbf)))))
    (testing "We earn tokens with time."
      (let [tbf (tbf/earn-tokens tbf 5)]
        (is (approx= (:current-tokens tbf) 50.0))))
    (testing "We earn the max number of tokens"
      (let [tbf (tbf/earn-tokens tbf 20)]
        (is (approx= (:current-tokens tbf) 100.0))))))

(deftest test-negative-tokens.
  (let [tbf (tbf/create-tbf 10.0 100 0)
        tbf (tbf/spend-tokens tbf 0 130)] ; 10/time unit, 100 max total, and the current time is 0, and we have -130 tokens.
    (testing "In debt as we earn tokens"
      (let [tbf (tbf/earn-tokens tbf 0)]
        (is (tbf/in-debt? tbf))
        (let [tbf (tbf/earn-tokens tbf 12)]
          (is (= 12 (:last-update tbf)))
          (is (tbf/in-debt? tbf)))
        (let [tbf (tbf/earn-tokens tbf 14)]
          (is (= 14 (:last-update tbf)))
          (is (not (tbf/in-debt? tbf))))))
    (testing "In debt as we incrementally earn tokens"
      (let [tbf0 (tbf/earn-tokens tbf 0)
            tbf1 (tbf/earn-tokens tbf0 5)
            tbf2 (tbf/earn-tokens tbf1 10)
            tbf3 (tbf/earn-tokens tbf2 12)
            tbf4 (tbf/earn-tokens tbf3 14)
            tbf5 (tbf/earn-tokens tbf4 15)]
        (is (= 15 (:last-update tbf5)))
        (is (tbf/in-debt? tbf0))
        (is (tbf/in-debt? tbf1))
        (is (tbf/in-debt? tbf2))
        (is (tbf/in-debt? tbf3))
        (is (not (tbf/in-debt? tbf4)))
        (is (not (tbf/in-debt? tbf5)))))
    (testing "In debt as we incrementally earn tokens"
      (let [tbf0 (tbf/earn-tokens tbf 0)
            tbf1 (tbf/earn-tokens tbf 5)
            tbf2 (tbf/earn-tokens tbf1 15)
            tbf3 (tbf/earn-tokens tbf2 12) ; Timestamps are out of order, so we don't actually earn here.
            tbf4 (tbf/earn-tokens tbf3 10)
            tbf5 (tbf/earn-tokens tbf4 20)
            tbf6 (tbf/earn-tokens tbf5 30)]

        (is (= 5 (:last-update tbf1)))
        (is (= 15 (:last-update tbf2)))
        (is (= 15 (:last-update tbf3)))
        (is (= 15 (:last-update tbf4)))
        (is (= 20 (:last-update tbf5)))

        (is (approx= (:current-tokens tbf1) -80.0))
        (is (approx= (:current-tokens tbf2) 20.0))
        (is (approx= (:current-tokens tbf3) 20.0))
        (is (approx= (:current-tokens tbf4) 20.0))
        (is (approx= (:current-tokens tbf5) 70.0))
        (is (approx= (:current-tokens tbf6) 100.0))

        (is (tbf/in-debt? tbf0))
        (is (tbf/in-debt? tbf1))
        (is (not (tbf/in-debt? tbf2)))
        (is (not (tbf/in-debt? tbf3)))
        (is (not (tbf/in-debt? tbf4)))
        (is (not (tbf/in-debt? tbf5)))))))

(deftest test-time-until
  (let [tbf (tbf/create-tbf 10.0 100 0)
        tbf (tbf/spend-tokens tbf 0 130)] ; 10/time unit, 100 max total, and the current time is 0, and we have -130 tokens.

    (let [tbf1 (tbf/earn-tokens tbf 0)
          tbf2 (tbf/earn-tokens tbf1 10)
          tbf3 (tbf/earn-tokens tbf2 5)
          tbf4 (tbf/earn-tokens tbf3 15)]

      (is (= (tbf/time-until tbf1) 13))
      (is (= (tbf/time-until tbf2) 3))
      (is (= (tbf/time-until tbf3) 3))
      (is (= (tbf/time-until tbf4) 0)))))

(deftest test-fractional-token
  ;; If we earn tokens a fraction at a time, make sure we keep all of the fractions and don't lose them from roundoff.
  (testing "Earn fractional token and maxing out bucket"
    (let [tbf2
          (loop [ii 1000
                 tbf (tbf/create-tbf 0.01 4 0)]
            (if (= 0 ii)
              tbf
              (recur (- ii 1) (tbf/earn-tokens tbf ii))))]
      (is (approx= (:current-tokens tbf2) 4))))

  (testing "Earn fractional token and not maxing bucket"
    ;; Note: Count by 3 timeunits, so we earn 12 tokens.
    (let [tbf2
          (loop [ii 3000
                 tbf (tbf/create-tbf 0.01 40 0)]
            (if (= 0 ii)
              tbf
              (recur (- ii 3) (tbf/earn-tokens tbf ii))))]
      (is (approx= (:current-tokens tbf2) 30)))))

(deftest test-take-fraction
  (let [tbf (tbf/create-tbf 0.1 100 0)
        tbf1 (tbf/spend-tokens tbf 0 130)
        tbf2 (tbf/spend-tokens tbf1 500 0)
        tbf3 (tbf/spend-tokens tbf2 1000 3)
        tbf4 (tbf/spend-tokens tbf3 500 3)
        tbf5 (tbf/spend-tokens tbf4 1500 10)
        tbf6 (tbf/spend-tokens tbf5 3000 5)]
    ;; -130 tokens.
    (is (approx= (:current-tokens tbf1) -130))
    (is (approx= (:current-tokens tbf2) -80)) ; -130 + 50 - 0
    (is (approx= (:current-tokens tbf3) -33)) ; -80 +50 - 3
    (is (approx= (:current-tokens tbf4) -36)) ; -27 -3
    (is (approx= (:current-tokens tbf5) 4)) ; -36 + 50 - 10
    (is (approx= (:current-tokens tbf6) 95)))) ;4 + 150 - 5 -- We earn, hit the max, then spend!

(deftest test-spend-tokens
  (let [tbf (tbf/create-tbf 10.0 100 0)
        tbf1 (tbf/spend-tokens tbf 0 130)
        tbf2 (tbf/spend-tokens tbf1 5 0)
        tbf3 (tbf/spend-tokens tbf2 10 3)
        tbf4 (tbf/spend-tokens tbf3 5 3)
        tbf5 (tbf/spend-tokens tbf4 15 10)
        tbf6 (tbf/spend-tokens tbf5 30 5)]
    ;; -130 tokens.
    (testing "We have the expected tokens"
      (is (approx= (:current-tokens tbf1) -130))
      (is (approx= (:current-tokens tbf2) -80)) ; -130 + 50 - 0
      (is (approx= (:current-tokens tbf3) -33)) ; -80 +50 - 3
      (is (approx= (:current-tokens tbf4) -36)) ; -27 -3
      (is (approx= (:current-tokens tbf5) 4)) ; -36 + 50 - 10
      (is (approx= (:current-tokens tbf6) 95))) ;4 + 150 - 5

    (testing "We report in-debt? correctly"
      (is (tbf/in-debt? tbf1))
      (is (tbf/in-debt? tbf2))
      (is (tbf/in-debt? tbf3))
      (is (tbf/in-debt? tbf4))
      (is (not (tbf/in-debt? tbf5)))
      (is (not (tbf/in-debt? tbf6))))))
