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
(ns cook.test.rate-limit.generic
  (:require [clojure.test :refer :all]
            [cook.rate-limit.generic :as rtg]))

(deftest independent-keys-1
  (let [ratelimit (rtg/make-tbf-rate-limiter {:bucket-size 60000
                                              :tokens-replenished-per-minute 60
                                              :expire-minutes 10000
                                              :enforce? true})]
    (rtg/time-until-out-of-debt-millis! ratelimit "Foo2")
    (rtg/spend! ratelimit "Foo4" 100)
    (is (= 2 (.size (.asMap (:cache ratelimit)))))))

(deftest independent-keys-2
  (let [ratelimit (rtg/make-tbf-rate-limiter {:bucket-size 60000
                                              :tokens-replenished-per-minute 60
                                              :expire-minutes 10000
                                              :enforce? true})]
    (rtg/earn-tokens! ratelimit "Foo4")
    (rtg/earn-tokens! ratelimit "Foo1")
    (rtg/time-until-out-of-debt-millis! ratelimit "Foo1")
    (rtg/time-until-out-of-debt-millis! ratelimit "Foo2")
    (rtg/spend! ratelimit "Foo3" 100)
    (rtg/spend! ratelimit "Foo4" 100)
    (is (= 4 (.size (.asMap (:cache ratelimit)))))))

(deftest earning-tokens-explicit
  (let [ratelimit (rtg/make-tbf-rate-limiter {:bucket-size 20
                                              :tokens-replenished-per-minute 60000
                                              :expire-minutes 10
                                              :enforce? true})]
    ;; take away the full bucket it starts with... (20 tokens)
    (with-redefs [rtg/current-time-in-millis (fn [] 1000000)]
      (rtg/spend! ratelimit "Foo1" 20)
      (rtg/spend! ratelimit "Foo2" 20)
      (rtg/spend! ratelimit "Foo3" 20)
      (rtg/spend! ratelimit "Foo4" 20)

      ;; Should be able to do this first request almost instantly.
      (is (= 0 (rtg/time-until-out-of-debt-millis! ratelimit "Foo1")))
      (is (= 0 (rtg/time-until-out-of-debt-millis! ratelimit "Foo2")))
      (is (= 0 (rtg/time-until-out-of-debt-millis! ratelimit "Foo3")))

      (rtg/spend! ratelimit "Foo1" 0)
      (rtg/spend! ratelimit "Foo2" 10)
      (rtg/spend! ratelimit "Foo3" 10000)

      (is (= (.getIfPresent (:cache ratelimit nil) "Foo1") {:current-tokens 0
                                                            :last-update 1000000
                                                            :bucket-size 20
                                                            :token-rate 1.0}))
      (is (= (.getIfPresent (:cache ratelimit nil) "Foo2") {:current-tokens -10
                                                            :last-update 1000000
                                                            :bucket-size 20
                                                            :token-rate 1.0}))
      (is (= (.getIfPresent (:cache ratelimit nil) "Foo3") {:current-tokens -10000
                                                            :last-update 1000000
                                                            :bucket-size 20
                                                            :token-rate 1.0})))

    (with-redefs [rtg/current-time-in-millis (fn [] 1000001)]
      ;; We've earned tokens.
      (is (= 0 (rtg/time-until-out-of-debt-millis! ratelimit "Foo1")))
      (is (= 9 (rtg/time-until-out-of-debt-millis! ratelimit "Foo2")))
      (is (= 9999 (rtg/time-until-out-of-debt-millis! ratelimit "Foo3")))

      (is (= (.getIfPresent (:cache ratelimit nil) "Foo1") {:current-tokens 1
                                                            :last-update 1000001
                                                            :bucket-size 20
                                                            :token-rate 1.0}))
      (is (= (.getIfPresent (:cache ratelimit nil) "Foo2") {:current-tokens -9
                                                            :last-update 1000001
                                                            :bucket-size 20
                                                            :token-rate 1.0}))
      (is (= (.getIfPresent (:cache ratelimit nil) "Foo3") {:current-tokens -9999
                                                            :last-update 1000001
                                                            :bucket-size 20
                                                            :token-rate 1.0})))

    (with-redefs [rtg/current-time-in-millis (fn [] 1000025)]
      ;; We've earned tokens. First two are out of debt.. Foo3 is in debt.
      (is (= 0 (rtg/time-until-out-of-debt-millis! ratelimit "Foo1")))
      (is (= 0 (rtg/time-until-out-of-debt-millis! ratelimit "Foo2")))
      (is (= 9975 (rtg/time-until-out-of-debt-millis! ratelimit "Foo3")))

      (is (= (rtg/get-token-count! ratelimit "Foo1") 20))
      (is (= (rtg/get-token-count! ratelimit "Foo2") 15))
      (is (= (rtg/get-token-count! ratelimit "Foo3") -9975))

      (is (= (.getIfPresent (:cache ratelimit nil) "Foo1") {:current-tokens 20
                                                            :last-update 1000025
                                                            :bucket-size 20
                                                            :token-rate 1.0}))
      (is (= (.getIfPresent (:cache ratelimit nil) "Foo2") {:current-tokens 15
                                                            :last-update 1000025
                                                            :bucket-size 20
                                                            :token-rate 1.0}))
      (is (= (.getIfPresent (:cache ratelimit nil) "Foo3") {:current-tokens -9975
                                                            :last-update 1000025
                                                            :bucket-size 20
                                                            :token-rate 1.0}))

      ; Make sure Foo4 is stale.
      (is (= (.getIfPresent (:cache ratelimit nil) "Foo4") {:current-tokens 0
                                                            :last-update 1000000
                                                            :bucket-size 20
                                                            :token-rate 1.0}))
      (is (= 0 (rtg/time-until-out-of-debt-millis! ratelimit "Foo4")))
      ; And not stale
      (is (= (.getIfPresent (:cache ratelimit nil) "Foo4") {:current-tokens 20
                                                            :last-update 1000025
                                                            :bucket-size 20
                                                            :token-rate 1.0})))))

(deftest per-key-configs
  (let [config {:expire-minutes 10 :enforce? true}
        make-tbf-fn (fn [key]
                      (case key
                        "Bar1" (rtg/make-token-bucket-filter 60000 100)
                        "Bar2" (rtg/make-token-bucket-filter 60000 200)
                        "Bar3" (rtg/make-token-bucket-filter 60000 300)
                        "Bar4" (rtg/make-token-bucket-filter 60000 400)
                        (print "Mismatch key " key)))
        ratelimit (rtg/make-generic-tbf-rate-limiter config make-tbf-fn)]
    (with-redefs [rtg/current-time-in-millis (fn [] 1000000)]
      (rtg/spend! ratelimit "Bar1" 40)
      (rtg/spend! ratelimit "Bar2" 30)
      (rtg/spend! ratelimit "Bar3" 20)
      (rtg/spend! ratelimit "Bar4" 10)

      (is (= (.getIfPresent (:cache ratelimit nil) "Bar1") {:current-tokens 60
                                                            :last-update 1000000
                                                            :bucket-size 100
                                                            :token-rate 1.0}))
      (is (= (.getIfPresent (:cache ratelimit nil) "Bar2") {:current-tokens 170
                                                            :last-update 1000000
                                                            :bucket-size 200
                                                            :token-rate 1.0}))
      (is (= (.getIfPresent (:cache ratelimit nil) "Bar3") {:current-tokens 280
                                                            :last-update 1000000
                                                            :bucket-size 300
                                                            :token-rate 1.0}))
      (is (= (.getIfPresent (:cache ratelimit nil) "Bar4") {:current-tokens 390
                                                            :last-update 1000000
                                                            :bucket-size 400
                                                            :token-rate 1.0}))
      (is (= (rtg/get-token-count! ratelimit "Bar1") 60))
      (is (= (rtg/get-token-count! ratelimit "Bar2") 170))
      (is (= (rtg/get-token-count! ratelimit "Bar3") 280))
      (testing "Flush one and make sure only that one resets"
        (rtg/flush! ratelimit "Bar1")
        (is (= (rtg/get-token-count! ratelimit "Bar1") 100))
        (is (= (rtg/get-token-count! ratelimit "Bar2") 170)))
      ; Flush all.
      (testing "Flush all and make sure only the others reset"
        (rtg/flush! ratelimit nil)
        (is (= (rtg/get-token-count! ratelimit "Bar2") 200))
        (is (= (rtg/get-token-count! ratelimit "Bar3") 300))))))



