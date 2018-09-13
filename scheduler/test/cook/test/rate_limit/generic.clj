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
  (:use clojure.test)
  (:require [cook.rate-limit.generic :as rt]))

(deftest independent-keys-1
  (let [ratelimit (rt/make-token-bucket-filter 60000 60 10 true)]
    (rt/time-until-out-of-debt-millis! ratelimit "Foo2")
    (rt/spend-tokens! ratelimit "Foo4" 100)
    (is (= 2 (.size (.asMap (:cache ratelimit)))))))

(deftest independent-keys-2
  (let [ratelimit (rt/make-token-bucket-filter 60000 60 10 true)]
    (rt/earn-tokens! ratelimit "Foo4")
    (rt/earn-tokens! ratelimit "Foo1")
    (rt/time-until-out-of-debt-millis! ratelimit "Foo1")
    (rt/time-until-out-of-debt-millis! ratelimit "Foo2")
    (rt/spend-tokens! ratelimit "Foo3" 100)
    (rt/spend-tokens! ratelimit "Foo4" 100)
    (is (= 4 (.size (.asMap (:cache ratelimit)))))))

(deftest earning-tokens-explicit
  (let [ratelimit (rt/make-token-bucket-filter 20 60000 10 true)]
    ;; take away the full bucket it starts with... (20 tokens)
    (with-redefs [rt/current-time-in-millis (fn [] 1000000)]
      (rt/spend-tokens! ratelimit "Foo1" 20)
      (rt/spend-tokens! ratelimit "Foo2" 20)
      (rt/spend-tokens! ratelimit "Foo3" 20)
      (rt/spend-tokens! ratelimit "Foo4" 20)

      ;; Should be able to do this first request almost instantly.
      (is (= 0 (rt/time-until-out-of-debt-millis! ratelimit "Foo1")))
      (is (= 0 (rt/time-until-out-of-debt-millis! ratelimit "Foo2")))
      (is (= 0 (rt/time-until-out-of-debt-millis! ratelimit "Foo3")))

      (rt/spend-tokens! ratelimit "Foo1" 0)
      (rt/spend-tokens! ratelimit "Foo2" 10)
      (rt/spend-tokens! ratelimit "Foo3" 10000)

      (is (= (.getIfPresent (:cache ratelimit nil) "Foo1") {:current-tokens 0
                                                            :last-update 1000000
                                                            :max-tokens 20
                                                            :token-rate 1.0}))
      (is (= (.getIfPresent (:cache ratelimit nil) "Foo2") {:current-tokens -10
                                                            :last-update 1000000
                                                            :max-tokens 20
                                                            :token-rate 1.0}))
      (is (= (.getIfPresent (:cache ratelimit nil) "Foo3") {:current-tokens -10000
                                                            :last-update 1000000
                                                            :max-tokens 20
                                                            :token-rate 1.0})))

    (with-redefs [rt/current-time-in-millis (fn [] 1000001)]
      ;; We've earned tokens.
      (is (= 0 (rt/time-until-out-of-debt-millis! ratelimit "Foo1")))
      (is (= 9 (rt/time-until-out-of-debt-millis! ratelimit "Foo2")))
      (is (= 9999 (rt/time-until-out-of-debt-millis! ratelimit "Foo3")))

      (is (= (.getIfPresent (:cache ratelimit nil) "Foo1") {:current-tokens 1
                                                            :last-update 1000001
                                                            :max-tokens 20
                                                            :token-rate 1.0}))
      (is (= (.getIfPresent (:cache ratelimit nil) "Foo2") {:current-tokens -9
                                                            :last-update 1000001
                                                            :max-tokens 20
                                                            :token-rate 1.0}))
      (is (= (.getIfPresent (:cache ratelimit nil) "Foo3") {:current-tokens -9999
                                                            :last-update 1000001
                                                            :max-tokens 20
                                                            :token-rate 1.0})))

    (with-redefs [rt/current-time-in-millis (fn [] 1000025)]
      ;; We've earned tokens. First two are out of debt.. Foo3 is in debt.
      (is (= 0 (rt/time-until-out-of-debt-millis! ratelimit "Foo1")))
      (is (= 0 (rt/time-until-out-of-debt-millis! ratelimit "Foo2")))
      (is (= 9975 (rt/time-until-out-of-debt-millis! ratelimit "Foo3")))

      (is (= (.getIfPresent (:cache ratelimit nil) "Foo1") {:current-tokens 20
                                                            :last-update 1000025
                                                            :max-tokens 20
                                                            :token-rate 1.0}))
      (is (= (.getIfPresent (:cache ratelimit nil) "Foo2") {:current-tokens 15
                                                            :last-update 1000025
                                                            :max-tokens 20
                                                            :token-rate 1.0}))
      (is (= (.getIfPresent (:cache ratelimit nil) "Foo3") {:current-tokens -9975
                                                            :last-update 1000025
                                                            :max-tokens 20
                                                            :token-rate 1.0}))

      ; Make sure Foo4 is stale.
      (is (= (.getIfPresent (:cache ratelimit nil) "Foo4") {:current-tokens 0
                                                            :last-update 1000000
                                                            :max-tokens 20
                                                            :token-rate 1.0}))
      (is (= 0 (rt/time-until-out-of-debt-millis! ratelimit "Foo4")))
      ; And not stale
      (is (= (.getIfPresent (:cache ratelimit nil) "Foo4") {:current-tokens 20
                                                            :last-update 1000025
                                                            :max-tokens 20
                                                            :token-rate 1.0})))))
