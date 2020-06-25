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
(ns cook.test.rest.basic-auth
  (:require [clojure.test :refer :all]
            [cook.rest.basic-auth :as basic-auth]))

(deftest make-user-password-valid?-test
  (testing "none"
    (let [user-password-valid? (basic-auth/make-user-password-valid? :none true)]
      (is (= true (user-password-valid?)))
      (is (= true (user-password-valid? "lol")))
      (is (= true (user-password-valid? "lol" :banana)))))
  (testing "config-file"
    (let [user-password-valid? (basic-auth/make-user-password-valid? :config-file 
                                                                     {:valid-logins #{["abc" "123"]
                                                                                      ["wyegelwe" "lol"]}})]
      (is (= true (user-password-valid? "abc" "123")))
      (is (= true (user-password-valid? "wyegelwe" "lol")))
      (is (= false (user-password-valid? "anything" "else"))))))
