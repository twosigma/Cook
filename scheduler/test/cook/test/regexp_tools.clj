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
(ns cook.test.regexp_tools
  (:require [clojure.test :refer :all]
            [cook.regexp-tools :as regexp-tools]))

(deftest test-match-based-on-regexp
  (let [key "kubernetes"
        regexp-name :compute-cluster-regex
        match-list [{:compute-cluster-regex "^.*$", :tbf-config {:bucket-size 1}}]]
    (is (= {:bucket-size 1}
           (regexp-tools/match-based-on-regexp regexp-name :tbf-config match-list key)))))

(deftest test-match-based-on-pool-name
  (let [matchlist
        [{:pool-regex "^foo$" :field {:foo 1}}
         {:pool-regex ".*" :field {:bar 2}}
         {:pool-regex "^baz$" :field {:baz 3}}]]
    (is (= (regexp-tools/match-based-on-pool-name matchlist "foo" :field) {:foo 1}))
    (is (= (regexp-tools/match-based-on-pool-name matchlist "bar" :field) {:bar 2}))
    (is (= (regexp-tools/match-based-on-pool-name matchlist "baz" :field) {:bar 2})))
  (is (= (regexp-tools/match-based-on-pool-name [] "foo" :field) nil)))
