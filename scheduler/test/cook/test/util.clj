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
(ns cook.test.util
  (:require [clojure.test :refer :all]
            [cook.util :refer :all])
  (:import (java.util UUID)))


(deftest test-diff-map-keys
  (is (= [#{:b} #{:c} #{:a :d}]
         (diff-map-keys {:a {:a :a}
                         :b {:b :b}
                         :d {:d :d}}
                        {:a {:a :a}
                         :c {:c :c}
                         :d {:d :e}})))
  (is (= [nil #{:c} #{:a :d}]
         (diff-map-keys {:a {:a :a}
                         :d {:d :d}}
                        {:a {:a :a}
                         :c {:c :c}
                         :d {:d :e}})))
  (is (= [#{:b} nil #{:a :d}]
         (diff-map-keys {:a {:a :a}
                         :b {:b :b}
                         :d {:d :d}}
                        {:a {:a :a}
                         :d {:d :e}}))))

(deftest test-deep-merge-with
  (is (= {:a {:b {:z 3, :c 3, :d {:z 9, :x 1, :y 2}}, :e 103}, :f 4}
         (deep-merge-with +
                          {:a {:b {:c 1 :d {:x 1 :y 2}} :e 3} :f 4}
                          {:a {:b {:c 2 :d {:z 9} :z 3} :e 100}})))
  (is (= {"foo" 2}
         (deep-merge-with - {"foo" 3} {"foo" 1})))
  (is (thrown? NullPointerException
               (deep-merge-with - {"foo" nil} {"foo" 1})))
  (is (thrown? NullPointerException
               (deep-merge-with - {"foo" 1} {"foo" nil}))))


(deftest test-set-atom!
  (let [state (atom {})]
    (is (= @state {}))
    (is (= (set-atom! state "a") {}))
    (is (= (set-atom! state {:a :b}) "a"))
    (is (= @state {:a :b}))))

(deftest test-format-map-for-structured-logging
  "Tests that the format-map-for-structured logging preserves nested maps."
  (let [uuid (UUID/randomUUID)
        map {:integer 2 :float 1.2 :string "foo" :uuid uuid :nested-map {:nested-string "bar" :nested-int 3}}
        formatted-map (format-map-for-structured-logging map)]
    (is (= {:integer 2 :float 1.2 :string "foo" :uuid (str uuid) :nested-map {:nested-string "bar" :nested-int 3}} formatted-map))))
