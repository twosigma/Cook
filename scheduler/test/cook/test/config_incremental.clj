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
(ns cook.test.config-incremental
  (:require [clojure.test :refer :all]
            [cook.config-incremental :refer :all]
            [cook.test.testutil :refer [restore-fresh-database!
                                        setup]]
            [plumbing.core :refer [map-from-keys]])
  (:import (java.util Random UUID)
           (java.math RoundingMode)))

(deftest test-incremental-config
  (setup)
  (let [uri "datomic:mem://test-compute-cluster-config"
        conn (restore-fresh-database! uri)
        key :my-incremental-config
        key2 :my-incremental-config-2
        values [0.2 "value a" 0.35 "value b" 0.45 "value c"]
        values2 [0.5 "value d" 0.5 "value e"]
        uuid-a (java.util.UUID/fromString "41062821-b248-4375-82f8-a8256643c94e")
        uuid-b (java.util.UUID/fromString "61062821-b248-4375-82f8-a8256643c94e")
        uuid-c (java.util.UUID/fromString "21062821-b248-4375-82f8-a8256643c94e")
        rand (Random. 0)
        bytes (byte-array 16)]
    (with-redefs [get-conn (fn [] conn)]
      (testing "database"
        (is (= '() (read-config key)))
        (write-config key values)
        (let [[wv-a wv-b wv-c] (read-config key)
              fields [:weighted-value/weight :weighted-value/value]]
          (is (= {:weighted-value/weight 0.2 :weighted-value/value "value a"} (map-from-keys #(% wv-a) fields)))
          (is (= {:weighted-value/weight 0.35 :weighted-value/value "value b"} (map-from-keys #(% wv-b) fields)))
          (is (= {:weighted-value/weight 0.45 :weighted-value/value "value c"} (map-from-keys #(% wv-c) fields))))
        (is (= "value a" (select-config-from-key uuid-a key)))
        (is (= "value b" (select-config-from-key uuid-b key)))
        (is (= "value c" (select-config-from-key uuid-c key))))
      (testing "static or dynamic config"
        (is (= "value a" (resolve-incremental-config uuid-a key)))
        (is (= "value a" (resolve-incremental-config uuid-a values)))
        (is (= "other value" (resolve-incremental-config uuid-a "other value"))))
      (testing "statistical distribution"
        (let [get-distribution (fn get-distribution
                                 [rand bytes key]
                                 (let [samples 10000
                                       freqs (->> (range samples)
                                                  (map (fn [_]
                                                         (.nextBytes rand bytes)
                                                         (select-config-from-key (UUID/nameUUIDFromBytes bytes) key)))
                                                  frequencies)
                                       round #(double (.setScale (bigdec (/ % samples)) 2 RoundingMode/HALF_EVEN))]
                                   (->> ["value a" "value b" "value c" "value d" "value e"]
                                        (map (fn [v] (some-> v freqs round (vector v))))
                                        (keep seq)
                                        flatten)))]
          (is (= values (get-distribution rand bytes key)))
          ; override
          (write-config key values2)
          (is (= values2 (get-distribution rand bytes key)))))
      (testing "multiple configs"
        (write-config key2 values)
        (is (= values2 (weighted-values->flat-values-array (read-config key))))
        (is (= values (weighted-values->flat-values-array (read-config key2))))))))