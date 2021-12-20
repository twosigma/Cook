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
            [cook.test.postgres]
            [cook.test.testutil :refer [restore-fresh-database!
                                        setup]]
            [plumbing.core :refer [map-from-keys]])
  (:import (java.util Random UUID)
           (java.math RoundingMode)))

(use-fixtures :once cook.test.postgres/with-pg-db)

(deftest test-incremental-config
  (setup)
  (let [uri "datomic:mem://test-compute-cluster-config"
        conn-atom (atom (restore-fresh-database! uri))
        key :my-incremental-config
        key2 :my-incremental-config-2
        values [{:value "value a" :portion 0.2} {:value "value b" :portion 0.35 :comment "test comment"} {:value "value c" :portion 0.45}]
        values2 [{:value "value d" :portion 0.5} {:value "value e" :portion 0.5 :comment "test comment 2"}]
        uuid-a (java.util.UUID/fromString "41062821-b248-4375-82f8-a8256643c94e")
        uuid-b (java.util.UUID/fromString "61062821-b248-4375-82f8-a8256643c94e")
        uuid-c (java.util.UUID/fromString "21062821-b248-4375-82f8-a8256643c94e")
        rand (Random. 0)
        bytes (byte-array 16)]
    (with-redefs [get-conn (fn [] @conn-atom)]
      (testing "database"
        (is (= '() (read-config key)))
        (write-configs [{:key key :values values}])
        (is (= values (read-config key)))
        (is (= "value a" (select-config-from-key uuid-a key)))
        (is (= "value b" (select-config-from-key uuid-b key)))
        (is (= "value c" (select-config-from-key uuid-c key))))
      (testing "static or dynamic config"
        (is (= "value a" (resolve-incremental-config uuid-a key)))
        (is (= "value a" (resolve-incremental-config uuid-a values)))
        (is (= nil (resolve-incremental-config uuid-a "other value")))
        (is (= ["value a" :resolved-incremental-config] (resolve-incremental-config uuid-a key "fallback")))
        (is (= ["value a" :resolved-incremental-config] (resolve-incremental-config uuid-a values "fallback")))
        (is (= ["fallback" :used-fallback-config] (resolve-incremental-config uuid-a "other value" "fallback"))))
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
                                        (map (fn [v] {:value v :portion (some-> v freqs round)}))
                                        (filter (fn [{:keys [portion]}] portion)))))]
          (is (= (map (fn [{:keys [value portion]}] {:value value :portion portion}) values) (get-distribution rand bytes key)))
          ; override
          (write-configs [{:key key :values values2}])
          (is (= (map (fn [{:keys [value portion]}] {:value value :portion portion}) values2) (get-distribution rand bytes key)))))
      (testing "multiple configs"
        (write-configs [{:key key2 :values values}])
        (is (= values2 (read-config key)))
        (is (= values (read-config key2))))
      (testing "miss"
        (is (= nil (select-config-from-values uuid-a nil)))
        (is (= nil (select-config-from-values uuid-a '())))
        (is (= nil (select-config-from-values uuid-a [{}]))))
      (testing "multiple configs - write two at once"
        (reset! conn-atom (restore-fresh-database! uri))
        (is (= '() (read-config key)))
        (is (= '() (read-config key2)))
        (write-configs [{:key key :values values} {:key key2 :values values2}])
        (is (= values (read-config key)))
        (is (= values2 (read-config key2)))))))