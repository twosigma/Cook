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
(ns cook.test.core
  (:use [cook.util]
        [clojure.tools.cli :only [cli]])
  (:use [clojure.test]))


(deftest combiner-options
  (let [[options _ _] (cli ["-a" "bar" "-e" "apple" "-a" "foo" "-e" "banana" "-e" "carrot"]
                                  ["-a"]
                                  ["-e" :combine-fn concat :parse-fn vector])]
        (is (= {:a "foo" :e ["apple" "banana" "carrot"]} options))
        ))
