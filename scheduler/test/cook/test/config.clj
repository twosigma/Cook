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
(ns cook.test.config
  (:require [clojure.test :refer :all]
            [cook.config :as config]))

(deftest test-read-edn-config
  (is (= {} (config/read-edn-config "{}")))
  (is (= {:foo 1} (config/read-edn-config "{:foo 1}")))
  (with-redefs [config/env (constantly "something")]
    (is (= {:master "something"} (config/read-edn-config "{:master #config/env \"MESOS_MASTER\"}"))))
  (with-redefs [config/env (constantly "12345")]
    (is (= {:port "12345"} (config/read-edn-config "{:port #config/env \"COOK_PORT\"}")))
    (is (= {:port 12345} (config/read-edn-config "{:port #config/env-int \"COOK_PORT\"}")))))