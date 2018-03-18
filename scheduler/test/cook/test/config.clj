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
            [cook.config :refer (config env read-edn-config)]
            [cook.test.testutil :refer (setup)]))

(deftest test-read-edn-config
  (is (= {} (read-edn-config "{}")))
  (is (= {:foo 1} (read-edn-config "{:foo 1}")))
  (with-redefs [env (constantly "something")]
    (is (= {:master "something"} (read-edn-config "{:master #config/env \"MESOS_MASTER\"}"))))
  (with-redefs [env (constantly "12345")]
    (is (= {:port "12345"} (read-edn-config "{:port #config/env \"COOK_PORT\"}")))
    (is (= {:port 12345} (read-edn-config "{:port #config/env-int \"COOK_PORT\"}")))))

(deftest test-redef-config
  (setup :config {:database {:datomic-uri "foo"}})
  (is (= "foo" (-> config :settings :mesos-datomic-uri)))
  (with-redefs [config {:settings {:mesos-datomic-uri "bar"}}]
    (is (= "bar" (-> config :settings :mesos-datomic-uri))))
  (is (= "foo" (-> config :settings :mesos-datomic-uri))))
