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
(ns cook.test.mesos.reason
  (:use clojure.test)
  (:require [cook.mesos.reason :as r]
            [cook.test.postgres]
            [cook.test.testutil :refer (restore-fresh-database!)]
            [datomic.api :as d :refer (q db)]))

(use-fixtures :once cook.test.postgres/with-pg-db)

(deftest reasons-api
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        default-limit 10
        ;; set default failure limit.
        _ @(d/transact conn [{:db/id :scheduler/config
                              :scheduler.config/mea-culpa-failure-limit
                              default-limit}])
        db (d/db conn)]

    (testing "all-known-failure-reasons"
      (let [reasons (r/all-known-reasons db)]
        (doseq [reason reasons]
          ;; testing in any more detail is overkill; it would amount
          ;; to proving that the schema is the schema
          (is (instance? datomic.Entity reason)))))

    (testing "default-failure-limit"
      (is (= default-limit (r/default-failure-limit db))))))
