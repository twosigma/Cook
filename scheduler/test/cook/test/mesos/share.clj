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
(ns cook.test.mesos.share
  (:use clojure.test)
  (:require [cook.mesos.scheduler :as sched]
            [cook.mesos.share :as share]
            [cook.test.testutil :refer (restore-fresh-database!)]
            [metatransaction.core :as mt :refer (db)]))

(deftest test'
  (let [uri "datomic:mem://test"
        conn (restore-fresh-database! uri)]
    (share/set-share! conn "u1" :cpus 20.0)
    (share/set-share! conn "u1" :cpus 20.0 :mem 10.0)
    (share/set-share! conn "u1" :cpus 5.0)
    (share/set-share! conn "u2" :cpus 5.0  :mem 10.0)
    (share/set-share! conn "default" :cpus 1.0 :mem 2.0 :gpus 1.0)
    (let [db (db conn)]
      (testing "set and query."
        (is (= {:cpus 5.0 :mem 10.0 :gpus 1.0} (share/get-share db "u2"))))
      (testing "set and overide."
        (is (= {:cpus 5.0 :mem 10.0 :gpus 1.0} (share/get-share db "u1"))))
      (testing "query default."
        (is (= {:cpus 1.0 :mem 2.0 :gpus 1.0} (share/get-share db "default"))))
      (testing "query unknown user."
        (is (= (share/get-share db "whoami") (share/get-share db "default"))))
      (testing "retract share"
        (share/retract-share! conn "u2")
        (let [db (mt/db conn)]
          (is (= {:cpus 1.0 :mem 2.0 :gpus 1.0} (share/get-share db "u2"))))))))
