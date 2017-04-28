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
            [cook.mesos.quota :as quota]
            [cook.test.testutil :refer (restore-fresh-database!)]
            [metatransaction.core :as mt :refer (db)]
            [datomic.api :as d]))

(deftest test-quota
  (let [uri "datomic:mem://test"
        conn (restore-fresh-database! uri)]
    (quota/set-quota! conn "u1" "needs some CPUs" :cpus 20.0 :mem 2.0)
    (quota/set-quota! conn "u1" "not enough mem" :cpus 20.0 :mem 10.0)
    (quota/set-quota! conn "u1" "too many CPUs" :cpus 5.0)
    (quota/set-quota! conn "u1" "higher count" :count 6)
    (quota/set-quota! conn "u2" "custom limits" :cpus 5.0  :mem 10.0)
    (quota/set-quota! conn "default" "lock most users down" :cpus 1.0 :mem 2.0 :gpus 1.0)
    (let [db (db conn)]
      (testing "set and query."
        (is (= {:count Double/MAX_VALUE
                :cpus 5.0 :mem 10.0 :gpus 1.0} (quota/get-quota db "u2"))))
      (testing "set and overide."
        (is (= {:count 6
                :cpus 5.0 :mem 10.0 :gpus 1.0} (quota/get-quota db "u1"))))
      (testing "query default."
        (is (= {:count Double/MAX_VALUE
                :cpus 1.0 :mem 2.0 :gpus 1.0} (quota/get-quota db "default"))))
      (testing "query unknown user."
        (is (= (quota/get-quota db "whoami") (quota/get-quota db "default"))))
      (testing "retract quota"
        (quota/retract-quota! conn "u2" "not special anymore")
        (let [db (mt/db conn)]
          (is (= {:count Double/MAX_VALUE
                  :cpus 1.0 :mem 2.0 :gpus 1.0} (quota/get-quota db "u2")))))

      (testing "quota history"
        (let [db-after (d/db conn)
              quota-history-u1 (quota/quota-history db-after "u1")
              quota-history-u2 (quota/quota-history db-after "u2")]
          (is (= (mapv :reason quota-history-u1
                       ["needs some CPUs" "not enough mem" "too many CPUs"])))
          (is (= (mapv :quota quota-history-u1
                       [{:count Double/MAX_VALUE
                         :mem 2.0, :cpus 20.0, :gpus Double/MAX_VALUE}
                        {:count Double/MAX_VALUE
                         :mem 10.0, :cpus 20.0, :gpus Double/MAX_VALUE}
                        {:count Double/MAX_VALUE
                         :mem 10.0, :cpus 5.0, :gpus Double/MAX_VALUE}])))
          (is (= (mapv :reason quota-history-u2
                       ["custom limits" "not special anymore"])))
          (is (= (mapv :quota quota-history-u2
                       [{:count Double/MAX_VALUE
                         :mem 10.0, :cpus 5.0, :gpus Double/MAX_VALUE}
                        {:count Double/MAX_VALUE
                         :mem 2.0, :cpus 1.0, :gpus 1.0}]))))))))
