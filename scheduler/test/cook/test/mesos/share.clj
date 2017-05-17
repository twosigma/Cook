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
  (:require [cook.mesos.share :as share]
            [cook.test.testutil :refer (restore-fresh-database!)]
            [metatransaction.core :as mt]
            [datomic.api :as d]))

(deftest test'
  (let [uri "datomic:mem://test"
        conn (restore-fresh-database! uri)]
    (share/set-share! conn "u1" "needs some CPUs" :cpus 20.0 :mem 2.0)
    (share/set-share! conn "u1" "not enough mem" :cpus 20.0 :mem 10.0)
    (share/set-share! conn "u1" "too many CPUs" :cpus 5.0)
    (share/set-share! conn "u2" "custom limits" :cpus 5.0  :mem 10.0)
    (share/set-share! conn "u3" "needs cpus, mem, and gpus" :cpus 10.0 :mem 20.0 :gpus 4.0)
    (share/set-share! conn "default" "lock most users down" :cpus 1.0 :mem 2.0 :gpus 1.0)
    (let [db (mt/db conn)]
      (testing "set and query."
        (is (= {:cpus 5.0 :mem 10.0 :gpus 1.0} (share/get-share db "u2"))))
      (testing "set and override."
        (is (= {:cpus 5.0 :mem 10.0 :gpus 1.0} (share/get-share db "u1"))))
      (testing "query default."
        (is (= {:cpus 1.0 :mem 2.0 :gpus 1.0} (share/get-share db "default"))))
      (testing "query unknown user."
        (is (= (share/get-share db "whoami") (share/get-share db "default"))))
      (testing "retract share"
        (share/retract-share! conn "u2" "not special anymore")
        (let [db (mt/db conn)]
          (is (= {:cpus 1.0 :mem 2.0 :gpus 1.0} (share/get-share db "u2")))))

      (testing "get-shares:all-defaults-available"
        (is (= {"u1" {:cpus 5.0 :mem 10.0 :gpus 1.0}
                "u2" {:cpus 5.0 :mem 10.0 :gpus 1.0}
                "u3" {:cpus 10.0 :mem 20.0 :gpus 4.0}
                "u4" {:cpus 1.0 :mem 2.0 :gpus 1.0}}
               (share/get-shares db ["u1" "u2" "u3" "u4"]))))
      (testing "get-shares:some-defaults-available"
        (share/retract-share! conn "default" "clear defaults")
        (share/set-share! conn "default" "cpu and gpu defaults available" :cpus 3.0 :gpus 1.0)
        (let [db (mt/db conn)]
          (is (= {"u1" {:cpus 5.0 :mem 10.0 :gpus 1.0}
                  "u2" {:cpus 3.0 :mem Double/MAX_VALUE :gpus 1.0}
                  "u3" {:cpus 10.0 :mem 20.0 :gpus 4.0}
                  "u4" {:cpus 3.0 :mem Double/MAX_VALUE :gpus 1.0}}
                 (share/get-shares db ["u1" "u2" "u3" "u4"])))))

      (testing "share history"
        (let [db-after (d/db conn)
              share-history-u1 (share/share-history db-after "u1")
              share-history-u2 (share/share-history db-after "u2")]
          (is (= (mapv :reason share-history-u1
                       ["needs some CPUs" "not enough mem" "too many CPUs"])))
          (is (= (mapv :share share-history-u1
                       [{:mem 2.0, :cpus 20.0, :gpus Double/MAX_VALUE}
                        {:mem 10.0, :cpus 20.0, :gpus Double/MAX_VALUE}
                        {:mem 10.0, :cpus 5.0, :gpus Double/MAX_VALUE}])))
          (is (= (mapv :reason share-history-u2
                       ["custom limits" "not special anymore"])))
          (is (= (mapv :share share-history-u2
                       [{:mem 10.0, :cpus 5.0, :gpus Double/MAX_VALUE}
                        {:mem 2.0, :cpus 1.0, :gpus 1.0}]))))))))
