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
(ns cook.test.quota
  (:require [clojure.test :refer :all]
            [cook.config :as config]
            [cook.quota :as quota]
            [cook.test.postgres]
            [cook.test.testutil :refer [create-pool restore-fresh-database!]]
            [datomic.api :as d]
            [metatransaction.core :as mt :refer [db]]))

(use-fixtures :once cook.test.postgres/with-pg-db)

(deftest test-quota
  (let [uri "datomic:mem://test"
        conn (restore-fresh-database! uri)]
    (quota/set-quota! conn "u1" nil "needs some CPUs" :cpus 20.0 :mem 2.0)
    (quota/set-quota! conn "u1" nil "not enough mem" :cpus 20.0 :mem 10.0)
    (quota/set-quota! conn "u1" nil "too many CPUs" :cpus 5.0)
    (quota/set-quota! conn "u1" nil "higher count" :count 6)
    (quota/set-quota! conn "u2" nil "custom limits" :cpus 5.0  :mem 10.0)
    (quota/set-quota! conn "u3" nil "needs no GPUs" :gpus 0.0)
    (quota/set-quota! conn "u4" nil "no jobs allowed" :count 0)
    (quota/set-quota! conn "default" nil "lock most users down" :cpus 1.0 :mem 2.0 :gpus 1.0)
    (let [db (db conn)]
      (testing "set and query zero job count"
        (is (= {:count 0 :cpus 1.0 :mem 2.0 :gpus 1.0
                :launch-rate-per-minute quota/default-launch-rate-per-minute
                :launch-rate-saved quota/default-launch-rate-saved}
               (quota/get-quota db "u4" nil))))
      (testing "set and query zero gpus"
        (is (= {:count Integer/MAX_VALUE
                :cpus 1.0 :mem 2.0 :gpus 0.0
                :launch-rate-per-minute quota/default-launch-rate-per-minute
                :launch-rate-saved quota/default-launch-rate-saved} (quota/get-quota db "u3" nil))))
      (testing "set and query."
        (is (= {:count Integer/MAX_VALUE
                :cpus 5.0 :mem 10.0 :gpus 1.0
                :launch-rate-per-minute quota/default-launch-rate-per-minute
                :launch-rate-saved quota/default-launch-rate-saved} (quota/get-quota db "u2" nil))))
      (testing "set and overide."
        (is (= {:count 6 :cpus 5.0 :mem 10.0 :gpus 1.0
                :launch-rate-per-minute quota/default-launch-rate-per-minute
                :launch-rate-saved quota/default-launch-rate-saved} (quota/get-quota db "u1" nil))))
      (testing "query default."
        (is (= {:count Integer/MAX_VALUE
                :cpus 1.0 :mem 2.0 :gpus 1.0
                :launch-rate-per-minute quota/default-launch-rate-per-minute
                :launch-rate-saved quota/default-launch-rate-saved} (quota/get-quota db "default" nil))))
      (testing "query unknown user."
        (is (= (quota/get-quota db "whoami" nil) (quota/get-quota db "default" nil))))
      (testing "retract quota"
        (quota/retract-quota! conn "u2" nil "not special anymore")
        (let [db (mt/db conn)]
          (is (= {:count Integer/MAX_VALUE
                  :cpus 1.0 :mem 2.0 :gpus 1.0
                  :launch-rate-per-minute quota/default-launch-rate-per-minute
                  :launch-rate-saved quota/default-launch-rate-saved} (quota/get-quota db "u2" nil))))))))

(deftest test-count-migration
  (let [uri "datomic:mem://test-count-migration"
        conn (restore-fresh-database! uri)]
    (testing "writes to resource instead of field"
      (quota/set-quota! conn "u1" nil "setting count" :count 1)
      (is (= {:count 1 :mem Double/MAX_VALUE :cpus Double/MAX_VALUE :gpus Double/MAX_VALUE
              :launch-rate-per-minute quota/default-launch-rate-per-minute
              :launch-rate-saved quota/default-launch-rate-saved}
             (quota/get-quota (d/db conn) "u1" nil)))
      (let [quota (d/entity (d/db conn) [:quota/user "u1"])
            resource (:quota/resource quota)]
        (is (nil? (:quota/count quota)))
        (is (= 1 (count resource)))
        (let [count-quota (first resource)]
          (is (= :resource.type/count (:resource/type count-quota)))
          (is (= 1.0 (:resource/amount count-quota))))))

    (testing "priority for reading count"
      (quota/set-quota! conn "u2" nil "setting cpus" :cpus 1.0)
      ; Fifth priority - Default value
      (is (= Integer/MAX_VALUE (:count (quota/get-quota (d/db conn) "u2" nil))))
      (quota/set-quota! conn "default" nil "test" :cpus 1.0)
      (quota/set-quota! conn "u3" nil "test" :count 5)
      @(d/transact conn [[:db/add [:quota/user "u3"] :quota/count 4]
                         [:db/add [:quota/user "default"] :quota/count 8]
                         [:db/add [:quota/user "u2"] :quota/count 6]])

      ; First priority - quota resource
      (is (= 5 (:count (quota/get-quota (d/db conn) "u3" nil))))
      ; Second priority - quota field
      (is (= 6 (:count (quota/get-quota (d/db conn) "u2" nil))))
      ; Fourth priority - quota field on default user
      (is (= 8 (:count (quota/get-quota (d/db conn) "fakeuser" nil))))
      (quota/set-quota! conn "default" nil "setting count" :cpus 1.0 :count 2)
      ; Third priority - quota resource on default user
      (is (= 2 (:count (quota/get-quota (d/db conn) "fakeuser" nil)))))

    (testing "retracts both count fields"
      (quota/retract-quota! conn "default" nil "clearing default")
      (quota/set-quota! conn "u1" nil "setting count" :count 1)
      @(d/transact conn [[:db/add [:quota/user "u1"] :quota/count 1]])
      (quota/retract-quota! conn "u1" nil "clearing user")

      (is (= Integer/MAX_VALUE (:count (quota/get-quota (d/db conn) "u1" nil)))))

    (testing "set-quota! clears count field"
      (quota/set-quota! conn "u1" nil "setup" :cpus 1.0)
      @(d/transact conn [[:db/add [:quota/user "u1"] :quota/count 1]])
      (is (= 1 (:quota/count (d/entity (d/db conn) [:quota/user "u1"]))))
      (quota/set-quota! conn "u1" nil "should clear count" :cpus 2.0)
      (is (nil? (:quota/count (d/entity (d/db conn) [:quota/user "u1"])))))))


(deftest test-pool-support
  (let [uri "datomic:mem://test-quota-pool-support"
        conn (restore-fresh-database! uri)]
    (create-pool conn "pool-1")
    (create-pool conn "pool-2")
    (create-pool conn "pool-3")
    (quota/set-quota! conn "default" nil "strict" :cpus 1.0 :mem 1.0 :gpus 1.0 :count 1)
    (quota/set-quota! conn "default" "pool-1" "lenient" :cpus 10.0 :mem 10.0 :gpus 10.0 :count 10.0)
    (quota/set-quota! conn "u1" nil "power user" :cpus 20.0 :mem 20.0)
    (quota/set-quota! conn "u1" "pool-1" "really lenient" :cpus 100.0 :mem 100.0)
    (testing "get-quota, no default pool configured"
      (let [db (mt/db conn)]
        (is (= {:cpus 20.0 :mem 20.0 :gpus 1.0 :count 1
                :launch-rate-per-minute quota/default-launch-rate-per-minute
                :launch-rate-saved quota/default-launch-rate-saved} (quota/get-quota db "u1" nil)))
        (is (= {:cpus 100.0 :mem 100.0 :gpus 10.0 :count 10
                :launch-rate-per-minute quota/default-launch-rate-per-minute
                :launch-rate-saved quota/default-launch-rate-saved} (quota/get-quota db "u1" "pool-1")))
        (is (= {:cpus Double/MAX_VALUE :mem Double/MAX_VALUE :gpus Double/MAX_VALUE
                :count Integer/MAX_VALUE
                :launch-rate-per-minute quota/default-launch-rate-per-minute
                :launch-rate-saved quota/default-launch-rate-saved}
               (quota/get-quota db "u1" "pool-2")))

        (is (= {:cpus 1.0 :mem 1.0 :gpus 1.0 :count 1
                :launch-rate-per-minute quota/default-launch-rate-per-minute
                :launch-rate-saved quota/default-launch-rate-saved} (quota/get-quota db "u2" nil)))
        (is (= {:cpus 10.0 :mem 10.0 :gpus 10.0 :count 10
                :launch-rate-per-minute quota/default-launch-rate-per-minute
                :launch-rate-saved quota/default-launch-rate-saved} (quota/get-quota db "u2" "pool-1")))))

    (testing "get-quota, default pool configured"
      (with-redefs [config/default-pool (constantly "pool-1")]
        (let [db (mt/db conn)]
          ; Should use the explicit defaults from pool-1
          (is (= {:cpus 100.0 :mem 100.0 :gpus 10.0 :count 10
                  :launch-rate-per-minute quota/default-launch-rate-per-minute
                  :launch-rate-saved quota/default-launch-rate-saved} (quota/get-quota db "u1" nil)))
          (is (= {:cpus 100.0 :mem 100.0 :gpus 10.0 :count 10
                  :launch-rate-per-minute quota/default-launch-rate-per-minute
                  :launch-rate-saved quota/default-launch-rate-saved} (quota/get-quota db "u1" "pool-1")))
          (is (= {:cpus Double/MAX_VALUE :mem Double/MAX_VALUE :gpus Double/MAX_VALUE
                  :count Integer/MAX_VALUE
                  :launch-rate-per-minute quota/default-launch-rate-per-minute
                  :launch-rate-saved quota/default-launch-rate-saved}
                 (quota/get-quota db "u1" "pool-2")))

          (is (= {:cpus 10.0 :mem 10.0 :gpus 10.0 :count 10
                  :launch-rate-per-minute quota/default-launch-rate-per-minute
                  :launch-rate-saved quota/default-launch-rate-saved} (quota/get-quota db "u2" nil)))
          (is (= {:cpus 10.0 :mem 10.0 :gpus 10.0 :count 10
                  :launch-rate-per-minute quota/default-launch-rate-per-minute
                  :launch-rate-saved quota/default-launch-rate-saved} (quota/get-quota db "u2" "pool-1"))))))

    (testing "retract quota, no default pool configured"
      (quota/set-quota! conn "u2" nil "defaults" :cpus 1.0 :mem 1.0 :gpus 1.0 :count 1)
      (quota/set-quota! conn "u2" "pool-2" "pool-2 settings" :cpus 2.0 :mem 2.0 :gpus 2.0 :count 2)
      (let [db (mt/db conn)]
        (is (= {:cpus 2.0 :mem 2.0 :gpus 2.0 :count 2
                :launch-rate-per-minute quota/default-launch-rate-per-minute
                :launch-rate-saved quota/default-launch-rate-saved} (quota/get-quota db "u2" "pool-2"))))

      (quota/retract-quota! conn "u2" "pool-2" "removing quota")
      (let [db (mt/db conn)]
        (is (= {:cpus 1.0 :mem 1.0 :gpus 1.0 :count 1}) (quota/get-quota db "u2" "pool-2")))

      (quota/retract-quota! conn "u2" nil "removing default quota")
      (let [db (mt/db conn)]
        ; With no default pool, we should get the default quota values
        (is (= {:cpus Double/MAX_VALUE :mem Double/MAX_VALUE :gpus Double/MAX_VALUE
                :count Integer/MAX_VALUE
                :launch-rate-per-minute quota/default-launch-rate-per-minute
                :launch-rate-saved quota/default-launch-rate-saved}
               (quota/get-quota db "u2" "pool-2")))))

    (testing "create-user->quota-fn"
      (quota/set-quota! conn "u1" "pool-2" "reason" :cpus 1.0 :mem 1.0 :gpus 1.0 :count 1)
      (quota/set-quota! conn "u2" "pool-2" "reason" :cpus 1.0 :mem 1.0 :gpus 1.0 :count 1)
      (quota/set-quota! conn "u3" "pool-2" "reason" :cpus 1.0 :mem 1.0 :gpus 1.0 :count 1)
      (quota/set-quota! conn "default" "pool-2" "reason" :cpus 2.0 :mem 2.0 :gpus 2.0 :count 2)

      (quota/set-quota! conn "u1" nil "reason" :cpus 5.0 :mem 5.0 :gpus 5.0 :count 5)
      (quota/set-quota! conn "u2" nil "reason" :cpus 5.0 :mem 5.0 :gpus 5.0 :count 5)
      (quota/set-quota! conn "u3" nil "reason" :cpus 5.0 :mem 5.0 :gpus 5.0 :count 5)
      (quota/set-quota! conn "default" nil "reason" :cpus 6.0 :mem 6.0 :gpus 6.0 :count 6)

      (let [user->quota-fn (quota/create-user->quota-fn (mt/db conn) "pool-2")]
        (doseq [user ["u1" "u2" "u3"]]
          (is (= {:cpus 1.0 :mem 1.0 :gpus 1.0 :count 1} (user->quota-fn user))))
        (is (= {:cpus 2.0 :mem 2.0 :gpus 2.0 :count 2
                :launch-rate-per-minute quota/default-launch-rate-per-minute
                :launch-rate-saved quota/default-launch-rate-saved} (user->quota-fn "u4"))))

      (with-redefs [config/default-pool (constantly "pool-3")]
        (let [user->quota-fn (quota/create-user->quota-fn (mt/db conn) "pool-3")]
          (doseq [user ["u1" "u2" "u3"]]
            (is (= {:cpus 5.0 :mem 5.0 :gpus 5.0 :count 5} (user->quota-fn user))))
          (is (= {:cpus 6.0 :mem 6.0 :gpus 6.0 :count 6
                  :launch-rate-per-minute quota/default-launch-rate-per-minute
                  :launch-rate-saved quota/default-launch-rate-saved} (user->quota-fn "u4"))))))))
