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
(ns cook.test.scheduler.share
  (:require [clojure.test :refer :all]
            [cook.config :as config]
            [cook.test.postgres]
            [cook.scheduler.share :as share]
            [cook.test.testutil :refer [create-pool restore-fresh-database!]]
            [metatransaction.core :as mt]))

(use-fixtures :once cook.test.postgres/with-pg-db)

(deftest test'
  (let [uri "datomic:mem://test"
        conn (restore-fresh-database! uri)]
    (share/set-share! conn "u1" nil "needs some CPUs" :cpus 20.0 :mem 2.0)
    (share/set-share! conn "u1" nil "not enough mem" :cpus 20.0 :mem 10.0)
    (share/set-share! conn "u1" nil "too many CPUs" :cpus 5.0)
    (share/set-share! conn "u2" nil "custom limits" :cpus 5.0  :mem 10.0)
    (share/set-share! conn "u3" nil "needs cpus, mem, and gpus" :cpus 10.0 :mem 20.0 :gpus 4.0)
    (share/set-share! conn "default" nil "lock most users down" :cpus 1.0 :mem 2.0 :gpus 1.0)
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
        (share/retract-share! conn "u2" nil "not special anymore")
        (let [db (mt/db conn)]
          (is (= {:cpus 1.0 :mem 2.0 :gpus 1.0} (share/get-share db "u2")))))

      (testing "get-shares:all-defaults-available"
        (is (= {"u1" {:cpus 5.0 :mem 10.0 :gpus 1.0}
                "u2" {:cpus 5.0 :mem 10.0 :gpus 1.0}
                "u3" {:cpus 10.0 :mem 20.0 :gpus 4.0}
                "u4" {:cpus 1.0 :mem 2.0 :gpus 1.0}}
               (share/get-shares db ["u1" "u2" "u3" "u4"]))))
      (testing "get-shares:some-defaults-available"
        (share/retract-share! conn "default" nil "clear defaults")
        (share/set-share! conn "default" nil "cpu and gpu defaults available" :cpus 3.0 :gpus 1.0)
        (let [db (mt/db conn)]
          (is (= {"u1" {:cpus 5.0 :mem 10.0 :gpus 1.0}
                  "u2" {:cpus 3.0 :mem Double/MAX_VALUE :gpus 1.0}
                  "u3" {:cpus 10.0 :mem 20.0 :gpus 4.0}
                  "u4" {:cpus 3.0 :mem Double/MAX_VALUE :gpus 1.0}}
                 (share/get-shares db ["u1" "u2" "u3" "u4"])))))

      (testing "create-user->share-fn"
        (let [user->share-fn (share/create-user->share-fn db nil)]
          (is (={:cpus 5.0 :mem 10.0 :gpus 1.0} (user->share-fn "u1")))
          (is (= {:cpus 5.0 :mem 10.0 :gpus 1.0} (user->share-fn "u2")))
          (is (= {:cpus 10.0 :mem 20.0 :gpus 4.0} (user->share-fn "u3")))
          (is (= {:cpus 1.0 :mem 2.0 :gpus 1.0} (user->share-fn "u4"))))))))


(deftest test-pool-support
  (let [uri "datomic:mem://test-share-pool-support"
        conn (restore-fresh-database! uri)]
    (create-pool conn "pool-1")
    (create-pool conn "pool-2")
    (create-pool conn "pool-3")
    (share/set-share! conn "default" nil "strict" :cpus 1.0 :mem 1.0 :gpus 1.0)
    (share/set-share! conn "default" "pool-1" "lenient" :cpus 10.0 :mem 10.0 :gpus 10.0)
    (share/set-share! conn "u1" nil "power user" :cpus 20.0 :mem 20.0)
    (share/set-share! conn "u1" "pool-1" "really lenient" :cpus 100.0 :mem 100.0)
    (testing "get-share, no default pool configured"
      (let [db (mt/db conn)]
        (is (= {:cpus 20.0 :mem 20.0 :gpus 1.0} (share/get-share db "u1" nil)))
        (is (= {:cpus 100.0 :mem 100.0 :gpus 10.0} (share/get-share db "u1" "pool-1")))
        (is (= {:cpus Double/MAX_VALUE :mem Double/MAX_VALUE :gpus Double/MAX_VALUE}
               (share/get-share db "u1" "pool-2")))

        (is (= {:cpus 1.0 :mem 1.0 :gpus 1.0} (share/get-share db "u2" nil)))
        (is (= {:cpus 10.0 :mem 10.0 :gpus 10.0} (share/get-share db "u2" "pool-1")))))

    (testing "get-share, default pool configured"
      (with-redefs [config/default-pool (constantly "pool-1")]
        (let [db (mt/db conn)]
          ; Should use the explicit defaults from pool-1
          (is (= {:cpus 100.0 :mem 100.0 :gpus 10.0} (share/get-share db "u1" nil)))
          (is (= {:cpus 100.0 :mem 100.0 :gpus 10.0} (share/get-share db "u1" "pool-1")))
          (is (= {:cpus Double/MAX_VALUE :mem Double/MAX_VALUE :gpus Double/MAX_VALUE}
                 (share/get-share db "u1" "pool-2")))

          (is (= {:cpus 10.0 :mem 10.0 :gpus 10.0} (share/get-share db "u2" nil)))
          (is (= {:cpus 10.0 :mem 10.0 :gpus 10.0} (share/get-share db "u2" "pool-1"))))))

    (testing "retract share, no default pool configured"
      (share/set-share! conn "u2" nil "defaults" :cpus 1.0 :mem 1.0 :gpus 1.0)
      (share/set-share! conn "u2" "pool-2" "pool-2 settings" :cpus 2.0 :mem 2.0 :gpus 2.0)
      (let [db (mt/db conn)]
        (is (= {:cpus 2.0 :mem 2.0 :gpus 2.0} (share/get-share db "u2" "pool-2"))))

      (share/retract-share! conn "u2" "pool-2" "removing share")
      (let [db (mt/db conn)]
        (is (= {:cpus 1.0 :mem 1.0 :gpus 1.0}) (share/get-share db "u2" "pool-2")))

      (share/retract-share! conn "u2" nil "removing default share")
      (let [db (mt/db conn)]
        ; With no default pool, we should get the default share values
        (is (= {:cpus Double/MAX_VALUE :mem Double/MAX_VALUE :gpus Double/MAX_VALUE}
               (share/get-share db "u2" "pool-2")))))

    (testing "retract share, default pool configured"
      (with-redefs [config/default-pool (constantly "pool-2")]
        (share/set-share! conn "u2" nil "defaults" :cpus 4.0 :mem 4.0 :gpus 4.0)
        (share/set-share! conn "u2" "pool-2" "pool-2 settings" :cpus 5.0 :mem 5.0 :gpus 5.0)
        (share/set-share! conn "u2" "pool-1" "pool-1 settings" :cpus 6.0 :mem 6.0 :gpus 6.0)
        (let [db (mt/db conn)]
          ; Should get the "u2"/pool-2 share
          (is (= {:cpus 5.0 :mem 5.0 :gpus 5.0} (share/get-share db "u2" "pool-2"))))

        (share/retract-share! conn "u2" "pool-2" "removing share")
        (let [db (mt/db conn)]
          ; We should now get the "default"/nil share since we're querying the default pool
          (is (= {:cpus 1.0 :mem 1.0 :gpus 1.0} (share/get-share db "u2" "pool-2")))

          ; pool-1 should be unaffected
          (is (= {:cpus 6.0 :mem 6.0 :gpus 6.0} (share/get-share db "u2" "pool-1"))))))

    (testing "create-user->share-fn"
      (share/set-share! conn "u1" "pool-2" "reason" :cpus 1.0 :mem 1.0 :gpus 1.0)
      (share/set-share! conn "u2" "pool-2" "reason" :cpus 1.0 :mem 1.0 :gpus 1.0)
      (share/set-share! conn "u3" "pool-2" "reason" :cpus 1.0 :mem 1.0 :gpus 1.0)
      (share/set-share! conn "default" "pool-2" "reason" :cpus 2.0 :mem 2.0 :gpus 2.0)

      (share/set-share! conn "u1" nil "reason" :cpus 5.0 :mem 5.0 :gpus 5.0)
      (share/set-share! conn "u2" nil "reason" :cpus 5.0 :mem 5.0 :gpus 5.0)
      (share/set-share! conn "u3" nil "reason" :cpus 5.0 :mem 5.0 :gpus 5.0)
      (share/set-share! conn "default" nil "reason" :cpus 6.0 :mem 6.0 :gpus 6.0)

      (let [user->share-fn (share/create-user->share-fn (mt/db conn) "pool-2")]
        (doseq [user ["u1" "u2" "u3"]]
          (is (= {:cpus 1.0 :mem 1.0 :gpus 1.0} (user->share-fn user))))
        (is (= {:cpus 2.0 :mem 2.0 :gpus 2.0} (user->share-fn "u4"))))

      (with-redefs [config/default-pool (constantly "pool-3")]
        (let [user->share-fn (share/create-user->share-fn (mt/db conn) "pool-3")]
          (doseq [user ["u1" "u2" "u3"]]
            (is (= {:cpus 5.0 :mem 5.0 :gpus 5.0} (user->share-fn user))))
          (is (= {:cpus 6.0 :mem 6.0 :gpus 6.0} (user->share-fn "u4"))))))))
