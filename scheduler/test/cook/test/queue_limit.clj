(ns cook.test.queue-limit
  (:require [clojure.test :refer :all]
            [cook.queue-limit :as queue-limit]))

(deftest test-query-queue-lengths
  (let [pool-1 {:pool/name "pool-1"}
        pool-2 {:pool/name "pool-2"}
        pending-jobs [{:job/pool pool-1 :job/user "alice"}
                      {:job/pool pool-1 :job/user "sally"}
                      {:job/pool pool-1 :job/user "alice"}
                      {:job/pool pool-2 :job/user "sally"}
                      {:job/pool pool-2 :job/user "alice"}
                      {:job/pool pool-2 :job/user "sally"}]]
    (with-redefs [queue-limit/get-pending-jobs (constantly pending-jobs)]
      (is (= {:pool->queue-length {"pool-1" 3 "pool-2" 3}
              :pool->user->queue-length {"pool-1" {"alice" 2 "sally" 1}
                                         "pool-2" {"alice" 1 "sally" 2}}}
             (queue-limit/query-queue-lengths))))))

(deftest test-inc-queue-length!
  (testing "throws on nil pool name"
    (is (thrown? AssertionError (queue-limit/inc-queue-length! nil "alice" 2)))))

(deftest test-dec-queue-length!
  (with-redefs [queue-limit/get-pending-jobs (constantly [])]
    (queue-limit/update-queue-lengths!))
  (queue-limit/inc-queue-length! "pool-1" "alice" 2)
  (queue-limit/inc-queue-length! "pool-1" "sally" 1)
  (queue-limit/inc-queue-length! "pool-2" "alice" 1)
  (queue-limit/inc-queue-length! "pool-2" "sally" 2)
  (is (= 2 (queue-limit/user-queue-length "pool-1" "alice")))
  (is (= 1 (queue-limit/user-queue-length "pool-1" "sally")))
  (is (= 1 (queue-limit/user-queue-length "pool-2" "alice")))
  (is (= 2 (queue-limit/user-queue-length "pool-2" "sally")))
  (let [pool-1 {:pool/name "pool-1"}
        pool-2 {:pool/name "pool-2"}
        pending-jobs [{:job/pool pool-1 :job/user "alice"}
                      {:job/pool pool-1 :job/user "sally"}
                      {:job/pool pool-1 :job/user "alice"}
                      {:job/pool pool-2 :job/user "sally"}
                      {:job/pool pool-2 :job/user "alice"}
                      {:job/pool pool-2 :job/user "sally"}]]
    (queue-limit/dec-queue-length! pending-jobs)
    (is (zero? (queue-limit/user-queue-length "pool-1" "alice")))
    (is (zero? (queue-limit/user-queue-length "pool-1" "sally")))
    (is (zero? (queue-limit/user-queue-length "pool-2" "alice")))
    (is (zero? (queue-limit/user-queue-length "pool-2" "sally")))
    (queue-limit/inc-queue-length! "pool-1" "alice" 4)
    (queue-limit/inc-queue-length! "pool-1" "sally" 2)
    (queue-limit/inc-queue-length! "pool-2" "alice" 2)
    (queue-limit/inc-queue-length! "pool-2" "sally" 4)
    (is (= 4 (queue-limit/user-queue-length "pool-1" "alice")))
    (is (= 2 (queue-limit/user-queue-length "pool-1" "sally")))
    (is (= 2 (queue-limit/user-queue-length "pool-2" "alice")))
    (is (= 4 (queue-limit/user-queue-length "pool-2" "sally")))
    (queue-limit/dec-queue-length! pending-jobs)
    (is (= 2 (queue-limit/user-queue-length "pool-1" "alice")))
    (is (= 1 (queue-limit/user-queue-length "pool-1" "sally")))
    (is (= 1 (queue-limit/user-queue-length "pool-2" "alice")))
    (is (= 2 (queue-limit/user-queue-length "pool-2" "sally")))
    (queue-limit/dec-queue-length! pending-jobs)
    (is (zero? (queue-limit/user-queue-length "pool-1" "alice")))
    (is (zero? (queue-limit/user-queue-length "pool-1" "sally")))
    (is (zero? (queue-limit/user-queue-length "pool-2" "alice")))
    (is (zero? (queue-limit/user-queue-length "pool-2" "sally")))
    (queue-limit/dec-queue-length! pending-jobs)
    (is (zero? (queue-limit/user-queue-length "pool-1" "alice")))
    (is (zero? (queue-limit/user-queue-length "pool-1" "sally")))
    (is (zero? (queue-limit/user-queue-length "pool-2" "alice")))
    (is (zero? (queue-limit/user-queue-length "pool-2" "sally")))))