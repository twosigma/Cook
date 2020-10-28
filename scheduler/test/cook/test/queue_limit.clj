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
