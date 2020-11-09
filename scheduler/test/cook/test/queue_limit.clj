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
  ; Clear the queues by mocking get-pending-jobs
  (with-redefs [queue-limit/get-pending-jobs (constantly [])]
    (queue-limit/update-queue-lengths!))

  ; Add some jobs to the queues
  (is (= {:pool->queue-length {"pool-1" 2}
          :pool->user->queue-length {"pool-1" {"alice" 2}}}
         (queue-limit/inc-queue-length! "pool-1" "alice" 2)))
  (is (= {:pool->queue-length {"pool-1" 3}
          :pool->user->queue-length {"pool-1" {"alice" 2
                                               "sally" 1}}}
         (queue-limit/inc-queue-length! "pool-1" "sally" 1)))
  (is (= {:pool->queue-length {"pool-1" 3
                               "pool-2" 1}
          :pool->user->queue-length {"pool-1" {"alice" 2
                                               "sally" 1}
                                     "pool-2" {"alice" 1}}}
         (queue-limit/inc-queue-length! "pool-2" "alice" 1)))
  (is (= {:pool->queue-length {"pool-1" 3
                               "pool-2" 3}
          :pool->user->queue-length {"pool-1" {"alice" 2
                                               "sally" 1}
                                     "pool-2" {"alice" 1
                                               "sally" 2}}}
         (queue-limit/inc-queue-length! "pool-2" "sally" 2)))

  ; Remove all jobs from the queue
  (let [pool-1 {:pool/name "pool-1"}
        pool-2 {:pool/name "pool-2"}
        pending-jobs [{:job/pool pool-1 :job/user "alice"}
                      {:job/pool pool-1 :job/user "sally"}
                      {:job/pool pool-1 :job/user "alice"}
                      {:job/pool pool-2 :job/user "sally"}
                      {:job/pool pool-2 :job/user "alice"}
                      {:job/pool pool-2 :job/user "sally"}]]
    (is (= {:pool->queue-length {"pool-1" 0
                                 "pool-2" 0}
            :pool->user->queue-length {"pool-1" {"alice" 0
                                                 "sally" 0}
                                       "pool-2" {"alice" 0
                                                 "sally" 0}}}
           (queue-limit/dec-queue-length! pending-jobs)))

    ; Add more jobs to the queue
    (is (= {:pool->queue-length {"pool-1" 4
                                 "pool-2" 0}
            :pool->user->queue-length {"pool-1" {"alice" 4
                                                 "sally" 0}
                                       "pool-2" {"alice" 0
                                                 "sally" 0}}}
           (queue-limit/inc-queue-length! "pool-1" "alice" 4)))
    (is (= {:pool->queue-length {"pool-1" 6
                                 "pool-2" 0}
            :pool->user->queue-length {"pool-1" {"alice" 4
                                                 "sally" 2}
                                       "pool-2" {"alice" 0
                                                 "sally" 0}}}
           (queue-limit/inc-queue-length! "pool-1" "sally" 2)))
    (is (= {:pool->queue-length {"pool-1" 6
                                 "pool-2" 2}
            :pool->user->queue-length {"pool-1" {"alice" 4
                                                 "sally" 2}
                                       "pool-2" {"alice" 2
                                                 "sally" 0}}}
           (queue-limit/inc-queue-length! "pool-2" "alice" 2)))
    (is (= {:pool->queue-length {"pool-1" 6
                                 "pool-2" 6}
            :pool->user->queue-length {"pool-1" {"alice" 4
                                                 "sally" 2}
                                       "pool-2" {"alice" 2
                                                 "sally" 4}}}
           (queue-limit/inc-queue-length! "pool-2" "sally" 4)))

    ; Remove some, but not all, jobs from the queue
    (is (= {:pool->queue-length {"pool-1" 3
                                 "pool-2" 3}
            :pool->user->queue-length {"pool-1" {"alice" 2
                                                 "sally" 1}
                                       "pool-2" {"alice" 1
                                                 "sally" 2}}}
           (queue-limit/dec-queue-length! pending-jobs)))

    ; Remove all jobs from the queue
    (is (= {:pool->queue-length {"pool-1" 0
                                 "pool-2" 0}
            :pool->user->queue-length {"pool-1" {"alice" 0
                                                 "sally" 0}
                                       "pool-2" {"alice" 0
                                                 "sally" 0}}}
           (queue-limit/dec-queue-length! pending-jobs)))

    ; Simulate more job kills coming in, and
    ; assert that the counts don't go negative
    (is (= {:pool->queue-length {"pool-1" 0
                                 "pool-2" 0}
            :pool->user->queue-length {"pool-1" {"alice" 0
                                                 "sally" 0}
                                       "pool-2" {"alice" 0
                                                 "sally" 0}}}
           (queue-limit/dec-queue-length! pending-jobs)))))