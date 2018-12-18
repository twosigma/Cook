(ns cook.test.hooks
  (:use clojure.test)
  (:require [clj-time.core :as t]
            [clojure.string :as str]
            [cook.hooks :as hooks]
            [cook.test.testutil :refer [create-dummy-job
                                        restore-fresh-database!] :as testutil]
            [datomic.api :as d]))


(deftest test-aged-out?
  (with-redefs
    [hooks/submission-hook-batch-timeout-seconds 40 ; Self-imposed deadline to submit a batch.
     hooks/age-out-last-seen-deadline (t/minutes 10)
     hooks/age-out-first-seen-deadline (t/hours 10)
     hooks/age-out-seen-count 10
     t/now (constantly (t/date-time 2018 12 20 23 10))]
    (let

      [t-5m  (t/date-time 2018 12 20 23 5)
       t-1h  (t/date-time 2018 12 20 22 10)
       t-9h  (t/date-time 2018 12 20 14 10)
       t-10h5m  (t/date-time 2018 12 20 13 5)]

      ; Meets all of the thresholds to be aged out:
      (is (true? (hooks/aged-out? {:last-seen t-5m
                                   :first-seen t-10h5m
                                   :seen-count 12})))

      ; None of these should be aged out...
      (is (false? (hooks/aged-out? {:last-seen t-5m
                                    :first-seen t-10h5m
                                    :seen-count 9})) "Not seen often enough.")
      (is (false? (hooks/aged-out? {:last-seen t-5m
                                    :first-seen t-9h
                                    :seen-count 12})) "Not first seen at least 9 hours ago.")
      (is (false? (hooks/aged-out? {:last-seen t-1h
                                    :first-seen t-10h5m
                                    :seen-count 12})) "Not last seen recently enough"))))

(deftest filter-job-invocation-miss
  (let [uri "datomic:mem://test-filter-job-invocation-miss"
        conn (restore-fresh-database! uri)]
    (testing "When aged out, we keep the job."
      (with-redefs [hooks/aged-out? (constantly true)
                    hooks/hook-object
                    (reify hooks/SchedulerHooks
                      (check-job-submission-default [_])
                      (check-job-submission [_ _])
                      (check-job-invocation [_ _] (is false "Shouldn't be invoked.")))]
        (is (true? (hooks/filter-job-invocations-miss (testutil/create-dummy-job conn))))))))



(deftest test-filter-job-invocations
  (testing "empty matched jobs"
    (let [uri "datomic:mem://test-filter-job-invocations"
          conn (restore-fresh-database! uri)
          test-db (d/db conn)
          ;get-uuid (fn [name] (get job-name->uuid name (d/squuid)))
          job-1 (d/entity test-db (create-dummy-job conn))
          job-2 (d/entity test-db (create-dummy-job conn))
          job-3 (d/entity test-db (create-dummy-job conn))]
      (with-redefs [hooks/hook-object testutil/fake-scheduler-hooks]
        ;(hooks/filter-job-invocations-miss [job-1 job-2 job-3])
        ; TODO
        ))))



