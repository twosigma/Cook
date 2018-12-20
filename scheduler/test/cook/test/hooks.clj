(ns cook.test.hooks
  (:use clojure.test)
  (:require [clj-time.core :as t]
            [clojure.string :as str]
            [clojure.tools.logging :as log]

            [cook.cache :as ccache]
            [cook.hooks :as hooks]
            [cook.mesos.util :as util]

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
    (testing "When aged out, we keep the job and don't call the backend."
      (with-redefs [hooks/aged-out? (constantly true)
                    hooks/hook-object
                    (reify hooks/SchedulerHooks
                      (check-job-submission-default [_])
                      (check-job-submission [_ _])
                      (check-job-invocation [_ _] (is false "Shouldn't be invoked.")))]
        (is (true? (hooks/filter-job-invocations-miss (testutil/create-dummy-job conn))))))

    (let [job-entid (create-dummy-job conn)
          job-entity (d/entity (d/db conn) job-entid)
          job (util/job-ent->map job-entity)]
      (with-redefs [hooks/aged-out? (constantly false)]
        (with-redefs [hooks/hook-object testutil/accept-defer-hook
                      t/now (constantly (t/date-time 2018 12 20 13 5))]
          (is false (hooks/filter-job-invocations-miss job))
          (is false (hooks/filter-job-invocations-miss job))
          (is false (hooks/filter-job-invocations-miss job))
          (with-redefs [hooks/hook-object testutil/accept-defer-hook
                        t/now (constantly (t/date-time 2018 12 20 15 5))]
            (is false (:state (hooks/filter-job-invocations-miss job))))
          ;; Now, we should see this job in the cache with 4 visits, and last-seen and most recently seen updated appropriately.
          (testing "Submit job several times, correctly updates timestamps as long as its found."
            (let [{:keys [first-seen last-seen seen-count]} (ccache/get-if-present hooks/job-invocations-cache :job/uuid job)]
              (is (= last-seen (t/date-time 2018 12 20 15 5)))
              (is (= first-seen (t/date-time 2018 12 20 13 5)))
              (is (= seen-count 4)))))
        (with-redefs [hooks/hook-object testutil/accept-accept-hook
                      t/now (constantly (t/date-time 2018 12 20 23 10))]
          (testing "Job moves into the accepted state as appropriate."
            (is true (:state (hooks/filter-job-invocations-miss job)))))))))

(deftest filter-job-invocation-miss
  (let [uri "datomic:mem://test-filter-job-invocation"
        conn (restore-fresh-database! uri)
        visited (atom false)
        job-entid (create-dummy-job conn)
        job-entity (d/entity (d/db conn) job-entid)
        job (util/job-ent->map job-entity)]
    (testing "On a cache miss, we always query, and also return true if the status is accepted."
      (with-redefs [ccache/get-if-present (constantly nil)
                    hooks/filter-job-invocations-miss
                    (fn [_]
                      (reset! visited true)
                      true)]
        (is (true? (hooks/filter-job-invocations job)))
        (is (true? @visited)))
      (reset! visited false))
    ;; Now, the old entry expires a second ago, so we can re-use the existing job!

    (testing "On a cache miss, we always query, and also return true if the status is rejected"
      (with-redefs [ccache/get-if-present (constantly nil)
                    hooks/filter-job-invocations-miss
                    (fn [_]
                      (reset! visited true)
                      false)]
        (is (false? (hooks/filter-job-invocations job)))
        (is (true? @visited)))
      (reset! visited false))

    (testing "On an expired cache hit, we always query."
      (with-redefs [ccache/get-if-present (constantly {:status :accepted :cache-expires-at (-> -1 t/seconds t/from-now)})
                    hooks/filter-job-invocations-miss
                    (fn [_]
                      (reset! visited true)
                      false)]
        (is (false? (hooks/filter-job-invocations job)))
        (is (true? @visited)))
      (reset! visited false))

    (testing "On an good cache hit, we don't query."
      (with-redefs [ccache/get-if-present (constantly {:status :accepted :cache-expires-at (-> 10 t/seconds t/from-now)})
                    hooks/filter-job-invocations-miss
                    (fn [_]
                      (reset! visited true)
                      false)]
        (is (true? (hooks/filter-job-invocations job)))
        (is (false? @visited)))
      (reset! visited false))


    (testing "Use the cache integration, run twice, putting it in the real cache."
      (with-redefs [hooks/hook-object (reify hooks/SchedulerHooks
                                        (check-job-submission-default [_])
                                        (check-job-submission [_ _])
                                        (check-job-invocation [_ _]
                                          (reset! visited true)
                                          {:status :deferred :cache-expires-at (-> 10 t/seconds t/from-now)}))]
        (is (false? (hooks/filter-job-invocations job)))
        (is (true? @visited))
        (reset! visited false)
        (is (false? (hooks/filter-job-invocations job)))
        (is (false? @visited))))))
