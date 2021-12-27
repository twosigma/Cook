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
(ns cook.test.plugins
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [cook.cache :as ccache]
            [cook.config :as config]
            [cook.plugins.definitions :as chd]
            [cook.plugins.launch :as launch-plugin]
            [cook.test.postgres]
            [cook.test.testutil :as testutil :refer [create-dummy-job restore-fresh-database!]]
            [cook.tools :as util]
            [datomic.api :as d]))

(use-fixtures :once cook.test.postgres/with-pg-db)

(deftest test-aged-out?
  (with-redefs
    [config/age-out-last-seen-deadline-minutes-config (constantly (t/minutes 10))
     config/age-out-first-seen-deadline-minutes-config (constantly (t/hours 10))
     config/age-out-seen-count-config (constantly 10)
     t/now (constantly (t/date-time 2018 12 20 23 10))]
    (let [t-5m  (t/date-time 2018 12 20 23 5)
          t-1h  (t/date-time 2018 12 20 22 10)
          t-9h  (t/date-time 2018 12 20 14 10)
          t-10h5m  (t/date-time 2018 12 20 13 5)]
      ; Meets all of the thresholds to be aged out:
      (is (true? (launch-plugin/aged-out? {:last-seen t-5m
                                   :first-seen t-10h5m
                                   :seen-count 12})))

      ; None of these should be aged out...
      (is (false? (launch-plugin/aged-out? {:last-seen t-5m
                                    :first-seen t-10h5m
                                    :seen-count 9})) "Not seen often enough.")
      (is (false? (launch-plugin/aged-out? {:last-seen t-5m
                                    :first-seen t-9h
                                    :seen-count 12})) "Not first seen at least 9 hours ago.")
      (is (false? (launch-plugin/aged-out? {:last-seen t-1h
                                    :first-seen t-10h5m
                                    :seen-count 12})) "Not last seen recently enough"))))

(deftest filter-job-launch-miss
  (let [uri "datomic:mem://test-filter-job-launch-miss"
        conn (restore-fresh-database! uri)]
    (testing "When aged out, we keep the job and don't call the backend."
      (with-redefs [launch-plugin/aged-out? (constantly true)
                    launch-plugin/plugin-object
                    (reify chd/JobLaunchFilter
                      (chd/check-job-launch [_ _] (is false "Shouldn't be invoked.")))]
        (is :accepted (:status launch-plugin/get-filter-status-miss (testutil/create-dummy-job conn)))))

    (let [job-entid (create-dummy-job conn)
          job-entity (d/entity (d/db conn) job-entid)
          job (util/job-ent->map job-entity)]
      (with-redefs [launch-plugin/aged-out? (constantly false)]

        ;; Make the job miss in the cache 3 times.
        (with-redefs [launch-plugin/plugin-object testutil/defer-launch-plugin
                      t/now (constantly (t/date-time 2018 12 20 13 5))]
          (is :deferred (:status (launch-plugin/get-filter-status-miss job)))
          (is :deferred (:status (launch-plugin/get-filter-status-miss job)))
          (is :deferred (:status (launch-plugin/get-filter-status-miss job))))

        ;; Make it miss one more time,
        (with-redefs [launch-plugin/plugin-object testutil/defer-launch-plugin
                      t/now (constantly (t/date-time 2018 12 20 15 5))]
          (is :deferred (:status (launch-plugin/get-filter-status-miss job))))

        ;; Now, we should see this job in the cache with 4 visits.
        ;; last-seen and most recently seen updated appropriately.
        (testing "Submit job several times, correctly updates timestamps as long as its found."
          (let [{:keys [first-seen last-seen seen-count]} (ccache/get-if-present launch-plugin/job-launch-cache :job/uuid job)]
            (is (= last-seen (t/date-time 2018 12 20 15 5)))
            (is (= first-seen (t/date-time 2018 12 20 13 5)))
            (is (= seen-count 4))))

        ;; Now, run again a bit later, and we should see the job go into the accept state
        (with-redefs [launch-plugin/plugin-object testutil/accept-launch-plugin
                      t/now (constantly (t/date-time 2018 12 20 23 10))]
          (testing "Job moves into the accepted state as appropriate."
            (is :accepted (:status (launch-plugin/get-filter-status-miss job)))))))))

(deftest filter-job-launch-miss
  (let [uri "datomic:mem://test-filter-job-launch"
        conn (restore-fresh-database! uri)
        visited (atom false)
        job-entid (create-dummy-job conn)
        job-entity (d/entity (d/db conn) job-entid)
        job (util/job-ent->map job-entity)]
    (testing "On a cache miss, we always query, and also return true if the status is accepted."
      (with-redefs [ccache/get-if-present (constantly nil)
                    launch-plugin/get-filter-status-miss
                    (fn [_]
                      (reset! visited true)
                      {:status :accepted})]
        (is (true? (launch-plugin/filter-job-launches job)))
        (is (true? @visited)))
      (reset! visited false))
    ;; Now, the old entry expires a second ago, so we can re-use the existing job!

    (testing "On a cache miss, we always query, and also return true if the status is rejected"
      (with-redefs [ccache/get-if-present (constantly nil)
                    launch-plugin/get-filter-status-miss
                    (fn [_]
                      (reset! visited true)
                      {:status :deferred})]
        (is (false? (launch-plugin/filter-job-launches job)))
        (is (true? @visited)))
      (reset! visited false))

    (testing "On an expired cache hit, we always query."
      (with-redefs [ccache/get-if-present (constantly {:status :accepted :cache-expires-at (-> -1 t/seconds t/from-now)})
                    launch-plugin/get-filter-status-miss
                    (fn [_]
                      (reset! visited true)
                      {:status :deferred})]
        (is (false? (launch-plugin/filter-job-launches job)))
        (is (true? @visited)))
      (reset! visited false))

    (testing "On an good cache hit, we don't query."
      (with-redefs [ccache/get-if-present (constantly {:status :accepted :cache-expires-at (-> 10 t/seconds t/from-now)})
                    launch-plugin/get-filter-status-miss
                    (fn [_]
                      (reset! visited true)
                      {:status :deferred})]
        (is (true? (launch-plugin/filter-job-launches job)))
        (is (false? @visited)))
      (reset! visited false))

    (testing "Use the cache integration, run twice. Make sure it is cached."
      ; Note that above tests mock out ccache/get-if-present and thus don't actually use the guava cache.
      ; This one doesn't mock it and thus uses the real cache.
      (with-redefs [launch-plugin/plugin-object (reify chd/JobLaunchFilter
                                        (chd/check-job-launch [_ _]
                                          (reset! visited true)
                                          {:status :deferred :cache-expires-at (-> 10 t/seconds t/from-now)}))]
        (is (false? (launch-plugin/filter-job-launches job)))
        (is (true? @visited))
        (reset! visited false)
        (is (false? (launch-plugin/filter-job-launches job)))
        (is (false? @visited))))))
