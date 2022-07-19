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
(ns cook.test.monitor
  (:require [clj-time.core :as time]
            [clojure.test :refer :all]
            [cook.mesos :as mesos]
            [cook.monitor :as monitor]
            [cook.queries :as queries]
            [cook.quota :as quota]
            [cook.rest.api :as api]
            [cook.scheduler.share :as share]
            [cook.test.testutil :as testutil :refer [restore-fresh-database! setup]]
            [cook.tools :as util]
            [datomic.api :as d :refer [db]]
            [metrics.counters :as counters])
  (:import (java.util UUID)
           (org.joda.time Interval)))


(def config-with-quota-group
  {:quota-grouping {"pool1" "accum" "pool2" "accum"}})

(defn create-job-in-pool
  [conn pool jobs]
  (testutil/create-jobs! conn {::api/job-pool-name-maps
                               (map (fn [x] {:job x :pool-name pool}) jobs)}))

(deftest test-set-stats-counters!
  (setup :config config-with-quota-group)

  ; Clear all metrics.
  (let [metrics-registry metrics.core/default-registry
        metrics (.getMetrics metrics-registry)]
    (doseq [metric (keys metrics)]
      (.remove metrics-registry metric)))

  (let [conn (restore-fresh-database! "datomic:mem://test-set-stats-counters!")
        counter #(counters/value (counters/counter (conj % "pool-pool1")))
        counter-helper (fn [pool key] (counters/value (counters/counter (conj key (str "pool-" pool)))))
        counters (fn [pool k1 k2] (into [] (map #(counter-helper pool (vector k1 k2 %)) ["jobs" "cpus" "mem"])))
        job1 {:uuid (UUID/randomUUID), :command "ls", :max-retries 1, :cpus 1., :mem 128., :user "alice"}
        job2 {:uuid (UUID/randomUUID), :command "ls", :max-retries 1, :cpus 2., :mem 256., :user "bob"}
        job3 {:uuid (UUID/randomUUID), :command "ls", :max-retries 1, :cpus 4., :mem (double Long/MAX_VALUE), :user "sally"}
        stats-atom (atom {})
        set-stats-counters-for-pool (fn [pool] (monitor/set-stats-counters! (db conn) stats-atom
                                                                            (queries/get-pending-job-ents (db conn))
                                                                            (util/get-running-job-ents (db conn))
                                                                            pool))
        run-job (fn [job] @(d/transact conn [[:db/add [:job/uuid (:uuid job)] :job/state :job.state/running]]))]

    (testutil/create-pool conn "pool1")
    (set-stats-counters-for-pool "pool1")

    (is (= [0 0 0] (counters "pool1" "running" "all")))
    (is (= [0 0 0] (counters "pool1" "starved" "all")))
    (is (= [0 0 0] (counters "pool1" "waiting-under-quota" "all")))
    (is (= [0 0 0] (counters "pool1" "waiting" "all")))

    (is (= 0 (counter ["total" "users"])))
    (is (= 0 (counter ["starved" "users"])))
    (is (= 0 (counter ["waiting-under-quota" "users"])))
    (is (= 0 (counter ["hungry" "users"])))
    (is (= 0 (counter ["satisfied" "users"])))

    (create-job-in-pool conn "pool1" [job1 job2])

    (set-stats-counters-for-pool "pool1")

    (is (= [0 0 0] (counters "pool1" "running" "all")))
    (is (= [2 3 384] (counters "pool1" "waiting" "all")))
    (is (= [1 1 128] (counters "pool1" "waiting" "alice")))
    (is (= [1 2 256] (counters "pool1" "waiting" "bob")))
    (is (= [2 3 384] (counters "pool1" "starved" "all")))
    (is (= [1 1 128] (counters "pool1" "starved" "alice")))
    (is (= [1 2 256] (counters "pool1" "starved" "bob")))
    (is (= [2 3 384] (counters "pool1" "waiting-under-quota" "all")))
    (is (= [1 1 128] (counters "pool1" "waiting-under-quota" "alice")))
    (is (= [1 2 256] (counters "pool1" "waiting-under-quota" "bob")))

    (is (= 2 (counter ["total" "users"])))
    (is (= 2 (counter ["starved" "users"])))
    (is (= 2 (counter ["waiting-under-quota" "users"])))
    (is (= 0 (counter ["hungry" "users"])))
    (is (= 0 (counter ["satisfied" "users"])))


    (testing "Do we accumulate to accum?"
      (set-stats-counters-for-pool "accum")
      (is (= [0 0 0] (counters "accum" "running" "all")))
      (is (= [2 3 384] (counters "accum" "waiting" "all")))
      (is (= [1 1 128] (counters "accum" "waiting" "alice")))
      (is (= [1 2 256] (counters "accum" "waiting" "bob")))
      (is (= [2 3 384] (counters "accum" "starved" "all")))
      (is (= [1 1 128] (counters "accum" "starved" "alice")))
      (is (= [1 2 256] (counters "accum" "starved" "bob")))
      (is (= [2 3 384] (counters "accum" "waiting-under-quota" "all")))
      (is (= [1 1 128] (counters "accum" "waiting-under-quota" "alice")))
      (is (= [1 2 256] (counters "accum" "waiting-under-quota" "bob"))))

    (testing "Mark alice's job as running"
      (run-job job1)
      (set-stats-counters-for-pool "pool1")
      (is (= [1 1 128] (counters "pool1" "running" "all")))
      (is (= [1 1 128] (counters "pool1" "running" "alice")))
      (is (= [0 0 0] (counters "pool1" "running" "bob")))
      (is (= [1 2 256] (counters "pool1" "waiting" "all")))
      (is (= [0 0 0] (counters "pool1" "waiting" "alice")))
      (is (= [1 2 256] (counters "pool1" "waiting" "bob")))
      (is (= [1 2 256] (counters "pool1" "starved" "all")))
      (is (= [0 0 0] (counters "pool1" "starved" "alice")))
      (is (= [1 2 256] (counters "pool1" "starved" "bob")))
      (is (= [1 2 256] (counters "pool1" "waiting-under-quota" "all")))
      (is (= [0 0 0] (counters "pool1" "waiting-under-quota" "alice")))
      (is (= [1 2 256] (counters "pool1" "waiting-under-quota" "bob")))
      (is (= 2 (counter ["total" "users"])))
      (is (= 1 (counter ["starved" "users"])))
      (is (= 1 (counter ["waiting-under-quota" "users"])))
      (is (= 0 (counter ["hungry" "users"])))
      (is (= 1 (counter ["satisfied" "users"]))))

    (testing "Kill running job"
      (mesos/kill-job conn [(:uuid job1)])
      (set-stats-counters-for-pool "pool1")
      (is (= [0 0 0] (counters "pool1" "running" "all")))
      (is (= [0 0 0] (counters "pool1" "running" "alice")))
      (is (= [0 0 0] (counters "pool1" "running" "bob")))
      (is (= [1 2 256] (counters "pool1" "waiting" "all")))
      (is (= [0 0 0] (counters "pool1" "waiting" "alice")))
      (is (= [1 2 256] (counters "pool1" "waiting" "bob")))
      (is (= [1 2 256] (counters "pool1" "starved" "all")))
      (is (= [0 0 0] (counters "pool1" "starved" "alice")))
      (is (= [1 2 256] (counters "pool1" "starved" "bob")))
      (is (= [1 2 256] (counters "pool1" "waiting-under-quota" "all")))
      (is (= [0 0 0] (counters "pool1" "waiting-under-quota" "alice")))
      (is (= [1 2 256] (counters "pool1" "waiting-under-quota" "bob")))
      (is (= 1 (counter ["total" "users"])))
      (is (= 1 (counter ["starved" "users"])))
      (is (= 0 (counter ["hungry" "users"])))
      (is (= 0 (counter ["satisfied" "users"]))))

    (testing "Run bob's job"
      (run-job job2)
      (set-stats-counters-for-pool "pool1")
      (is (= [1 2 256] (counters "pool1" "running" "all")))
      (is (= [0 0 0] (counters "pool1" "running" "alice")))
      (is (= [1 2 256] (counters "pool1" "running" "bob")))
      (is (= [0 0 0] (counters "pool1" "waiting" "all")))
      (is (= [0 0 0] (counters "pool1" "waiting" "alice")))
      (is (= [0 0 0] (counters "pool1" "waiting" "bob")))
      (is (= [0 0 0] (counters "pool1" "starved" "all")))
      (is (= [0 0 0] (counters "pool1" "starved" "alice")))
      (is (= [0 0 0] (counters "pool1" "starved" "bob")))
      (is (= [0 0 0] (counters "pool1" "waiting-under-quota" "all")))
      (is (= [0 0 0] (counters "pool1" "waiting-under-quota" "alice")))
      (is (= [0 0 0] (counters "pool1" "waiting-under-quota" "bob")))

      (is (= 1 (counter ["total" "users"])))
      (is (= 0 (counter ["starved" "users"])))
      (is (= 0 (counter ["hungry" "users"])))
      (is (= 1 (counter ["satisfied" "users"]))))

    (testing "Kill Bob's job"
      (mesos/kill-job conn [(:uuid job2)])
      (set-stats-counters-for-pool "pool1")
      (is (= [0 0 0] (counters "pool1" "running" "all")))
      (is (= [0 0 0] (counters "pool1" "running" "alice")))
      (is (= [0 0 0] (counters "pool1" "running" "bob")))
      (is (= [0 0 0] (counters "pool1" "waiting" "all")))
      (is (= [0 0 0] (counters "pool1" "waiting" "alice")))
      (is (= [0 0 0] (counters "pool1" "waiting" "bob")))
      (is (= [0 0 0] (counters "pool1" "starved" "all")))
      (is (= [0 0 0] (counters "pool1" "starved" "alice")))
      (is (= [0 0 0] (counters "pool1" "starved" "bob")))
      (is (= [0 0 0] (counters "pool1" "waiting-under-quota" "all")))
      (is (= [0 0 0] (counters "pool1" "waiting-under-quota" "alice")))
      (is (= [0 0 0] (counters "pool1" "waiting-under-quota" "bob")))

      (is (= 0 (counter ["total" "users"])))
      (is (= 0 (counter ["starved" "users"])))
      (is (= 0 (counter ["hungry" "users"])))
      (is (= 0 (counter ["satisfied" "users"]))))

    (testing "Does waiting under qutoa obey quota?"
      (create-job-in-pool conn "pool1" [job3])
      (with-redefs [share/get-share (constantly {:cpus 0 :mem 0})
                    quota/get-quota (constantly {:mem 10, :cpus 10, :gpus 10, :count 10, :launch-rate-saved 1.0000097E7, :launch-rate-per-minute 600013.0})]
        (set-stats-counters-for-pool "pool1"))

      (is (= [0 0 0] (counters "pool1" "running" "all")))
      (is (= [0 0 0] (counters "pool1" "running" "alice")))
      (is (= [0 0 0] (counters "pool1" "running" "bob")))
      (is (= [0 0 0] (counters "pool1" "running" "sally")))
      (is (= [1 4 Long/MAX_VALUE] (counters "pool1" "waiting" "all")))
      (is (= [0 0 0] (counters "pool1" "waiting" "alice")))
      (is (= [0 0 0] (counters "pool1" "waiting" "bob")))
      (is (= [1 4 Long/MAX_VALUE] (counters "pool1" "waiting" "sally")))
      (is (= [0 0 0] (counters "pool1" "starved" "all")))
      (is (= [0 0 0] (counters "pool1" "starved" "alice")))
      (is (= [0 0 0] (counters "pool1" "starved" "bob")))
      (is (= [0 0 0] (counters "pool1" "starved" "sally")))
      (is (= [1 4 10] (counters "pool1" "waiting-under-quota" "all")))
      (is (= [0 0 0] (counters "pool1" "waiting-under-quota" "alice")))
      (is (= [0 0 0] (counters "pool1" "waiting-under-quota" "bob")))
      (is (= [1 4 10] (counters "pool1" "waiting-under-quota" "sally")))

      (is (= 1 (counter ["total" "users"])))
      (is (= 0 (counter ["starved" "users"])))
      (is (= 1 (counter ["waiting-under-quota" "users"])))
      (is (= 1 (counter ["hungry" "users"])))
      (is (= 0 (counter ["satisfied" "users"])))

      (run-job job3)
      (set-stats-counters-for-pool "pool1")
      (is (= [1 4 Long/MAX_VALUE] (counters "pool1" "running" "all")))
      (is (= [0 0 0] (counters "pool1" "running" "alice")))
      (is (= [0 0 0] (counters "pool1" "running" "bob")))
      (is (= [1 4 Long/MAX_VALUE] (counters "pool1" "running" "sally")))
      (is (= [0 0 0] (counters "pool1" "waiting" "all")))
      (is (= [0 0 0] (counters "pool1" "waiting" "alice")))
      (is (= [0 0 0] (counters "pool1" "waiting" "bob")))
      (is (= [0 0 0] (counters "pool1" "waiting" "sally")))
      (is (= [0 0 0] (counters "pool1" "starved" "all")))
      (is (= [0 0 0] (counters "pool1" "starved" "alice")))
      (is (= [0 0 0] (counters "pool1" "starved" "bob")))
      (is (= [0 0 0] (counters "pool1" "starved" "sally")))
      (is (= [0 0 0] (counters "pool1" "waiting-under-quota" "all")))
      (is (= [0 0 0] (counters "pool1" "waiting-under-quota" "alice")))
      (is (= [0 0 0] (counters "pool1" "waiting-under-quota" "bob")))
      (is (= [0 0 0] (counters "pool1" "waiting-under-quota" "sally")))

      (is (= 1 (counter ["total" "users"])))
      (is (= 0 (counter ["starved" "users"])))
      (is (= 0 (counter ["hungry" "users"])))
      (is (= 1 (counter ["satisfied" "users"])))
      ; Clean up. Kill Sally's job so it doesn't cause later global aggregate metrics
      (mesos/kill-job conn [(:uuid job3)]))

    (testing "Testing aggregates across several jobs by the same user for waiting-under-quota"
      (let [job4 {:uuid (UUID/randomUUID), :command "ls", :max-retries 1, :cpus 2., :mem 3., :user "user"}
            job5 {:uuid (UUID/randomUUID), :command "ls", :max-retries 1, :cpus 5., :mem 8., :user "user"}
            job6 {:uuid (UUID/randomUUID), :command "ls", :max-retries 1, :cpus 13., :mem 21., :user "user"}]

        (testutil/create-pool conn "pool2")
        (create-job-in-pool conn "pool1" [job4 job5 job6])
        (with-redefs [quota/get-quota (constantly {:mem 10, :cpus 100, :gpus 10, :count 10, :launch-rate-saved 10, :launch-rate-per-minute 10})]
          (set-stats-counters-for-pool "pool1")
          (is (= [3 20 10] (counters "pool1" "waiting-under-quota" "user")))

          (run-job job4)
          (set-stats-counters-for-pool "pool1")
          (is (= [2 18 7] (counters "pool1" "waiting-under-quota" "user")))

          (run-job job5)
          (set-stats-counters-for-pool "pool1")
          (is (= [0 0 0] (counters "pool1" "waiting-under-quota" "user"))))
      ; Cleanup so the next test doesn't see these spurious jobs.
      (mesos/kill-job conn [(:uuid job4) (:uuid job5) (:uuid job6)])))

    (testing "Testing aggregates quota over several pools by the same user"
      (let [job7 {:uuid (UUID/randomUUID), :command "ls", :max-retries 1, :cpus 2., :mem 30., :user "user"}
            job8 {:uuid (UUID/randomUUID), :command "ls", :max-retries 1, :cpus 5., :mem 80., :user "user"}
            job9 {:uuid (UUID/randomUUID), :command "ls", :max-retries 1, :cpus 13., :mem 210., :user "user"}]
        (create-job-in-pool conn "pool1" [job7 job8])
        (create-job-in-pool conn "pool2" [job9])

        (set-stats-counters-for-pool "pool1")
        (set-stats-counters-for-pool "pool2")
        (set-stats-counters-for-pool "accum")
        (is (= [2 7 110] (counters "pool1" "waiting" "user")))
        (is (= [1 13 210] (counters "pool2" "waiting" "user")))
        (is (= [3 20 320] (counters "accum" "waiting" "user")))
        (is (= [2 7 110] (counters "pool1" "waiting" "all")))
        (is (= [1 13 210] (counters "pool2" "waiting" "all")))
        (is (= [3 20 320] (counters "accum" "waiting" "all")))

        (run-job job7)
        (run-job job9)
        (set-stats-counters-for-pool "pool1")
        (set-stats-counters-for-pool "pool2")
        (set-stats-counters-for-pool "accum")
        (is (= [1 2 30] (counters "pool1" "running" "user")))
        (is (= [1 13 210] (counters "pool2" "running" "user")))
        (is (= [2 15 240] (counters "accum" "running" "user")))
        (is (= [1 2 30] (counters "pool1" "running" "all")))
        (is (= [1 13 210] (counters "pool2" "running" "all")))
        (is (= [2 15 240] (counters "accum" "running" "all")))

        (is (= [1 5 80] (counters "pool1" "waiting" "user")))
        (is (= [0 0 0] (counters "pool2" "waiting" "user")))
        (is (= [1 5 80] (counters "accum" "waiting" "user")))
        (is (= [1 5 80] (counters "pool1" "waiting" "all")))
        (is (= [0 0 0] (counters "pool2" "waiting" "all")))
        (is (= [1 5 80] (counters "accum" "waiting" "all")))

    ))))


(deftest test-start-collecting-stats
  (setup :config {:metrics {:user-metrics-interval-seconds 1}})
  (let [period (atom nil)
        millis-between (fn [a b]
                         (-> a
                             ^Interval (time/interval b)
                             .toDuration
                             .getMillis))]
    (with-redefs [chime/chime-at (fn [p _ _] (reset! period p))]
      (monitor/start-collecting-stats)
      (is (= 1000 (millis-between (first @period) (second @period))))
      (is (= 1000 (millis-between (second @period) (nth @period 2))))
      (is (= 1000 (millis-between (nth @period 2) (nth @period 3)))))))
