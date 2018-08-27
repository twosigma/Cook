(ns cook.test.mesos.data-locality
  (:use clojure.test)
  (:require [cook.config :as config]
            [cook.mesos.data-locality :as dl]
            [cook.test.testutil :refer (restore-fresh-database! create-dummy-job)]
            [datomic.api :as d]
            [plumbing.core :as pc])
  (:import java.util.UUID
           [com.netflix.fenzo TaskRequest
            VMTaskFitnessCalculator
            VirtualMachineCurrentState]))

(deftest test-update-data-local-costs
  (testing "sanitizes input costs"
    (let [job-1 (str (UUID/randomUUID))
          job-2 (str (UUID/randomUUID))]
      (dl/reset-data-local-costs!)
      (dl/update-data-local-costs {job-1 {"hostA" 2.0
                                          "hostB" 0.2
                                          "hostC" -20}
                                   job-2 {"hostB" 10.0}}
                                  [])
      (is (= {job-1 {"hostA" 1.0
                     "hostB" 0.2
                     "hostC" 0}
              job-2 {"hostB" 1.0}}
             (dl/get-data-local-costs)))))

  (testing "correctly updates existing values"
    (let [job-1 (UUID/randomUUID)
          job-2 (UUID/randomUUID)
          job-3 (UUID/randomUUID)
          job-4 (UUID/randomUUID)]
      (dl/reset-data-local-costs!)
      (dl/update-data-local-costs {job-1 {"hostA" 100}
                                   job-2 {"hostB" 200}
                                   job-4 {"hostC" 300}}
                                  [])
      (dl/update-data-local-costs {job-3 {"hostB" 300}
                                   job-4 {"hostA" 200}}
                                  [job-2])
      (is (= {job-1 {"hostA" 100}
              job-3 {"hostB" 300}
              job-4 {"hostA" 200}})
          (dl/get-data-local-costs)))))

(deftype FixedFitnessCalculator [fitness]
  VMTaskFitnessCalculator
  (getName [this] "FixedFitnessCalculator")
  (calculateFitness [this _ _ _] fitness))

(defn fake-vm-for-host [hostname]
  (reify
    VirtualMachineCurrentState
    (getHostname [this] hostname)))

(defrecord FakeTaskRequest [job]
    TaskRequest)

(deftest test-data-local-fitness-calculator
  (let [base-fitness 0.5
        base-calculator (FixedFitnessCalculator. base-fitness)
        data-locality-weight 0.9
        base-fitness-portion (* base-fitness (- 1 data-locality-weight))
        calculator (dl/->DataLocalFitnessCalculator base-calculator
                                                    data-locality-weight)
        [d1 d2] [#{{:dataset {"a" "a"}}} #{{:dataset {"b" "b"}}}]
        job-1 {:job/uuid (UUID/randomUUID)
               :job/datasets d1}
        job-2 {:job/uuid (UUID/randomUUID)}]
    (dl/update-data-local-costs {d1 {"hostA" 0
                                     "hostB" 0.5}
                                 d2 {"hostA" 0
                                     "hostB" 0}}
                                [])
    (testing "uses base fitness for jobs that do not support data locality"
      (is (= base-fitness (.calculateFitness calculator (FakeTaskRequest. job-2) (fake-vm-for-host "hostA") nil)))
      (is (= base-fitness (.calculateFitness calculator (FakeTaskRequest. job-2) (fake-vm-for-host "hostB") nil))))

    (testing "calculates fitness for jobs that support data locality"
      (is (= (+ data-locality-weight base-fitness-portion)
             (.calculateFitness calculator (FakeTaskRequest. job-1) (fake-vm-for-host "hostA") nil)))
      (is (= (+ (* 0.5 data-locality-weight) base-fitness-portion)
             (.calculateFitness calculator (FakeTaskRequest. job-1) (fake-vm-for-host "hostB") nil)))
      (is (= base-fitness-portion
             (.calculateFitness calculator (FakeTaskRequest. job-1) (fake-vm-for-host "hostC") nil))))))


(deftest test-jobs-to-update
  (with-redefs [config/data-local-fitness-config (constantly {:batch-size 3})]
    (dl/reset-data-local-costs!)
    (testing "does not update data for running and completed jobs"
      (let [conn (restore-fresh-database! "datomic:mem://test-job-ids-to-update")
            _ (create-dummy-job conn :job-state :job.state/running
                                :datasets #{{:dataset {"a" "a"}}})
            _ (create-dummy-job conn :job-state :job.state/completed
                                :datasets #{{:dataset {"b" "b"}}})
            _ (create-dummy-job conn :job-state :job.state/running
                                :datasets #{{:dataset {"c" "c"}}})
            {:keys [to-fetch]} (dl/jobs-to-update (d/db conn))]
        (is (empty? to-fetch))))

    (testing "ignores jobs which don't support data locality"
      (let [conn (restore-fresh-database! "datomic:mem://test-job-ids-to-update")
            _ (create-dummy-job conn :job-state :job.state/waiting)
            {:keys [to-fetch]} (dl/jobs-to-update (d/db conn))]
        (is (empty? to-fetch))))

    (testing "prefers jobs with missing data, sorted by submit time"
      (let [conn (restore-fresh-database! "datomic:mem://test-job-ids-to-update")
            [d1 d2 d3 d4] [#{{:dataset {"a" "a"}}} #{{:dataset {"b" "b"}}}
                           #{{:dataset {"c" "c"}}} #{{:dataset {"d" "d"}}}]
            j1 (create-dummy-job conn :job-state :job.state/waiting
                                 :submit-time (java.util.Date. 0)
                                 :datasets d1)
            j2 (create-dummy-job conn :job-state :job.state/waiting
                                 :submit-time (java.util.Date. 1)
                                 :datasets d2)
            j3 (create-dummy-job conn :job-state :job.state/waiting
                                 :submit-time (java.util.Date. 3)
                                 :datasets d3)
            j4 (create-dummy-job conn :job-state :job.state/waiting
                                 :submit-time (java.util.Date. 2)
                                 :datasets d4)
            db (d/db conn)
            _ (dl/update-data-local-costs {d1 {"hostA" 100}} [])
            {:keys [to-fetch to-remove]} (dl/jobs-to-update db)]
        (is (= (map :job/datasets to-fetch) [d2 d4 d3]))
        (is (= #{} to-remove))))

    (testing "removes jobs which are no longer waiting"
      (dl/reset-data-local-costs!)
      (let [conn (restore-fresh-database! "datomic:mem://test-job-ids-to-update")
            [d1 d2 d3] [#{{:dataset {"a" "a"}}} #{{:dataset {"b" "b"}}}
                           #{{:dataset {"c" "c"}}}]
            j1 (create-dummy-job conn :job-state :job.state/waiting
                                 :datasets d1)
            j2 (create-dummy-job conn :job-state :job.state/running
                                 :datasets d2)
            j3 (create-dummy-job conn :job-state :job.state/completed
                                 :datasets d3)
            db (d/db conn)
            _ (dl/update-data-local-costs {d1 {"hostA" 100}
                                           d2 {"hostA" 100}
                                           d3 {"hostA" 100}}
                                          [])
            {:keys [to-fetch to-remove]} (dl/jobs-to-update db)]
        (is (= (map :job/datasets to-fetch) [d1]))
        (is (= to-remove #{d2 d3}))))))


(deftest test-fetch-and-update-data-local-costs
  (let [first-cost {"hostA" 1.0}
        second-cost {"hostA" 0.5}
        third-cost {"hostA" 0.2}
        current-cost-atom (atom first-cost)]
    (with-redefs [config/data-local-fitness-config (constantly {:batch-size 2})
                  dl/fetch-data-local-costs (fn [jobs]
                                              (pc/map-from-keys (fn [_] @current-cost-atom)
                                                                (map :job/datasets jobs)))]
      (testing "calls service and updates jobs"
        (dl/reset-data-local-costs!)
        (let [conn (restore-fresh-database! "datomic:mem://test-fetch-and-update-data-local-costs")
              j1 (create-dummy-job conn :job-state :job.state/waiting
                                   :datasets #{{:dataset {"foo" "bar"}}})
              j2 (create-dummy-job conn :job-state :job.state/waiting
                                   :datasets #{{:dataset {"bar" "baz"}}})
              db (d/db conn)
              [id1 id2] (map #(:job/uuid (d/entity db %)) [j1 j2])]
          (dl/update-data-local-costs {#{{"foo" "bar"}} first-cost} [])
          (dl/fetch-and-update-data-local-costs db)
          (is (= {#{{:dataset {"foo" "bar"}}} first-cost
                  #{{:dataset {"bar" "baz"}}} first-cost}
                 (dl/get-data-local-costs)))))

      (testing "correctly computes update order"
        (dl/reset-data-local-costs!)
        (let [conn (restore-fresh-database! "datomic:mem://test-fetch-and-update-data-local-costs")
              [d1 d2 d3] [#{{:dataset {"a" "a"}}} #{{:dataset {"b" "b"}}} #{{:dataset {"c" "c"}}}]
              j1 (create-dummy-job conn :job-state :job.state/waiting
                                   :datasets d1
                                   :submit-time (java.util.Date. 0))
              j2 (create-dummy-job conn :job-state :job.state/waiting
                                   :datasets d2
                                   :submit-time (java.util.Date. 1))
              j3 (create-dummy-job conn :job-state :job.state/waiting
                                   :datasets d3
                                   :submit-time (java.util.Date. 2))
              db (d/db conn)
              [id1 id2 id3] (map #(:job/uuid (d/entity db %)) [j1 j2 j3])]
          (dl/fetch-and-update-data-local-costs db)
          (is (= {d1 first-cost
                  d2 first-cost}
                 (dl/get-data-local-costs)))
          (reset! current-cost-atom second-cost)
          (dl/fetch-and-update-data-local-costs db)
          (let [cost-data (dl/get-data-local-costs)
                ; We should have updated id3 and one other uuid, let's grab that one
                [updated-dataset] (filter #(= @current-cost-atom (cost-data %))
                                          [d1 d2])
                [other-dataset] (keys (dissoc cost-data d3 updated-dataset))]
            (is (= {d3 second-cost
                    updated-dataset second-cost
                    other-dataset first-cost}
                   cost-data))

            (reset! current-cost-atom third-cost)
            (dl/fetch-and-update-data-local-costs db)
            ; Now, we should update the job we skipped last time
            (is (= third-cost (get (dl/get-data-local-costs) other-dataset)))))))))
