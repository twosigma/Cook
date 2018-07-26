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

(deftest test-get-normalized-cost
  (with-redefs [config/data-local-fitness-config (constantly {:maximum-cost 100})]
    (let [job-1 (UUID/randomUUID)
          job-2 (UUID/randomUUID)
          job-3 (UUID/randomUUID)]
      (dl/reset-data-local-costs!)
      (dl/update-data-local-costs {(str job-1) {"hostA" 10
                                                "hostB" 20}
                                   (str job-2) {"hostA" 30}}
                                  [])
      (testing "correctly normalizes costs"
        (is (= 0.75 (dl/get-normalized-fitness job-1 "hostA" 40)))
        (is (= 0.5 (dl/get-normalized-fitness job-1 "hostB" 40)))
        (is (= 0.25 (dl/get-normalized-fitness job-2 "hostA" 40))))

      (testing "uses max cost for missing host or job"
        (is (= 0.0 (dl/get-normalized-fitness job-2 "hostB" 40)))
        (is (= 0.0 (dl/get-normalized-fitness job-3 "hostA" 40)))))))

(deftest test-update-data-local-costs
  (with-redefs [config/data-local-fitness-config (constantly {:maximum-cost 100})]
    (testing "sanitizes input costs"
      (let [job-1 (str (UUID/randomUUID))
            job-2 (str (UUID/randomUUID))]
        (dl/reset-data-local-costs!)
        (dl/update-data-local-costs {job-1 {"hostA" 200
                                            "hostB" 20
                                            "hostC" -20}
                                     job-2 {"hostB" 1000}}
                                    [])
        (is (= {job-1 {"hostA" 100
                       "hostB" 20
                       "hostC" 0}
                job-2 {"hostB" 100}}
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
            (dl/get-data-local-costs))))))

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
  (with-redefs [config/data-local-fitness-config (constantly {:maximum-cost 100})]
    (let [base-fitness 0.5
          base-calculator (FixedFitnessCalculator. base-fitness)
          maximum-cost 40
          data-locality-weight 0.9
          base-fitness-portion (* base-fitness (- 1 data-locality-weight))
          calculator (dl/->DataLocalFitnessCalculator base-calculator
                                                      data-locality-weight
                                                      maximum-cost)
          job-1 {:job/uuid (UUID/randomUUID)
                 :job/data-local true}
          job-2 {:job/uuid (UUID/randomUUID)
                 :job/data-local false}]
      (dl/update-data-local-costs {(str (:job/uuid job-1)) {"hostA" 0
                                                            "hostB" 20}
                                   (str (:job/uuid job-2)) {"hostA" 0
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
               (.calculateFitness calculator (FakeTaskRequest. job-1) (fake-vm-for-host "hostC") nil)))))))


(deftest test-job-ids-to-update
  (with-redefs [config/data-local-fitness-config (constantly {:batch-size 3
                                                              :maximum-cost 100})]
    (dl/reset-data-local-costs!)
    (testing "does not update data for running and completed jobs"
      (let [conn (restore-fresh-database! "datomic:mem://test-job-ids-to-update")
            _ (create-dummy-job conn :job-state :job.state/running
                                :data-local true)
            _ (create-dummy-job conn :job-state :job.state/completed
                                :data-local true)
            _ (create-dummy-job conn :job-state :job.state/running
                                :data-local true)
            {:keys [to-fetch]} (dl/job-ids-to-update (d/db conn))]
        (is (empty? to-fetch))))

    (testing "ignores jobs which don't support data locality"
      (let [conn (restore-fresh-database! "datomic:mem://test-job-ids-to-update")
            _ (create-dummy-job conn :job-state :job.state/waiting)
            {:keys [to-fetch]} (dl/job-ids-to-update (d/db conn))]
        (is (empty? to-fetch))))

    (testing "prefers jobs with missing data, sorted by submit time"
      (let [conn (restore-fresh-database! "datomic:mem://test-job-ids-to-update")
            j1 (create-dummy-job conn :job-state :job.state/waiting
                                 :submit-time (java.util.Date. 0)
                                 :data-local true)
            j2 (create-dummy-job conn :job-state :job.state/waiting
                                 :submit-time (java.util.Date. 1)
                                 :data-local true)
            j3 (create-dummy-job conn :job-state :job.state/waiting
                                 :submit-time (java.util.Date. 3)
                                 :data-local true)
            j4 (create-dummy-job conn :job-state :job.state/waiting
                                 :submit-time (java.util.Date. 2)
                                 :data-local true)
            db (d/db conn)
            _ (dl/update-data-local-costs {(:job/uuid (d/entity db j1)) {"hostA" 100}} [])
            {:keys [to-fetch to-remove]} (dl/job-ids-to-update db)]
        (is (= to-fetch (map (fn [id] (:job/uuid (d/entity db id))) [j2 j4 j3])))
        (is (= #{} to-remove))))

    (testing "removes jobs which are no longer waiting"
      (dl/reset-data-local-costs!)
      (let [conn (restore-fresh-database! "datomic:mem://test-job-ids-to-update")
            j1 (create-dummy-job conn :job-state :job.state/waiting
                                 :data-local true)
            j2 (create-dummy-job conn :job-state :job.state/running
                                 :data-local true)
            j3 (create-dummy-job conn :job-state :job.state/completed
                                 :data-local true)
            db (d/db conn)
            [id1 id2 id3] (map #(:job/uuid (d/entity db %)) [j1 j2 j3])
            _ (dl/update-data-local-costs {id1 {"hostA" 100}
                                           id2 {"hostA" 100}
                                           id3 {"hostA" 100}}
                                          [])
            {:keys [to-fetch to-remove]} (dl/job-ids-to-update db)]
        (is (= to-fetch [id1]))
        (is (= to-remove #{id2 id3}))))))


(deftest test-fetch-and-update-data-local-costs
  (let [first-cost {"hostA" 100}
        second-cost {"hostA" 50}
        third-cost {"hostA" 20}
        current-cost-atom (atom first-cost)]
    (with-redefs [config/data-local-fitness-config (constantly {:batch-size 2
                                                                :maximum-cost 100})
                  dl/fetch-data-local-costs (fn [uuids]
                                              (pc/map-from-keys (fn [_] @current-cost-atom)
                                                                uuids))]
      (testing "calls service and updates jobs"
        (dl/reset-data-local-costs!)
        (let [conn (restore-fresh-database! "datomic:mem://test-fetch-and-update-data-local-costs")
              j1 (create-dummy-job conn :job-state :job.state/waiting
                                   :data-local true)
              j2 (create-dummy-job conn :job-state :job.state/waiting
                                   :data-local true)
              db (d/db conn)
              [id1 id2] (map #(:job/uuid (d/entity db %)) [j1 j2])]
          (dl/update-data-local-costs {id1 first-cost} [])
          (dl/fetch-and-update-data-local-costs db)
          (is (= {id1 first-cost
                  id2 first-cost}
                 (dl/get-data-local-costs)))))

      (testing "correctly computes update order"
        (dl/reset-data-local-costs!)
        (let [conn (restore-fresh-database! "datomic:mem://test-fetch-and-update-data-local-costs")
              j1 (create-dummy-job conn :job-state :job.state/waiting
                                   :data-local true
                                   :submit-time (java.util.Date. 0))
              j2 (create-dummy-job conn :job-state :job.state/waiting
                                   :data-local true
                                   :submit-time (java.util.Date. 1))
              j3 (create-dummy-job conn :job-state :job.state/waiting
                                   :data-local true
                                   :submit-time (java.util.Date. 2))
              db (d/db conn)
              [id1 id2 id3] (map #(:job/uuid (d/entity db %)) [j1 j2 j3])]
          (dl/fetch-and-update-data-local-costs db)
          (is (= {id1 first-cost
                  id2 first-cost}
                 (dl/get-data-local-costs)))
          (reset! current-cost-atom second-cost)
          (dl/fetch-and-update-data-local-costs db)
          (let [cost-data (dl/get-data-local-costs)
                ; We should have updated id3 and one other uuid, let's grab that one
                [updated-uuid] (filter #(= @current-cost-atom (cost-data %))
                                       [id1 id2])
                [other-uuid] (keys (dissoc cost-data id3 updated-uuid))]
            (is (= {id3 second-cost
                    updated-uuid second-cost
                    other-uuid first-cost}
                   cost-data))

            (reset! current-cost-atom third-cost)
            (dl/fetch-and-update-data-local-costs db)
            ; Now, we should update the job we skipped last time
            (is (= third-cost (get (dl/get-data-local-costs) other-uuid)))))))))
