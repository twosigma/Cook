(ns cook.test.mesos.data-locality
  (:use clojure.test)
  (:require [cook.config :as config]
            [cook.mesos.data-locality :as dl]
            [datomic.api :as d])
  (:import java.util.UUID
           [com.netflix.fenzo TaskRequest
                              VMTaskFitnessCalculator
                              VirtualMachineCurrentState]))

(deftest test-get-normalized-cost
  (with-redefs [config/data-local-fitness-config (constantly {:maximum-cost 100})]
    (let [job-1 (UUID/randomUUID)
          job-2 (UUID/randomUUID)
          job-3 (UUID/randomUUID)]
      (dl/update-data-local-costs {job-1 {"hostA" 10
                                          "hostB" 20}
                                   job-2 {"hostA" 30}})
      (testing "correctly normalizes costs"
        (is (= 0.75 (dl/get-normalized-fitness job-1 "hostA" 40)))
        (is (= 0.5 (dl/get-normalized-fitness job-1 "hostB" 40)))
        (is (= 0.25 (dl/get-normalized-fitness job-2 "hostA" 40))))

      (testing "uses max cost for missing host or job"
        (is (= 0.0 (dl/get-normalized-fitness job-2 "hostB" 40)))
        (is (= 0.0 (dl/get-normalized-fitness job-3 "hostA" 40)))))))

(deftest test-update-data-local-costs
  (with-redefs [config/data-local-fitness-config (constantly {:maximum-cost 100})]
    (let [job-1 (UUID/randomUUID)
          job-2 (UUID/randomUUID)]
      (dl/update-data-local-costs {job-1 {"hostA" 200
                                          "hostB" 20
                                          "hostC" -20}
                                   job-2 {"hostB" 1000}})
      (is (= {job-1 {"hostA" 100
                     "hostB" 20
                     "hostC" 0}
              job-2 {"hostB" 100}}
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
      (dl/update-data-local-costs {(:job/uuid job-1) {"hostA" 0
                                                      "hostB" 20}
                                   (:job/uuid job-2) {"hostA" 0
                                                      "hostB" 0}})
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

