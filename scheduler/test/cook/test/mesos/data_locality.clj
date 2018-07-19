(ns cook.test.mesos.data-locality
  (:use clojure.test)
  (:require [cook.mesos.data-locality :as dl]
            [datomic.api :as d])
  (:import java.util.UUID
           [com.netflix.fenzo TaskRequest
                              VMTaskFitnessCalculator
                              VirtualMachineCurrentState]))

(deftest test-get-normalized-cost
  (let [job-1 (UUID/randomUUID)
        job-2 (UUID/randomUUID)
        job-3 (UUID/randomUUID)]
    (reset! dl/data-local-costs {job-1 {"hostA" 10
                                        "hostB" 20}
                                 job-2 {"hostA" 30}})
    (testing "correctly normalizes costs"
      (is (= 0.75 (dl/get-normalized-fitness job-1 "hostA" 40)))
      (is (= 0.5 (dl/get-normalized-fitness job-1 "hostB" 40)))
      (is (= 0.25 (dl/get-normalized-fitness job-2 "hostA" 40))))

    (testing "uses max cost for missing host or job"
      (is (= 0.0 (dl/get-normalized-fitness job-2 "hostB" 40)))
      (is (= 0.0 (dl/get-normalized-fitness job-3 "hostA" 40))))))

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
        maximum-cost 40
        data-locality-weight 0.9
        base-fitness-portion (* base-fitness (- 1 data-locality-weight))
        calculator (dl/->DataLocalFitnessCalculator base-calculator
                                                    data-locality-weight
                                                    maximum-cost)
        job-1 {:job/uuid (UUID/randomUUID)
               :job/supports-data-locality true}
        job-2 {:job/uuid (UUID/randomUUID)
               :job/supports-data-locality false}]
    (reset! dl/data-local-costs {(:job/uuid job-1) {"hostA" 0
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
             (.calculateFitness calculator (FakeTaskRequest. job-1) (fake-vm-for-host "hostC") nil))))))

