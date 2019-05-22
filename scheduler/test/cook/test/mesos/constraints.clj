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

(ns cook.test.mesos.constraints
  (:use [clojure.test])
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [cook.config :as config]
            [cook.scheduler.constraints :as constraints]
            [cook.scheduler.data-locality :as dl]
            [cook.scheduler.scheduler :as sched]
            [cook.util2 :as util]
            [cook.test.testutil :refer (restore-fresh-database! create-dummy-group create-dummy-job create-dummy-instance create-dummy-job-with-instances setup)]
            [datomic.api :as d :refer (db)])
  (:import [java.util Date UUID]
           org.joda.time.DateTime
           org.mockito.Mockito))

(deftest test-get-group-constraint-name
  (is (= "unique-host-placement-group-constraint"
         (constraints/group-constraint-name (constraints/->unique-host-placement-group-constraint nil))))
  (is (= "balanced-host-placement-group-constraint"
         (constraints/group-constraint-name (constraints/->balanced-host-placement-group-constraint nil))))
  (is (= "attribute-equals-host-placement-group-constraint"
         (constraints/group-constraint-name (constraints/->attribute-equals-host-placement-group-constraint nil)))))

(deftest test-user-defined-constraint
  (let [constraints [{:constraint/attribute "is_spot"
                      :constraint/operator :constraint.operator/equals
                      :constraint/pattern "true"}
                     {:constraint/attribute "instance_type"
                      :constraint/operator :constraint.operator/equals
                      :constraint/pattern "mem.large"}]
        user-defined-constraint (constraints/->user-defined-constraint constraints)]
    (is (= true (first (constraints/job-constraint-evaluate user-defined-constraint nil {"is_spot" "true" "instance_type" "mem.large"}))))
    (is (= false (first (constraints/job-constraint-evaluate user-defined-constraint nil {"is_spot" "true" "instance_type" "cpu.large"}))))
    (is (= false (first (constraints/job-constraint-evaluate user-defined-constraint nil {"is_spot" "false" "instance_type" "mem.large"}))))
    (is (= false (first (constraints/job-constraint-evaluate user-defined-constraint nil {"is_spot" "true"}))))
    (is (= false (first (constraints/job-constraint-evaluate user-defined-constraint nil {"instance_type" "mem.large"}))))
    (is (= false (first (constraints/job-constraint-evaluate user-defined-constraint nil {}))))))


(deftest test-gpu-constraint
  (cook.test.testutil/setup)
  (let [framework-id #mesomatic.types.FrameworkID{:value "my-framework-id"}
        gpu-offer #mesomatic.types.Offer{:id #mesomatic.types.OfferID {:value "my-offer-id"}
                                         :framework-id framework-id
                                         :slave-id #mesomatic.types.SlaveID{:value "my-slave-id"},
                                         :hostname "slave3",
                                         :resources [#mesomatic.types.Resource{:name "cpus", :type :value-scalar, :scalar 40.0, :ranges [], :set #{}, :role "*"}
                                                     #mesomatic.types.Resource{:name "mem", :type :value-scalar, :scalar 5000.0, :ranges [], :set #{}, :role "*"}
                                                     #mesomatic.types.Resource{:name "disk", :type :value-scalar, :scalar 6000.0, :ranges [], :set #{}, :role "*"}
                                                     #mesomatic.types.Resource{:name "ports", :type :value-ranges, :scalar 0.0, :ranges [#mesomatic.types.ValueRange{:begin 31000, :end 32000}], :set #{}, :role "*"}
                                                     #mesomatic.types.Resource{:name "gpus", :type :value-scalar :scalar 2.0 :role "*"}],
                                         :attributes [],
                                         :executor-ids []}
        non-gpu-offer #mesomatic.types.Offer{:id #mesomatic.types.OfferID {:value "my-offer-id"}
                                             :framework-id framework-id
                                             :slave-id #mesomatic.types.SlaveID{:value "my-slave-id"},
                                             :hostname "slave3",
                                             :resources [#mesomatic.types.Resource{:name "cpus", :type :value-scalar, :scalar 40.0, :ranges [], :set #{}, :role "*"}
                                                         #mesomatic.types.Resource{:name "mem", :type :value-scalar, :scalar 5000.0, :ranges [], :set #{}, :role "*"}
                                                         #mesomatic.types.Resource{:name "disk", :type :value-scalar, :scalar 6000.0, :ranges [], :set #{}, :role "*"}
                                                         #mesomatic.types.Resource{:name "ports", :type :value-ranges, :scalar 0.0, :ranges [#mesomatic.types.ValueRange{:begin 31000, :end 32000}], :set #{}, :role "*"}],
                                             :attributes [],
                                             :executor-ids []}
        uri "datomic:mem://test-gpu-constraint"
        conn (restore-fresh-database! uri)
        gpu-job-id (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 1.0)
        other-gpu-job-id (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 1.0)
        non-gpu-job-id (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 0.0)
        db (db conn)
        gpu-job (d/entity db gpu-job-id)
        other-gpu-job (d/entity db other-gpu-job-id)
        non-gpu-job (d/entity db non-gpu-job-id)
        mock-gpu-assignment #(-> (Mockito/when (.getRequest (Mockito/mock com.netflix.fenzo.TaskAssignmentResult)))
                                 (.thenReturn (sched/make-task-request db other-gpu-job))
                                 (.getMock))]
    (doseq [[type gpu-lease] [["gpu avail"
                               (reify com.netflix.fenzo.VirtualMachineCurrentState
                                (getHostname [_] "test-host")
                                (getRunningTasks [_] [])
                                (getTasksCurrentlyAssigned [_] [])
                                (getCurrAvailableResources [_]  (sched/->VirtualMachineLeaseAdapter gpu-offer 0)))]
                              ["running gpu"
                               (reify com.netflix.fenzo.VirtualMachineCurrentState
                                (getHostname [_] "test-host")
                                (getRunningTasks [_] [(sched/make-task-request db other-gpu-job)])
                                (getTasksCurrentlyAssigned [_] [])
                                (getCurrAvailableResources [_]  (sched/->VirtualMachineLeaseAdapter non-gpu-offer 0)))]
                              ["gpu assigned"
                               (reify com.netflix.fenzo.VirtualMachineCurrentState
                                (getHostname [_] "test-host")
                                (getRunningTasks [_] [])
                                (getTasksCurrentlyAssigned [_] [(mock-gpu-assignment)])
                                (getCurrAvailableResources [_]  (sched/->VirtualMachineLeaseAdapter non-gpu-offer 0)))]]]
      (is (.isSuccessful
            (.evaluate (constraints/fenzoize-job-constraint (constraints/build-gpu-host-constraint gpu-job))
                       (sched/make-task-request db gpu-job)
                       gpu-lease
                       nil))
          (str "GPU task on GPU host with " type " should succeed"))
      (is (not (.isSuccessful
                 (.evaluate (constraints/fenzoize-job-constraint (constraints/build-gpu-host-constraint non-gpu-job))
                            (sched/make-task-request db non-gpu-job)
                            gpu-lease
                            nil)))
          (str "non GPU task on GPU host with " type " should fail"))
      (is (not (.isSuccessful
                 (.evaluate (constraints/fenzoize-job-constraint (constraints/build-gpu-host-constraint gpu-job))
                            (sched/make-task-request db gpu-job)
                            (reify com.netflix.fenzo.VirtualMachineCurrentState
                              (getHostname [_] "test-host")
                              (getRunningTasks [_] [])
                              (getTasksCurrentlyAssigned [_] [])
                              (getCurrAvailableResources [_]  (sched/->VirtualMachineLeaseAdapter non-gpu-offer 0)))
                            nil)))
          "GPU task on non GPU host should fail")
      (is (.isSuccessful
            (.evaluate (constraints/fenzoize-job-constraint (constraints/build-gpu-host-constraint non-gpu-job))
                       (sched/make-task-request db non-gpu-job)
                       (reify com.netflix.fenzo.VirtualMachineCurrentState
                         (getHostname [_] "test-host")
                         (getRunningTasks [_] [])
                         (getTasksCurrentlyAssigned [_] [])
                         (getCurrAvailableResources [_]  (sched/->VirtualMachineLeaseAdapter non-gpu-offer 0)))
                       nil))
        "non GPU task on non GPU host should succeed"))))


(deftest test-rebalancer-reservation-constraint
  (setup)
  (let [framework-id #mesomatic.types.FrameworkID{:value "my-framework-id"}
        uri "datomic:mem://test-rebalancer-reservation-constraint"
        conn (restore-fresh-database! uri)
        job-id (create-dummy-job conn :user "pschorf" :ncpus 5.0 :memory 5.0)
        db (d/db conn)
        job (d/entity db job-id)
        reserved-hosts #{"hostB"}
        hostA-offer #mesomatic.types.Offer{:id #mesomatic.types.OfferID {:value "my-offer-id"}
                                           :framework-id framework-id
                                           :slave-id #mesomatic.types.SlaveID{:value "my-slave-id"},
                                           :hostname "hostA",
                                           :resources [#mesomatic.types.Resource{:name "cpus", :type :value-scalar, :scalar 40.0, :ranges [], :set #{}, :role "*"}
                                                       #mesomatic.types.Resource{:name "mem", :type :value-scalar, :scalar 5000.0, :ranges [], :set #{}, :role "*"}
                                                       #mesomatic.types.Resource{:name "disk", :type :value-scalar, :scalar 6000.0, :ranges [], :set #{}, :role "*"}
                                                       #mesomatic.types.Resource{:name "ports", :type :value-ranges, :scalar 0.0, :ranges [#mesomatic.types.ValueRange{:begin 31000, :end 32000}], :set #{}, :role "*"}],
                                           :attributes [],
                                           :executor-ids []}
        hostB-offer #mesomatic.types.Offer{:id #mesomatic.types.OfferID {:value "my-offer-id"}
                                           :framework-id framework-id
                                           :slave-id #mesomatic.types.SlaveID{:value "my-slave-id"},
                                           :hostname "hostB",
                                           :resources [#mesomatic.types.Resource{:name "cpus", :type :value-scalar, :scalar 40.0, :ranges [], :set #{}, :role "*"}
                                                       #mesomatic.types.Resource{:name "mem", :type :value-scalar, :scalar 5000.0, :ranges [], :set #{}, :role "*"}
                                                       #mesomatic.types.Resource{:name "disk", :type :value-scalar, :scalar 6000.0, :ranges [], :set #{}, :role "*"}
                                                       #mesomatic.types.Resource{:name "ports", :type :value-ranges, :scalar 0.0, :ranges [#mesomatic.types.ValueRange{:begin 31000, :end 32000}], :set #{}, :role "*"}],
                                           :attributes [],
                                           :executor-ids []}
        constraint (constraints/build-rebalancer-reservation-constraint reserved-hosts)]
    (is (not (.isSuccessful
              (.evaluate constraint
                         (sched/make-task-request db job-id)
                         (reify com.netflix.fenzo.VirtualMachineCurrentState
                           (getHostname [_] "hostB")
                           (getRunningTasks [_] [])
                           (getTasksCurrentlyAssigned [_] [])
                           (getCurrAvailableResources [_]  (sched/->VirtualMachineLeaseAdapter hostB-offer 0)))
                         nil))))
    (is (.isSuccessful
         (.evaluate constraint
                    (sched/make-task-request db job-id)
                    (reify com.netflix.fenzo.VirtualMachineCurrentState
                      (getHostname [_] "hostA")
                      (getRunningTasks [_] [])
                      (getTasksCurrentlyAssigned [_] [])
                      (getCurrAvailableResources [_]  (sched/->VirtualMachineLeaseAdapter hostA-offer 0)))
                    nil)))))


(deftest test-build-estimated-completion-constraint
  (testing "does not generate a constraint when turned off"
    (with-redefs [config/estimated-completion-config (constantly nil)]
      (is (nil? (constraints/build-estimated-completion-constraint {})))))

  (let [conn (restore-fresh-database! "datomic:mem://test-estimated-completion-constraint")]
    (with-redefs [config/estimated-completion-config (constantly {:expected-runtime-multiplier 1.2
                                                                  :host-lifetime-mins 60
                                                                  :agent-start-grace-period-mins 10})
                  t/now (constantly (DateTime. 0))]
      (let [job-id (create-dummy-job conn)
            job (util/job-ent->map (d/entity (d/db conn) job-id))]
        (is (nil? (constraints/build-estimated-completion-constraint job))))

      (let [job-id (create-dummy-job conn :expected-runtime 1000)
            job (util/job-ent->map (d/entity (d/db conn) job-id))
            constraint (constraints/build-estimated-completion-constraint job)]
        (is (= 1200 (:estimated-end-time constraint)))
        (is (= 60 (:host-lifetime-mins constraint))))

      (let [[job-id _] (create-dummy-job-with-instances conn :instances [{:instance-status :instance.status/failed
                                                                          :reason :preempted-by-rebalancer}])
            job (util/job-ent->map (d/entity (d/db conn) job-id))]
        (is (nil? (constraints/build-estimated-completion-constraint job))))

      (let [instance-duration 10000
            instance {:instance-status :instance.status/failed
                      :reason :mesos-slave-removed
                      :mesos-start-time (Date. 0)
                      :end-time (Date. instance-duration)}
            [job-id _] (create-dummy-job-with-instances conn
                                                        :expected-runtime 1000
                                                        :instances [instance])
            job (util/job-ent->map (d/entity (d/db conn) job-id))
            constraint (constraints/build-estimated-completion-constraint job)]
        (is (= 10000 (:estimated-end-time constraint)))
        (is (= 60 (:host-lifetime-mins constraint))))

      (let [instance-duration 10000
            instance {:instance-status :instance.status/failed
                      :reason :mesos-slave-removed
                      :mesos-start-time nil
                      :end-time (Date. instance-duration)}
            [job-id _] (create-dummy-job-with-instances conn
                                                        :expected-runtime 1000
                                                        :instances [instance])
            job (util/job-ent->map (d/entity (d/db conn) job-id))
            constraint (constraints/build-estimated-completion-constraint job)]
        (is (= 1200 (:estimated-end-time constraint)))
        (is (= 60 (:host-lifetime-mins constraint))))

      (let [instance {:instance-status :instance.status/failed
                      :reason :mesos-slave-removed
                      :mesos-start-time (Date. 0)
                      :end-time nil}
            [job-id _] (create-dummy-job-with-instances conn
                                                        :expected-runtime 1000
                                                        :instances [instance])
            job (util/job-ent->map (d/entity (d/db conn) job-id))
            constraint (constraints/build-estimated-completion-constraint job)]
        (is (= 1200 (:estimated-end-time constraint)))
        (is (= 60 (:host-lifetime-mins constraint))))

      (let [job-id (create-dummy-job conn :expected-runtime (* 90 60 1000))
            job (util/job-ent->map (d/entity (d/db conn) job-id))
            constraint (constraints/build-estimated-completion-constraint job)]
        (is (= (* 50 1000 60) (:estimated-end-time constraint)))
        (is (= 60 (:host-lifetime-mins constraint))))

      (let [instance-duration (* 59 60 1000)
            instance {:instance-status :instance.status/failed
                      :reason :mesos-slave-removed
                      :mesos-start-time (Date. 0)
                      :end-time (Date. instance-duration)}
            [job-id _] (create-dummy-job-with-instances conn :expected-runtime (+ instance-duration 10) :instances [instance])
            job (util/job-ent->map (d/entity (d/db conn) job-id))
            constraint (constraints/build-estimated-completion-constraint job)]
        (is (= (* 50 1000 60) (:estimated-end-time constraint)))
        (is (= 60 (:host-lifetime-mins constraint)))))))

(deftest test-estimated-completion-constraint
  (let [estimated-end-time 100000
        host-lifetime-mins 1
        constraint (constraints/->estimated-completion-constraint estimated-end-time host-lifetime-mins)]
    (is (first (constraints/job-constraint-evaluate constraint nil {})))
    (is (not (first (constraints/job-constraint-evaluate constraint nil {"host-start-time" 0.0}))))
    (is (first (constraints/job-constraint-evaluate constraint nil {"host-start-time" 51.0})))))


(deftest test-data-locality-constraint
  (with-redefs [dl/job-uuid->dataset-maps-cache (util/new-cache)]
    (testing "disabled when not using data local fitness calculator"
      (with-redefs [config/fitness-calculator-config (constantly config/default-fitness-calculator)
                    config/data-local-fitness-config (constantly {:launch-wait-seconds 60})]
        (is (nil? (constraints/build-data-locality-constraint {:job/uuid (UUID/randomUUID)})))
        (is (nil? (constraints/build-data-locality-constraint {:job/uuid (UUID/randomUUID)
                                                               :job/datasets #{{:dataset {"a" "a"}}}})))))

    (testing "disabled for non data-local jobs"
      (with-redefs [config/fitness-calculator-config (constantly dl/data-local-fitness-calculator)
                    config/data-local-fitness-config (constantly {:launch-wait-seconds 60})]
        (is (nil? (constraints/build-data-locality-constraint {:job/uuid (UUID/randomUUID)})))
        (is (not (nil? (constraints/build-data-locality-constraint {:job/uuid (UUID/randomUUID)
                                                                    :job/datasets #{{:dataset {"a" "a"}}}}))))))

    (testing "passes jobs older than launch-wait-seconds"
      (with-redefs [config/fitness-calculator-config (constantly dl/data-local-fitness-calculator)
                    config/data-local-fitness-config (constantly {:launch-wait-seconds 60})]
        (dl/reset-data-local-costs!)
        (let [submit-time (tc/to-date (t/minus (t/now) (t/seconds 61)))
              constraint (constraints/build-data-locality-constraint {:job/uuid (UUID/randomUUID)
                                                                      :job/datasets #{{:dataset {"a" "a"}}}
                                                                      :job/submit-time submit-time})
              [passes reason] (constraints/job-constraint-evaluate constraint
                                                                   nil
                                                                   nil)]
          (is passes))))

    (testing "requires data for newer jobs"
      (dl/reset-data-local-costs!)
      (with-redefs [config/fitness-calculator-config (constantly dl/data-local-fitness-calculator)
                    config/data-local-fitness-config (constantly {:launch-wait-seconds 60})]
        (let [with-data-datasets #{{:dataset {"a" "a"}}}
              _ (dl/update-data-local-costs {with-data-datasets {"hostA" {:cost 0
                                                                          :suitable true}}} [])
              with-data-constraint (constraints/build-data-locality-constraint {:job/uuid (UUID/randomUUID)
                                                                                :job/datasets #{{:dataset/parameters #{{:dataset.parameter/key "a" :dataset.parameter/value "a"}}}}
                                                                                :job/submit-time (tc/to-date (t/now))})
              without-data-constraint (constraints/build-data-locality-constraint {:job/uuid (UUID/randomUUID)
                                                                                   :job/datasets #{{:datasets {"b" "b"}}}
                                                                                   :job/submit-time (tc/to-date (t/now))})
              [with-data-result _] (constraints/job-constraint-evaluate with-data-constraint nil nil)
              [without-data-result msg] (constraints/job-constraint-evaluate without-data-constraint nil nil)]
          (is with-data-result)
          (is (not without-data-result))
          (is (= "No data locality costs available" msg)))))

    (testing "fails for unsuitable hosts"
      (dl/reset-data-local-costs!)
      (with-redefs [config/fitness-calculator-config (constantly dl/data-local-fitness-calculator)
                    config/data-local-fitness-config (constantly {:launch-wait-seconds 60})]
        (let [datasets #{{:dataset {"a" "a"}}}
              _ (dl/update-data-local-costs {datasets {"hostA" {:cost 0
                                                                :suitable true}
                                                       "hostB" {:cost 1.0
                                                                :suitable false}}} [])
              constraint (constraints/build-data-locality-constraint {:job/uuid (UUID/randomUUID)
                                                                      :job/datasets #{{:dataset/parameters #{{:dataset.parameter/key "a" :dataset.parameter/value "a"}}}}
                                                                      :job/submit-time (tc/to-date (t/now))})]

          ;; suitable
          (is (= [true nil] (constraints/job-constraint-evaluate constraint nil {"HOSTNAME" "hostA"})))
          ;; default allow
          (is (= [true nil] (constraints/job-constraint-evaluate constraint nil {"HOSTNAME" "hostC"})))
          ;; unsuitable
          (is (= [false "Host is not suitable for datasets"] (constraints/job-constraint-evaluate constraint nil {"HOSTNAME" "hostB"}))))))))
