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

(ns cook.test.scheduler.constraints
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [cook.config :as config]
            [cook.test.postgres]
            [cook.scheduler.constraints :as constraints]
            [cook.scheduler.offer :as offer]
            [cook.scheduler.scheduler :as sched]
            [cook.test.testutil :as testutil
             :refer [create-dummy-group create-dummy-instance create-dummy-job create-dummy-job-with-instances create-pool
                     make-task-assignment-result make-task-request restore-fresh-database! setup]]
            [cook.tools :as util]
            [datomic.api :as d :refer [db]])
  (:import (java.util Date)
           (org.joda.time DateTime)))

(use-fixtures :once cook.test.postgres/with-pg-db)

(deftest test-get-group-constraint-name
  (is (= "unique-host-placement-group-constraint"
         (constraints/group-constraint-name (constraints/->unique-host-placement-group-constraint nil))))
  (is (= "balanced-host-placement-group-constraint"
         (constraints/group-constraint-name (constraints/->balanced-host-placement-group-constraint nil))))
  (is (= "attribute-equals-host-placement-group-constraint"
         (constraints/group-constraint-name (constraints/->attribute-equals-host-placement-group-constraint nil)))))

(deftest test-user-defined-constraint
  (setup)
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
  (let [framework-id "my-framework-id"
        k8s-gpu-offer {:id "my-offer-id"
                       :framework-id framework-id
                       :slave-id "my-slave-id",
                       :hostname "slave3",
                       :resources [{:name "cpus", :type :value-scalar, :scalar 40.0, :ranges [], :set #{}, :role "*"}
                                   {:name "mem", :type :value-scalar, :scalar 5000.0, :ranges [], :set #{}, :role "*"}
                                   {:name "disk", :type :value-scalar, :scalar 6000.0, :ranges [], :set #{}, :role "*"}
                                   {:name "ports", :type :value-ranges, :scalar 0.0, :ranges [{:begin 31000, :end 32000}], :set #{}, :role "*"}
                                   {:name "gpus", :type :value-text->scalar :text->scalar {"nvidia-tesla-p100" 4} :role "*"}],
                       :attributes [{:name "compute-cluster-type", :type :value-text, :text "kubernetes" :role "*"}],
                       :executor-ids []}
        k8s-non-gpu-offer {:id "my-offer-id"
                           :framework-id framework-id
                           :slave-id "my-slave-id",
                           :hostname "slave3",
                           :resources [{:name "cpus", :type :value-scalar, :scalar 40.0, :ranges [], :set #{}, :role "*"}
                                       {:name "mem", :type :value-scalar, :scalar 5000.0, :ranges [], :set #{}, :role "*"}
                                       {:name "disk", :type :value-scalar, :scalar 6000.0, :ranges [], :set #{}, :role "*"}
                                       {:name "ports", :type :value-ranges, :scalar 0.0, :ranges [{:begin 31000, :end 32000}], :set #{}, :role "*"}
                                       {:name "gpus", :type :value-text->scalar :text->scalar {} :role "*"}],
                           :attributes [{:name "compute-cluster-type", :type :value-text, :text "kubernetes" :role "*"}],
                           :executor-ids []}
        uri "datomic:mem://test-gpu-constraint"
        conn (restore-fresh-database! uri)
        _ (create-pool conn "test-pool")
        _ (create-pool conn "mesos-pool")
        gpu-job-id-1 (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 1 :pool "test-pool" :env {"COOK_GPU_MODEL" "nvidia-tesla-p100"})
        gpu-job-id-2 (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 8 :pool "test-pool" :env {"COOK_GPU_MODEL" "nvidia-tesla-p100"})
        gpu-job-id-3 (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 4 :pool "test-pool" :env {"COOK_GPU_MODEL" "nvidia-tesla-k80"})
        gpu-job-id-4 (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 4 :pool "test-pool" :env {"COOK_GPU_MODEL" "nvidia-tesla-p100"})
        gpu-job-id-5 (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 4 :pool "test-pool")
        non-gpu-job-id (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 0 :pool "test-pool")
        mesos-gpu-job-id (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 1.0 :pool "mesos-pool")
        mesos-non-gpu-job-id (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :gpus 0.0 :pool "mesos-pool")
        db (db conn)
        gpu-job-1 (d/entity db gpu-job-id-1)
        gpu-job-2 (d/entity db gpu-job-id-2)
        gpu-job-3 (d/entity db gpu-job-id-3)
        gpu-job-4 (d/entity db gpu-job-id-4)
        gpu-job-5 (d/entity db gpu-job-id-5)
        non-gpu-job (d/entity db non-gpu-job-id)
        mesos-gpu-job (d/entity db mesos-gpu-job-id)
        mesos-non-gpu-job (d/entity db mesos-non-gpu-job-id)
        gpu-task-assignment (-> gpu-job-5
                                make-task-request
                                make-task-assignment-result)]

    (with-redefs [config/valid-gpu-models (constantly [{:pool-regex "test-pool"
                                                        :valid-models #{"nvidia-tesla-p100"}
                                                        :default-model "nvidia-tesla-p100"}])]
      (is (not (.isSuccessful
                 (.evaluate (constraints/fenzoize-job-constraint (constraints/build-gpu-host-constraint gpu-job-1))
                            (sched/make-task-request db gpu-job-1 nil)
                            (reify com.netflix.fenzo.VirtualMachineCurrentState
                              (getHostname [_] "test-host")
                              (getRunningTasks [_] [])
                              (getTasksCurrentlyAssigned [_] [])
                              (getCurrAvailableResources [_] (offer/offer->lease k8s-gpu-offer 0)))
                            nil)))
          (str "GPU task on GPU host with too many GPUs should fail"))
      (is (not (.isSuccessful
                 (.evaluate (constraints/fenzoize-job-constraint (constraints/build-gpu-host-constraint gpu-job-2))
                            (sched/make-task-request db gpu-job-2 nil)
                            (reify com.netflix.fenzo.VirtualMachineCurrentState
                              (getHostname [_] "test-host")
                              (getRunningTasks [_] [])
                              (getTasksCurrentlyAssigned [_] [])
                              (getCurrAvailableResources [_] (offer/offer->lease k8s-gpu-offer 0)))
                            nil)))
          (str "GPU task on GPU host with too little GPUs should fail"))
      (is (not (.isSuccessful
                 (.evaluate (constraints/fenzoize-job-constraint (constraints/build-gpu-host-constraint gpu-job-3))
                            (sched/make-task-request db gpu-job-3 nil)
                            (reify com.netflix.fenzo.VirtualMachineCurrentState
                              (getHostname [_] "test-host")
                              (getRunningTasks [_] [])
                              (getTasksCurrentlyAssigned [_] [])
                              (getCurrAvailableResources [_] (offer/offer->lease k8s-gpu-offer 0)))
                            nil)))
          (str "GPU task on GPU host with correct number of GPUs but without correct GPU models should fail"))
      (is (.isSuccessful
            (.evaluate (constraints/fenzoize-job-constraint (constraints/build-gpu-host-constraint gpu-job-4))
                       (sched/make-task-request db gpu-job-4 nil)
                       (reify com.netflix.fenzo.VirtualMachineCurrentState
                         (getHostname [_] "test-host")
                         (getRunningTasks [_] [])
                         (getTasksCurrentlyAssigned [_] [])
                         (getCurrAvailableResources [_] (offer/offer->lease k8s-gpu-offer 0)))
                       nil))
          (str "GPU task on GPU host with the correct number of GPUs should succeed"))
      (is (not (.isSuccessful
                 (.evaluate (constraints/fenzoize-job-constraint (constraints/build-gpu-host-constraint gpu-job-4))
                            (sched/make-task-request db gpu-job-4 nil)
                            (reify com.netflix.fenzo.VirtualMachineCurrentState
                              (getHostname [_] "test-host")
                              (getRunningTasks [_] [])
                              (getTasksCurrentlyAssigned [_] [gpu-task-assignment])
                              (getCurrAvailableResources [_] (offer/offer->lease k8s-gpu-offer 0)))
                            nil)))
          (str "We only allow one GPU job per VM. This VM already has a GPU job assigned to it, so it should not get any additional GPU tasks."))
      (is (not (.isSuccessful
                 (.evaluate (constraints/fenzoize-job-constraint (constraints/build-gpu-host-constraint non-gpu-job))
                            (sched/make-task-request db non-gpu-job nil)
                            (reify com.netflix.fenzo.VirtualMachineCurrentState
                              (getHostname [_] "test-host")
                              (getRunningTasks [_] [])
                              (getTasksCurrentlyAssigned [_] [])
                              (getCurrAvailableResources [_] (offer/offer->lease k8s-gpu-offer 0)))
                            nil)))
          (str "non GPU task on GPU host should fail"))
      (is (not (.isSuccessful
                 (.evaluate (constraints/fenzoize-job-constraint (constraints/build-gpu-host-constraint gpu-job-1))
                            (sched/make-task-request db gpu-job-1 nil)
                            (reify com.netflix.fenzo.VirtualMachineCurrentState
                              (getHostname [_] "test-host")
                              (getRunningTasks [_] [])
                              (getTasksCurrentlyAssigned [_] [])
                              (getCurrAvailableResources [_] (offer/offer->lease k8s-non-gpu-offer 0)))
                            nil)))
          "GPU task on non GPU host should fail")
      (is (.isSuccessful
            (.evaluate (constraints/fenzoize-job-constraint (constraints/build-gpu-host-constraint non-gpu-job))
                       (sched/make-task-request db non-gpu-job nil)
                       (reify com.netflix.fenzo.VirtualMachineCurrentState
                         (getHostname [_] "test-host")
                         (getRunningTasks [_] [])
                         (getTasksCurrentlyAssigned [_] [])
                         (getCurrAvailableResources [_] (offer/offer->lease k8s-non-gpu-offer 0)))
                       nil))
          "non GPU task on non GPU host should succeed")
      (is (not (.isSuccessful
                 (.evaluate (constraints/fenzoize-job-constraint (constraints/build-gpu-host-constraint mesos-gpu-job))
                            (sched/make-task-request db mesos-gpu-job nil)
                            (reify com.netflix.fenzo.VirtualMachineCurrentState
                              (getHostname [_] "test-host")
                              (getRunningTasks [_] [])
                              (getTasksCurrentlyAssigned [_] [])
                              (getCurrAvailableResources [_] (offer/offer->lease mesos-gpu-job 0)))
                            nil)))
          "GPU task on mesos GPU host should fail")
      (is (.isSuccessful
            (.evaluate (constraints/fenzoize-job-constraint (constraints/build-gpu-host-constraint mesos-non-gpu-job))
                       (sched/make-task-request db mesos-non-gpu-job nil)
                       (reify com.netflix.fenzo.VirtualMachineCurrentState
                         (getHostname [_] "test-host")
                         (getRunningTasks [_] [])
                         (getTasksCurrentlyAssigned [_] [])
                         (getCurrAvailableResources [_] (offer/offer->lease mesos-non-gpu-job 0)))
                       nil))
          "non GPU task on non GPU mesos host should succeed"))
    (is (.isSuccessful
          (.evaluate (constraints/fenzoize-job-constraint (constraints/build-gpu-host-constraint non-gpu-job))
                     (sched/make-task-request db non-gpu-job nil)
                     (reify com.netflix.fenzo.VirtualMachineCurrentState
                       (getHostname [_] "test-host")
                       (getRunningTasks [_] [])
                       (getTasksCurrentlyAssigned [_] [])
                       (getCurrAvailableResources [_] (offer/offer->lease k8s-non-gpu-offer 0)))
                     nil))
        "non GPU task on non GPU host should succeed")))

(deftest test-disk-constraint
  (cook.test.testutil/setup)
  (let [framework-id "my-framework-id"
        offer {:id "my-offer-id"
               :framework-id framework-id
               :slave-id "my-slave-id",
               :hostname "slave3",
               :resources [{:name "cpus", :type :value-scalar, :scalar 40.0, :ranges [], :set #{}, :role "*"}
                           {:name "mem", :type :value-scalar, :scalar 5000.0, :ranges [], :set #{}, :role "*"}
                           {:name "disk", :type :value-text->scalar, :text->scalar {"pd-standard" 50}, :ranges [], :set #{}, :role "*"}
                           {:name "ports", :type :value-ranges, :scalar 0.0, :ranges [{:begin 31000, :end 32000}], :set #{}, :role "*"}
                           {:name "gpus", :type :value-text->scalar :text->scalar {} :role "*"}],
               :attributes [{:name "compute-cluster-type", :type :value-text, :text "kubernetes" :role "*"}],
               :executor-ids []}
        uri "datomic:mem://test-disk-constraint"
        conn (restore-fresh-database! uri)
        _ (create-pool conn "test-pool")
        _ (create-pool conn "mesos-pool")

        disk-request-job-id-1 (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :pool "test-pool" :disk {:request 10.0 :limit 20.0 :type "standard"})
        disk-request-job-id-2 (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :pool "test-pool" :disk {:request 50.0 :limit 60.0})
        disk-request-job-id-3 (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :pool "test-pool" :disk {:request 100.0 :type "standard"})
        disk-request-job-id-4 (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :pool "test-pool" :disk {:request 10.0 :type "pd-ssd"})
        disk-request-job-id-5 (create-dummy-job conn :user "ljin" :ncpus 5.0 :memory 5.0 :pool "mesos-pool")
        db (db conn)
        disk-request-job-1 (d/entity db disk-request-job-id-1)
        disk-request-job-2 (d/entity db disk-request-job-id-2)
        disk-request-job-3 (d/entity db disk-request-job-id-3)
        disk-request-job-4 (d/entity db disk-request-job-id-4)
        disk-request-job-5 (d/entity db disk-request-job-id-5)]

    (with-redefs [config/disk (constantly [{:pool-regex "test-pool"
                                            :max-size 256000.0
                                            :valid-types #{"standard", "pd-ssd"}
                                            :default-type "standard"
                                            :default-request 10000.0
                                            :type-map {"standard", "pd-standard"}
                                            :enable-constraint? true}])]
      (is (.isSuccessful
            (.evaluate (constraints/fenzoize-job-constraint (constraints/build-disk-host-constraint disk-request-job-1))
                       (sched/make-task-request db disk-request-job-1 nil)
                       (reify com.netflix.fenzo.VirtualMachineCurrentState
                         (getHostname [_] "test-host")
                         (getRunningTasks [_] [])
                         (getTasksCurrentlyAssigned [_] [])
                         (getCurrAvailableResources [_] (offer/offer->lease offer 0)))
                       nil))
          (str "Disk task on host with enough disk and correct type should succeed"))
      (is (.isSuccessful
            (.evaluate (constraints/fenzoize-job-constraint (constraints/build-disk-host-constraint disk-request-job-2))
                       (sched/make-task-request db disk-request-job-2 nil)
                       (reify com.netflix.fenzo.VirtualMachineCurrentState
                         (getHostname [_] "test-host")
                         (getRunningTasks [_] [])
                         (getTasksCurrentlyAssigned [_] [])
                         (getCurrAvailableResources [_] (offer/offer->lease offer 0)))
                       nil))
          (str "Disk task on host with enough disk and correct type should succeed"))
      (is (not (.isSuccessful
                 (.evaluate (constraints/fenzoize-job-constraint (constraints/build-disk-host-constraint disk-request-job-3))
                            (sched/make-task-request db disk-request-job-3 nil)
                            (reify com.netflix.fenzo.VirtualMachineCurrentState
                              (getHostname [_] "test-host")
                              (getRunningTasks [_] [])
                              (getTasksCurrentlyAssigned [_] [])
                              (getCurrAvailableResources [_] (offer/offer->lease offer 0)))
                            nil)))
          (str "Disk task on host without enough disk should fail"))
      (is (not (.isSuccessful
                 (.evaluate (constraints/fenzoize-job-constraint (constraints/build-disk-host-constraint disk-request-job-4))
                            (sched/make-task-request db disk-request-job-4 nil)
                            (reify com.netflix.fenzo.VirtualMachineCurrentState
                              (getHostname [_] "test-host")
                              (getRunningTasks [_] [])
                              (getTasksCurrentlyAssigned [_] [])
                              (getCurrAvailableResources [_] (offer/offer->lease offer 0)))
                            nil)))
          (str "Disk task on host without correct disk type should fail"))
      (is (nil? (constraints/build-disk-host-constraint disk-request-job-5))))))

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
                                                       #mesomatic.types.Resource{:name "disk", :type :value-text->scalar, :text->scalar {"standard" 6000.0}, :ranges [], :set #{}, :role "*"}
                                                       #mesomatic.types.Resource{:name "ports", :type :value-ranges, :scalar 0.0, :ranges [#mesomatic.types.ValueRange{:begin 31000, :end 32000}], :set #{}, :role "*"}],
                                           :attributes [],
                                           :executor-ids []}
        hostB-offer #mesomatic.types.Offer{:id #mesomatic.types.OfferID {:value "my-offer-id"}
                                           :framework-id framework-id
                                           :slave-id #mesomatic.types.SlaveID{:value "my-slave-id"},
                                           :hostname "hostB",
                                           :resources [#mesomatic.types.Resource{:name "cpus", :type :value-scalar, :scalar 40.0, :ranges [], :set #{}, :role "*"}
                                                       #mesomatic.types.Resource{:name "mem", :type :value-scalar, :scalar 5000.0, :ranges [], :set #{}, :role "*"}
                                                       #mesomatic.types.Resource{:name "disk", :type :value-text->scalar, :text->scalar {"standard" 6000.0}, :ranges [], :set #{}, :role "*"}
                                                       #mesomatic.types.Resource{:name "ports", :type :value-ranges, :scalar 0.0, :ranges [#mesomatic.types.ValueRange{:begin 31000, :end 32000}], :set #{}, :role "*"}],
                                           :attributes [],
                                           :executor-ids []}
        constraint (constraints/build-rebalancer-reservation-constraint reserved-hosts)]
    (is (not (.isSuccessful
              (.evaluate constraint
                         (sched/make-task-request db job-id nil)
                         (reify com.netflix.fenzo.VirtualMachineCurrentState
                           (getHostname [_] "hostB")
                           (getRunningTasks [_] [])
                           (getTasksCurrentlyAssigned [_] [])
                           (getCurrAvailableResources [_]  (offer/offer->lease hostB-offer 0)))
                         nil))))
    (is (.isSuccessful
         (.evaluate constraint
                    (sched/make-task-request db job-id nil)
                    (reify com.netflix.fenzo.VirtualMachineCurrentState
                      (getHostname [_] "hostA")
                      (getRunningTasks [_] [])
                      (getTasksCurrentlyAssigned [_] [])
                      (getCurrAvailableResources [_]  (offer/offer->lease hostA-offer 0)))
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

(deftest test-job->previous-hosts-to-avoid
  (testing "uniqueness"
    (let [hostnames (constraints/job->previous-hosts-to-avoid
                      {:job/instance [{:instance/hostname "host-3"}
                                      {:instance/hostname "host-2"}
                                      {:instance/hostname "host-1"}
                                      {:instance/hostname "host-3"}
                                      {:instance/hostname "host-2"}
                                      {:instance/hostname "host-1"}]})]
      (is (= 3 (count hostnames)))
      (is (= (set ["host-1" "host-2" "host-3"]) (set hostnames))))))

(deftest test-job->constraints
  (let [default-constraints [{:constraint/attribute "cpu-architecture"
                              :constraint/operator :constraint.operator/equals
                              :constraint/pattern "intel-haswell"}
                             {:constraint/attribute "node-family"
                              :constraint/operator :constraint.operator/equals
                              :constraint/pattern "n1"}]]
    (with-redefs [config/default-job-constraints
                  (constantly [{:pool-regex ".*"
                                :default-constraints default-constraints}])]

      (testing "default constraints"
        (is (= default-constraints (constraints/job->constraints {}))))

      (testing "default constraints plus user constraint"
        (let [user-constraints [{:constraint/attribute "foo"
                                 :constraint/operator :constraint.operator/equals
                                 :constraint/pattern "bar"}]]
          (is (= (concat user-constraints default-constraints)
                 (constraints/job->constraints
                   {:job/constraint user-constraints})))))

      (testing "removes default machine-type constraints if user specified any"
        (let [user-constraints [{:constraint/attribute "node-type"
                                 :constraint/operator :constraint.operator/equals
                                 :constraint/pattern "c2-standard-30"}]]
          (is (= user-constraints
                 (constraints/job->constraints
                   {:job/constraint user-constraints}))))))

    (testing "constraint transformations"
      (with-redefs [config/constraint-attribute->transformation
                    (constantly {"a" {:new-attribute "z"
                                      :pattern-transformations [{:match #"^bcd-" :replacement ""}]}})]
        (let [user-constraints [{:constraint/attribute "a"
                                 :constraint/operator :constraint.operator/equals
                                 :constraint/pattern "bcd-efg"}]]
          (is (= [{:constraint/attribute "z"
                   :constraint/operator :constraint.operator/equals
                   :constraint/pattern "efg"}]
                (constraints/job->constraints
                  {:job/constraint user-constraints}))))))))

(deftest test-transform-constraints
  (testing "basic transformation"
    (is
      (=
        [{:constraint/attribute "z" :constraint/pattern "efg"}]
        (cook.scheduler.constraints/transform-constraints
          [{:constraint/attribute "a" :constraint/pattern "bcd-efg"}]
          {"a" {:new-attribute "z" :pattern-transformations [{:match #"^bcd-" :replacement ""}]}}))))

  (testing "multiple transformations"
    (is
      (=
        [{:constraint/attribute "z" :constraint/pattern "something else"}]
        (cook.scheduler.constraints/transform-constraints
          [{:constraint/attribute "a" :constraint/pattern "bcd-efg"}]
          {"a" {:new-attribute "z"
                :pattern-transformations
                [{:match #"^bcd-" :replacement ""}
                 {:match #"^efg$" :replacement "something else"}]}}))))

  (testing "no transformations"
    (is
      (=
        [{:constraint/attribute "a" :constraint/pattern "bcd-efg"}]
        (cook.scheduler.constraints/transform-constraints
          [{:constraint/attribute "a" :constraint/pattern "bcd-efg"}]
          nil))))

  (testing "no matching transformation"
    (is
      (=
        [{:constraint/attribute "h" :constraint/pattern "bcd-efg"}]
        (cook.scheduler.constraints/transform-constraints
          [{:constraint/attribute "h" :constraint/pattern "bcd-efg"}]
          {"a" {}}))))

  (testing "no constraints"
    (is
      (=
        []
        (cook.scheduler.constraints/transform-constraints
          nil
          {"a" {}})))))