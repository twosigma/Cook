(ns cook.test.mesos.mesos-mock
  (:require [chime :refer [chime-ch]]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [cook.compute-cluster :as cc]
            [cook.mesos.mesos-mock :as mm]
            [cook.test.postgres]
            [cook.scheduler.share :as share]
            [cook.test.testutil :refer [create-dummy-job poll-until restore-fresh-database! setup]]
            [cook.test.zz-simulator :refer [dump-jobs-to-csv pull-all-task-ents with-cook-scheduler]]
            [cook.util :as util]
            [datomic.api :as d]
            [mesomatic.scheduler :as mesos]
            [mesomatic.types :as mesos-types])
  (:import (java.util UUID)))

(use-fixtures :once cook.test.postgres/with-pg-db)

(defn string->uuid
  "Parses the uuid if `uuid-str` is a uuid, returns nil otherwise"
  [uuid-str]
  (try
    (UUID/fromString uuid-str)
    (catch Exception e
      nil)))

(defn noop-mock-scheduler
  []
  (mesos/scheduler
    (registered [this driver framework-id master-info])
    (reregistered [this driver master-info])
    (offer-rescinded [this driver offer-id])
    (framework-message [this driver executor-id slave-id data])
    (disconnected [this driver])
    (slave-lost [this driver slave-id])
    (executor-lost [this driver executor-id slave-id status])
    (error [this driver message])
    (resource-offers [this driver offers])
    (status-update [this driver status])))

(deftest make-offer
  (let [basic-host-info {:hostname "hosta"
                         :attributes {}
                         :resources {:cpus {"*" 10}
                                     :mem {"*" 100000}
                                     :ports {"*" [{:begin 10000 :end 20000}]}}
                         :available-resources {:cpus {"*" 10}
                                               :mem {"*" 100000}
                                               :ports {"*" [{:begin 10000 :end 20000}]}}
                         :slave-id "e8ab3892-3023-4546-888d-67cc13eac3cc"}
        offer (mm/make-offer basic-host-info)]
    (is (string->uuid (-> offer :id :value)))
    (is (= (dissoc offer :id)
           {:hostname "hosta"
            :attributes {}
            :resources [{:name "cpus", :role "*", :scalar 10 :type :value-scalar}
                        {:name "mem", :role "*", :scalar 100000 :type :value-scalar}
                        {:name "ports", :role "*", :ranges [{:begin 10000, :end 20000}] :type :value-ranges}]
            :slave-id {:value "e8ab3892-3023-4546-888d-67cc13eac3cc"}}))))

(deftest prepare-new-offers
  (let [state {:slave-id->host
               {"e8ab3892-3023-4546-888d-67cc13eac3cc"
                {:hostname "hosta"
                 :attributes {}
                 :resources {:cpus {"*" 10}
                             :mem {"*" 100000}
                             :ports {"*" [{:begin 10000 :end 20000}]}}
                 :available-resources {:cpus {"*" 10}
                                       :mem {"*" 100000}
                                       :ports {"*" [{:begin 10000 :end 20000}]}}
                 :slave-id "e8ab3892-3023-4546-888d-67cc13eac3cc"}

                "a5cb3212-3023-4546-888d-67cc13eac3dd"
                {:hostname "hostb"
                 :attributes {}
                 :resources {:cpus {"*" 12}
                             :mem {"*" 120000}
                             :ports {"*" [{:begin 10000 :end 20000}]}}
                 :available-resources {:cpus {}
                                       :mem {}
                                       :ports {}}
                 :slave-id "a5cb3212-3023-4546-888d-67cc13eac3dd"}}
               :offer-id->offer
               {"8fe5bf6c-ea39-4700-b1f6-9fd8a0271d7c"
                {:id {:value "8fe5bf6c-ea39-4700-b1f6-9fd8a0271d7c"}
                 :slave-id {:value "a5cb3212-3023-4546-888d-67cc13eac3dd"}
                 :hostname "hostb"
                 :resources [{:name "cpus" :role "*" :scalar 12 :type :value-scalar}
                             {:name "mem" :role "*" :scalar 120000 :type :value-scalar}
                             {:name "ports" :role "*" :ranges [{:begin 10000 :end 20000}] :type :value-ranges}]}}
               :task-id->task {}}
        [new-offers new-state] (mm/prepare-new-offers state)]
    (is (= (count new-offers) 1))
    (is (= (:slave-id->host new-state)
           {"e8ab3892-3023-4546-888d-67cc13eac3cc"
            {:hostname "hosta"
             :attributes {}
             :resources {:cpus {"*" 10}
                         :mem {"*" 100000}
                         :ports {"*" [{:begin 10000 :end 20000}]}}
             :available-resources {:cpus {}
                                   :mem {}
                                   :ports {}}
             :slave-id "e8ab3892-3023-4546-888d-67cc13eac3cc"}

            "a5cb3212-3023-4546-888d-67cc13eac3dd"
            {:hostname "hostb"
             :attributes {}
             :resources {:cpus {"*" 12}
                         :mem {"*" 120000}
                         :ports {"*" [{:begin 10000 :end 20000}]}}
             :available-resources {:cpus {}
                                   :mem {}
                                   :ports {}}
             :slave-id "a5cb3212-3023-4546-888d-67cc13eac3dd"}}))
    (is (= (count (vals (:offer-id->offer new-state))) 2))))

(deftest combine-ranges
  (is (= (mm/combine-ranges [{:begin 16 :end 20} {:begin 0 :end 2}
                             {:begin 3 :end 5} {:begin 8 :end 10}])
         [{:begin 0 :end 5} {:begin 8 :end 10} {:begin 16 :end 20}])))

(deftest combine-resources
  (is (= (mm/combine-resources [{:name "cpus" :role "*" :scalar 1 :type :value-scalar}
                                {:name "cpus" :role "*" :scalar 6 :type :value-scalar}
                                {:name "mem" :role "*" :scalar 60000 :type :value-scalar}
                                {:name "ports" :role "*" :ranges [{:begin 10000 :end 15000}] :type :value-ranges}
                                {:name "ports" :role "*" :ranges [{:begin 15001 :end 15003}] :type :value-ranges}])
         {:cpus {"*" 7}
          :mem {"*" 60000}
          :ports {"*" [{:begin 10000 :end 15003}]}})))

(deftest range-contains?
  (testing "ranges are the same"
    (is (mm/range-contains? {:begin 0 :end 5} {:begin 0 :end 5})))
  (testing "range larger on left"
    (is (mm/range-contains? {:begin 0 :end 5} {:begin 1 :end 5})))
  (testing "range strictly contains"
    (is (mm/range-contains? {:begin 0 :end 5} {:begin 1 :end 4})))
  (testing "range contains single number"
    (is (mm/range-contains? {:begin 0 :end 5} {:begin 4 :end 4})))
  (testing "range exceeds on left"
    (is (not (mm/range-contains? {:begin 0 :end 5} {:begin -1 :end 5}))))
  (testing "range exceeds on right"
    (is (not (mm/range-contains? {:begin 0 :end 5} {:begin 1 :end 6}))))
  (testing "range not contained completely on right"
    (is (not (mm/range-contains? {:begin 0 :end 5} {:begin 6 :end 8}))))
  (testing "range not contained completely on left"
    (is (not (mm/range-contains? {:begin 0 :end 5} {:begin -2 :end -1})))))

(deftest ranges-contains?
  (is (mm/ranges-contains? [{:begin 0 :end 5} {:begin 7 :end 10}] {:begin 7 :end 8}))
  (is (not (mm/ranges-contains? [{:begin 0 :end 5} {:begin 7 :end 10}] {:begin 5 :end 7}))))

(deftest subtract-range
  (testing "Full overlap"
    (is (= (mm/subtract-range {:begin 0 :end 5} {:begin 0 :end 5})
           [])))
  (testing "Equal on left"
    (is (= (mm/subtract-range {:begin 0 :end 5} {:begin 0 :end 4})
           [{:begin 5 :end 5}])))
  (testing "Equal on right"
    (is (= (mm/subtract-range {:begin 0 :end 5} {:begin 1 :end 5})
           [{:begin 0 :end 0}])))
  (testing "Remove from middle"
    (is (= (mm/subtract-range {:begin 0 :end 5} {:begin 2 :end 3})
           [{:begin 0 :end 1} {:begin 4 :end 5}]))))

(deftest subtract-ranges
  (is (= (mm/subtract-ranges [{:begin 0 :end 10}] [{:begin 0 :end 2} {:begin 4 :end 5} {:begin 7 :end 8}])
         [{:begin 3 :end 3} {:begin 6 :end 6} {:begin 9 :end 10}])))

(deftest subtract-resources
  (testing "Full subtraction test"
    (is (= (mm/subtract-resources {:cpus {"*" 7
                                          "cook" 3}
                                   :mem {"*" 60000}
                                   :ports {"*" [{:begin 10000 :end 15003} {:begin 16000 :end 20000}]
                                           "cook" [{:begin 100 :end 200}]}}
                                  {:cpus {"*" 4
                                          "cook" 2}
                                   :mem {"*" 30000}
                                   :ports {"*" [{:begin 10000 :end 14003} {:begin 17000 :end 20000}]
                                           "cook" [{:begin 100 :end 200}]}})
           {:cpus {"*" 3
                   "cook" 1}
            :mem {"*" 30000}
            :ports {"*" [{:begin 14004 :end 15003} {:begin 16000 :end 16999}]
                    "cook" []}})))
  (testing "not all resources subtracted"
    (is (= (mm/subtract-resources {:cpus {"*" 7
                                          "cook" 3}
                                   :mem {"*" 60000}
                                   :ports {"*" [{:begin 10000 :end 15003} {:begin 16000 :end 20000}]
                                           "cook" [{:begin 100 :end 200}]}}
                                  {:cpus {"*" 4
                                          "cook" 2}
                                   :mem {"*" 30000}})
           {:cpus {"*" 3
                   "cook" 1}
            :mem {"*" 30000}
            :ports {"*" [{:begin 10000 :end 15003} {:begin 16000 :end 20000}]
                    "cook" [{:begin 100 :end 200}]}}))))

(deftest handle-decline-action
  (let [offer-id "8fe5bf6c-ea39-4700-b1f6-9fd8a0271d7c"
        state {:slave-id->host
               {"e8ab3892-3023-4546-888d-67cc13eac3cc"
                {:hostname "hosta"
                 :attributes {}
                 :resources {:cpus {"*" 10}
                             :mem {"*" 100000}
                             :ports {"*" [{:begin 10000 :end 20000}]}}
                 :available-resources {:cpus {"*" 10}
                                       :mem {"*" 100000}
                                       :ports {"*" [{:begin 10000 :end 20000}]}}
                 :slave-id "e8ab3892-3023-4546-888d-67cc13eac3cc"}

                "a5cb3212-3023-4546-888d-67cc13eac3dd"
                {:hostname "hostb"
                 :attributes {}
                 :resources {:cpus {"*" 12}
                             :mem {"*" 120000}
                             :ports {"*" [{:begin 10000 :end 30000}]}}
                 :available-resources {:cpus {}
                                       :mem {}
                                       :ports {}}
                 :slave-id "a5cb3212-3023-4546-888d-67cc13eac3dd"}}
               :offer-id->offer
               {offer-id
                {:id {:value offer-id}
                 :slave-id {:value "a5cb3212-3023-4546-888d-67cc13eac3dd"}
                 :hostname "hostb"
                 :resources [{:name "cpus" :role "*" :scalar 7 :type :value-scalar}
                             {:name "mem" :role "*" :scalar 70000 :type :value-scalar}
                             {:name "ports" :role "*" :ranges [{:begin 10000 :end 20000}] :type :value-ranges}]}
                "45cb7812-1233-4546-888d-67cc13eac398"
                {:id {:value "45cb7812-1233-4546-888d-67cc13eac398"}
                 :slave-id {:value "a5cb3212-3023-4546-888d-67cc13eac3dd"}
                 :hostname "hostb"
                 :resources [{:name "cpus" :role "*" :scalar 5 :type :value-scalar}
                             {:name "mem" :role "*" :scalar 50000 :type :value-scalar}
                             {:name "ports" :role "*" :ranges [{:begin 20001 :end 30000}] :type :value-ranges}]}}
               :task-id->task {}}
        new-state (mm/handle-action! :decline offer-id state nil (noop-mock-scheduler))]
    (is (= (count (:offer-id->offer new-state)) 1))
    (is (= (get-in new-state [:slave-id->host "a5cb3212-3023-4546-888d-67cc13eac3dd"])
           {:hostname "hostb"
            :attributes {}
            :resources {:cpus {"*" 12}
                        :mem {"*" 120000}
                        :ports {"*" [{:begin 10000 :end 30000}]}}
            :available-resources {:cpus {"*" 7}
                                  :mem {"*" 70000}
                                  :ports {"*" [{:begin 10000 :end 20000}]}}
            :slave-id "a5cb3212-3023-4546-888d-67cc13eac3dd"}))))

(deftest handle-kill-action
  (let [task-id "45cb7812-1233-4546-888d-67cc13eac398"
        state {:slave-id->host
               {"e8ab3892-3023-4546-888d-67cc13eac3cc"
                {:hostname "hosta"
                 :attributes {}
                 :resources {:cpus {"*" 10}
                             :mem {"*" 100000}
                             :ports {"*" [{:begin 10000 :end 20000}]}}
                 :available-resources {:cpus {"*" 10}
                                       :mem {"*" 100000}
                                       :ports {"*" [{:begin 10000 :end 20000}]}}
                 :slave-id "e8ab3892-3023-4546-888d-67cc13eac3cc"}

                "a5cb3212-3023-4546-888d-67cc13eac3dd"
                {:hostname "hostb"
                 :attributes {}
                 :resources {:cpus {"*" 12}
                             :mem {"*" 120000}
                             :ports {"*" [{:begin 10000 :end 30000}]}}
                 :available-resources {:cpus {"*" 7}
                                       :mem {"*" 70000}
                                       :ports {"*" [{:begin 10000 :end 20000}]}}
                 :slave-id "a5cb3212-3023-4546-888d-67cc13eac3dd"}}
               :offer-id->offer {}
               :task-id->task
               {task-id
                {:task-id {:value task-id}
                 :slave-id {:value "a5cb3212-3023-4546-888d-67cc13eac3dd"}
                 :name "to-kill"
                 :resources [{:name "cpus" :role "*" :scalar 5 :type :value-scalar}
                             {:name "mem" :role "*" :scalar 50000 :type :value-scalar}
                             {:name "ports" :role "*" :ranges [{:begin 20001 :end 30000}] :type :value-ranges}]
                 :labels {}
                 :data ""}}}
        new-state (mm/handle-action! :kill-task task-id state nil (noop-mock-scheduler))]
    (is (= (count (:task-id->task new-state)) 0))
    (is (= (get-in new-state [:slave-id->host "a5cb3212-3023-4546-888d-67cc13eac3dd"])
           {:hostname "hostb"
            :attributes {}
            :resources {:cpus {"*" 12}
                        :mem {"*" 120000}
                        :ports {"*" [{:begin 10000 :end 30000}]}}
            :available-resources {:cpus {"*" 12}
                                  :mem {"*" 120000}
                                  :ports {"*" [{:begin 10000 :end 30000}]}}
            :slave-id "a5cb3212-3023-4546-888d-67cc13eac3dd"}))))


(deftest handle-launch-action
  (let [tasks [{:task-id {:value "45cb7812-1233-4546-888d-67cc13eac398"}
                :slave-id {:value "a5cb3212-3023-4546-888d-67cc13eac3dd"}
                :name "to-launch"
                :executor {:executor-id {:value "executor-id"}
                           :framework-id {:value "framework-id"}
                           :name "dummy-executor"
                           :command {:value "10000"
                                     :environment {:variables [{:name "EXECUTION_TIME"
                                                                :value "10000"}]}
                                     :user "user-A"
                                     :uris []}}
                :resources [{:name "cpus" :role "*" :scalar 8 :type :value-scalar}
                            {:name "mem" :role "*" :scalar 80000 :type :value-scalar}
                            {:name "ports" :role "*" :ranges [{:begin 12001 :end 25000}] :type :value-ranges}]
                :labels {}
                :data nil}]
        tasks (map (comp mesos-types/pb->data (partial mesos-types/->pb :TaskInfo)) tasks)
        offer-ids ["8fe5bf6c-ea39-4700-b1f6-9fd8a0271d7c" "45cb7812-1233-4546-888d-67cc13eac398"]
        state {:config {:task->runtime-ms mm/default-task->runtime-ms}
               :slave-id->host
               {"e8ab3892-3023-4546-888d-67cc13eac3cc"
                {:hostname "hosta"
                 :attributes {}
                 :resources {:cpus {"*" 10}
                             :mem {"*" 100000}
                             :ports {"*" [{:begin 10000 :end 20000}]}}
                 :available-resources {:cpus {"*" 10}
                                       :mem {"*" 100000}
                                       :ports {"*" [{:begin 10000 :end 20000}]}}
                 :slave-id "e8ab3892-3023-4546-888d-67cc13eac3cc"}

                "a5cb3212-3023-4546-888d-67cc13eac3dd"
                {:hostname "hostb"
                 :attributes {}
                 :resources {:cpus {"*" 12}
                             :mem {"*" 120000}
                             :ports {"*" [{:begin 10000 :end 30000}]}}
                 :available-resources {:cpus {}
                                       :mem {}
                                       :ports {}}
                 :slave-id "a5cb3212-3023-4546-888d-67cc13eac3dd"}}
               :offer-id->offer
               {"8fe5bf6c-ea39-4700-b1f6-9fd8a0271d7c"
                {:id {:value "8fe5bf6c-ea39-4700-b1f6-9fd8a0271d7c"}
                 :slave-id {:value "a5cb3212-3023-4546-888d-67cc13eac3dd"}
                 :hostname "hostb"
                 :resources [{:name "cpus" :role "*" :scalar 7 :type :value-scalar}
                             {:name "mem" :role "*" :scalar 70000 :type :value-scalar}
                             {:name "ports" :role "*" :ranges [{:begin 10000 :end 20000}] :type :value-ranges}]}
                "45cb7812-1233-4546-888d-67cc13eac398"
                {:id {:value "45cb7812-1233-4546-888d-67cc13eac398"}
                 :slave-id {:value "a5cb3212-3023-4546-888d-67cc13eac3dd"}
                 :hostname "hostb"
                 :resources [{:name "cpus" :role "*" :scalar 5 :type :value-scalar}
                             {:name "mem" :role "*" :scalar 50000 :type :value-scalar}
                             {:name "ports" :role "*" :ranges [{:begin 20001 :end 30000}] :type :value-ranges}]}}
               :task-id->task {}}
        new-state (mm/handle-action! :launch {:tasks tasks :offer-ids offer-ids} state nil (noop-mock-scheduler))]
    (is (= (count (:offer-id->offer new-state)) 0))
    (is (= (count (:task-id->task new-state)) 1))
    (is (= (get-in new-state [:slave-id->host "a5cb3212-3023-4546-888d-67cc13eac3dd"])
           {:hostname "hostb"
            :attributes {}
            :resources {:cpus {"*" 12}
                        :mem {"*" 120000}
                        :ports {"*" [{:begin 10000 :end 30000}]}}
            :available-resources {:cpus {"*" 4.0}
                                  :mem {"*" 40000.0}
                                  :ports {"*" [{:begin 10000 :end 12000} {:begin 25001 :end 30000}]}}
            :slave-id "a5cb3212-3023-4546-888d-67cc13eac3dd"}))))

(defn dummy-host
  [& {:keys [mem cpus ports uuid slave-id hostname attributes]
      :or {mem {"*" 12000}
           cpus {"*" 24}
           ports {"*" [{:begin 0 :end 1000}]}
           uuid (str (UUID/randomUUID))
           slave-id (str (UUID/randomUUID))
           attributes {}}}]
  {:hostname (or hostname slave-id)
   :attributes attributes
   :resources {:cpus cpus
               :mem mem
               :ports ports}
   :slave-id slave-id})

(deftest mesos-mock-offers
  (testing "offers sent and received"
    (let [registered-atom (atom false)
          offer-atom (atom [])
          scheduler (mesos/scheduler
                      (registered [this driver framework-id master-info]
                                  (reset! registered-atom true))
                      (resource-offers [this driver offers]
                                       (swap! offer-atom into offers)))
          host (dummy-host)
          offer-trigger-chan (chime-ch (util/time-seq (t/now) (t/millis 50)))
          mock-driver (mm/mesos-mock [host] offer-trigger-chan scheduler)]
      (mesos/start! mock-driver)
      (poll-until #(= (count @offer-atom) 1) 20 600)
      (mesos/stop! mock-driver)
      (is @registered-atom)
      (is (= (count @offer-atom) 1))
      (is (= (get-in (first @offer-atom) [:slave-id :value]) (:slave-id host)))))
  (testing "offers received, declined and received again"
    (let [registered-atom (atom false)
          offer-atom (atom [])
          scheduler (mesos/scheduler
                      (registered [this driver framework-id master-info]
                                  (reset! registered-atom true))
                      (resource-offers [this driver offers]
                                       (log/debug offers)
                                       ;; We want to make sure the offer, decline, re-offer loop works
                                       ;; but it gets confusing if we go through the cycle more than once
                                       (when-not (seq @offer-atom)
                                         (async/thread
                                           (doseq [offer offers]
                                             (try
                                               (mesos/decline-offer driver (:id offer))
                                               (catch Exception ex
                                                 (log/error ex))))))
                                       (swap! offer-atom into offers)))
          hosts [(dummy-host)]
          offer-trigger-chan (chime-ch (util/time-seq (t/now) (t/millis 50)))
          mock-driver (mm/mesos-mock hosts offer-trigger-chan scheduler)]
      (mesos/start! mock-driver)
      (poll-until #(= (count @offer-atom) 2) 20 600)
      (mesos/stop! mock-driver)
      (is @registered-atom)
      (is (= (count @offer-atom) 2))))
  (testing "offers received, declined and received again, multiple hosts"
    (let [registered-atom (atom false)
          offer-atom (atom [])
          scheduler (mesos/scheduler
                      (registered [this driver framework-id master-info]
                                  (reset! registered-atom true))
                      (resource-offers [this driver offers]
                                       (log/debug offers)
                                       ;; We want to make sure the offer, decline, re-offer loop works
                                       ;; but it gets confusing if we go through the cycle more than once
                                       (when-not (seq @offer-atom)
                                         (async/thread
                                           (doseq [offer offers]
                                             (try
                                               (mesos/decline-offer driver (:id offer))
                                               (catch Exception ex
                                                 (log/error ex))))))
                                       (swap! offer-atom into offers)))
          hosts [(dummy-host) (dummy-host) (dummy-host)]
          offer-trigger-chan (chime-ch (util/time-seq (t/now) (t/millis 50)))
          mock-driver (mm/mesos-mock hosts offer-trigger-chan scheduler)]
      (mesos/start! mock-driver)
      (poll-until #(= (count @offer-atom) 6) 20 1000)
      (mesos/stop! mock-driver)
      (is @registered-atom)
      (is (= (count @offer-atom) 6)))))

(defn dummy-task
  [& {:keys [task-id slave-id name command-str env user uris mem cpus ports labels data exec-time-ms-str]
      :or {task-id (str (UUID/randomUUID))
           slave-id (str (UUID/randomUUID))
           name "my-cool-job"
           command-str "dummy command"
           env {:variables [{:name "EXECUTION_TIME"
                             :value "50"}]}
           user "user-a"
           uris []
           mem {"*" 1000}
           cpus {"*" 2}
           ports {"*" [{:begin 1 :end 100}]}
           labels {}
           data nil
           exec-time-ms-str "50"}}]
  (let [make-scalar-resources (fn [name [role scalar]]
                                {:name name :role role :scalar scalar :type :value-scalar})
        make-ranges-resources (fn [name [role ranges]]
                                {:name name :role role :ranges ranges :type :value-ranges})
        resources (concat (map (partial make-scalar-resources "cpus") cpus)
                          (map (partial make-scalar-resources "mem") mem)
                          (map (partial make-ranges-resources "ports") ports))]
    {:task-id {:value task-id}
     :slave-id {:value slave-id}
     :name name
     :executor {:executor-id {:value "executor-id"}
                :framework-id {:value "framework-id"}
                :name "dummy-executor"
                :command {:value command-str
                          :environment env
                          :user user
                          :uris uris}}
     :resources resources
     :labels labels
     :data data}))

(deftest mesos-mock-launch
  ;; Note, both loading classes and jit-ing mesomatic files takes a long time
  ;; which can cause tests to fail if they rely on tight
  ;; timing assumptions and are only run once...
  (testing "basic launch task"
    (let [registered-atom (atom false)
          launched-atom (atom [])
          task-running-atom (atom [])
          task-complete-atom (atom [])
          atom-map {:registered-atom registered-atom
                    :launched-atom launched-atom
                    :task-running-atom task-running-atom
                    :task-complete-atom task-complete-atom}]
      (try
        (let [slave-id (str (UUID/randomUUID))
              cpus {"*" 2}
              mem {"*" 1000}
              ports {"*" [{:begin 1 :end 100}]}
              task (dummy-task :mem mem :cpus cpus :ports ports :slave-id slave-id)
              scheduler (mesos/scheduler
                          (registered [this driver framework-id master-info]
                                      (reset! registered-atom true))
                          (resource-offers [this driver offers]
                                           (log/info {:offers offers :count (count offers) :id (map :id offers)})
                                           (when (seq offers)
                                             (when (= (count @launched-atom) 0)
                                               (swap! launched-atom conj (:task-id task))
                                               (mesos/launch-tasks! driver
                                                                    (map :id offers)
                                                                    [task]))))
                          (status-update [this driver status]
                                         (log/info "Status update:" status)
                                         (condp = (:state status)
                                           :task-finished (swap! task-complete-atom conj (-> status :task-id :value))
                                           :task-running (swap! task-running-atom conj (-> status :task-id :value))
                                           (throw (ex-info "Unexpected status sent" {:status status})))))
              hosts [(dummy-host :mem mem :cpus cpus :ports ports :slave-id slave-id)]
              offer-trigger-chan (chime-ch (util/time-seq (t/now) (t/millis 50)))
              mock-driver (mm/mesos-mock hosts offer-trigger-chan scheduler)]
          (mesos/start! mock-driver)
          (poll-until #(= (count @task-complete-atom) 1) 20 1000)
          (log/warn "Calling stop")
          (mesos/stop! mock-driver)
          (is @registered-atom)
          (is (= (count @launched-atom) 1))
          (is (= (count @task-running-atom) 1))
          (is (= (count @task-complete-atom) 1)))
        (catch Throwable t
          (throw (ex-info "Error while testing launch" atom-map t))))))
  (testing "launch task"
    (let [registered-atom (atom false)
          launched-atom (atom [])
          task-running-atom (atom [])
          task-complete-atom (atom [])
          atom-map {:registered-atom registered-atom
                    :launched-atom launched-atom
                    :task-running-atom task-running-atom
                    :task-complete-atom task-complete-atom}]
      (try
        (let [slave-id (str (UUID/randomUUID))
              cpus {"*" 2}
              mem {"*" 1000}
              ports {"*" [{:begin 1 :end 100}]}
              task-a (dummy-task :mem mem :cpus cpus :ports ports :slave-id slave-id)
              task-b (dummy-task :mem mem :cpus cpus :ports ports :slave-id slave-id)
              scheduler (mesos/scheduler
                          (registered [this driver framework-id master-info]
                                      (reset! registered-atom true))
                          (resource-offers [this driver offers]
                                           (log/info {:offers offers :count (count offers) :id (map :id offers)})
                                           (when (seq offers)
                                             (when-let [task (condp = (count @launched-atom)
                                                               0 task-a
                                                               1 task-b
                                                               2 nil)]
                                               (log/info "Want to schedule task" {:task task})
                                               (swap! launched-atom conj (:task-id task))
                                               (log/info "Sending launch to mesos")
                                               (mesos/launch-tasks! driver
                                                                    (map :id offers)
                                                                    [task]))))
                          (status-update [this driver status]
                                         (log/info "Status update:" status)
                                         (condp = (:state status)
                                           :task-finished (swap! task-complete-atom conj (-> status :task-id :value))
                                           :task-running (swap! task-running-atom conj (-> status :task-id :value))
                                           (throw (ex-info "Unexpected status sent" {:status status})))))
              hosts [(dummy-host :mem mem :cpus cpus :ports ports :slave-id slave-id)]
              offer-trigger-chan (chime-ch (util/time-seq (t/now) (t/millis 50)))
              mock-driver (mm/mesos-mock hosts offer-trigger-chan scheduler)]
          (mesos/start! mock-driver)
          (poll-until #(= (count @task-complete-atom) 2) 20 1000)
          (mesos/stop! mock-driver)
          (is @registered-atom)
          (is (= (count @launched-atom) 2))
          (is (= (count @task-running-atom) 2))
          (is (= (count @task-complete-atom) 2)))
        (catch Throwable t
          (throw (ex-info "Error while testing launch" atom-map t)))))))

(deftest mesos-mock-kill
  ;; Mock this out with a stub string, as the compute cluster shouldn't be used anywhere.
  (with-redefs [cc/compute-cluster-name->ComputeCluster "mesos-mock-kill"]
    (testing "kill task"
      (let [registered-atom (atom false)
            offer-atom (atom [])
            launched-atom (atom [])
            task-running-atom (atom [])
            task-complete-atom (atom [])
            atom-map {:registered-atom registered-atom
                      :offer-atom offer-atom
                      :launched-atom launched-atom
                      :task-running-atom task-running-atom
                      :task-complete-atom task-complete-atom}]
        (try
          (let [slave-id (str (UUID/randomUUID))
                cpus {"*" 2}
                mem {"*" 1000}
                ports {"*" [{:begin 1 :end 100}]}
                task (dummy-task :mem mem :cpus cpus :ports ports :slave-id slave-id)
                scheduler (mesos/scheduler
                            (registered [this driver framework-id master-info]
                                        (reset! registered-atom true))
                            (resource-offers [this driver offers]
                                             (log/info {:offers offers :count (count offers) :id (map :id offers)})
                                             (when (seq offers)
                                               (when (< (count @launched-atom) 1)
                                                 (swap! launched-atom conj (:task-id task))
                                                 (mesos/launch-tasks! driver
                                                                      (map :id offers)
                                                                      [task]))
                                               (swap! offer-atom into offers)))
                            (status-update [this driver status]
                                           (log/info "Status update:" status)
                                           (condp = (:state status)
                                             :task-killed (swap! task-complete-atom conj (-> status :task-id :value))
                                             :task-running (let [task-id (-> status :task-id :value)]
                                                             (is (= (:value (:task-id task)) task-id))
                                                             (log/info "killing " task-id)
                                                             (mesos/kill-task! driver {:value task-id})
                                                             (swap! task-running-atom conj task-id))
                                             (throw (ex-info "Unexpected status sent" {:status status})))))
                hosts [(dummy-host :mem mem :cpus cpus :ports ports :slave-id slave-id)]
                offer-trigger-chan (chime-ch (util/time-seq (t/now) (t/millis 50)))
                mock-driver (mm/mesos-mock hosts offer-trigger-chan scheduler)]
            (mesos/start! mock-driver)
            (poll-until #(= (count @offer-atom) 2) 20 1000)
            (log/warn "Calling stop")
            (mesos/stop! mock-driver)
            (is @registered-atom)
            (is (= (count @offer-atom) 2))
            (is (= (count @launched-atom) 1))
            (is (= (count @task-running-atom) 1))
            (is (= (count @task-complete-atom) 1)))
          (catch Throwable t
            (throw (ex-info "Error while testing launch" atom-map t))))))))

(deftest cook-scheduler-integration
  (setup)
  ;; This tests the case where one user has all the resources in the cluster
  ;; and then another user shows up which causes the scheduler to preempt jobs
  ;; so both may run.
  ;; TODO: Explicitly check that both users have ~same resources instead of
  ;;       simply checking that the first user had some jobs preempted
  (testing "basic scheduling / rebalancing test"
    (let [mesos-datomic-conn (restore-fresh-database! (str (gensym "datomic:mem://mock-mesos")))
          mem 2000.0
          cpus 2.0
          ports [{:begin 1 :end 100}]
          num-hosts 10
          hosts (for [_ (range num-hosts)]
                  (dummy-host :mem {"*" mem} :cpus {"*" cpus} :ports {"*" ports}))
          offer-trigger-chan (chime-ch (util/time-seq (t/now) (t/millis 50)))
          make-mesos-driver-fn (fn [config scheduler framework-id] ;; _ is framework-id
                                 (mm/mesos-mock hosts offer-trigger-chan scheduler))]
      (with-cook-scheduler
        mesos-datomic-conn make-mesos-driver-fn {} true []
        (share/set-share! mesos-datomic-conn "default" nil "new cluster settings"
                          :mem mem :cpus cpus :gpus 1.0)
        ;; Note these two vars are lazy, need to realize to put them in db.
        (let [user-a-job-exec-time-ms 10000
              user-a-job-ent-ids (for [_ (range 20)]
                                   (create-dummy-job mesos-datomic-conn
                                                     :user "a"
                                                     :command "dummy command"
                                                     :executor :executor/cook
                                                     :custom-executor? false
                                                     :memory mem
                                                     :ncpus cpus
                                                     :env {"EXECUTION_TIME" (str user-a-job-exec-time-ms)}))

              user-b-job-exec-time-ms 1000
              user-b-job-ent-ids (for [_ (range 5)]
                                   (create-dummy-job mesos-datomic-conn
                                                     :user "b"
                                                     :command "dummy command"
                                                     :custom-executor? false
                                                     :executor :executor/cook
                                                     :memory mem
                                                     :ncpus cpus
                                                     :env {"EXECUTION_TIME" (str user-b-job-exec-time-ms)}))
              update-status-error-ms 500]
          ;; Transact user "a"s jobs
          (doall user-a-job-ent-ids)
          ;; Let them get scheduled
          (poll-until (fn [] (>= (count (filter #(= (:job/state (d/entity (d/db mesos-datomic-conn) %))
                                                    :job.state/running)
                                                user-a-job-ent-ids))
                                 10))
                      200
                      10000
                      (fn [] (str (into [] (map (comp :job/state (partial d/entity (d/db mesos-datomic-conn)))
                                                user-a-job-ent-ids)))))
          ;; Transact user "b"s jobs
          (doall user-b-job-ent-ids)
          ;; Let the system complete
          (poll-until (fn [] (every? (comp #(= (:job/state %) :job.state/completed)
                                           (partial d/entity (d/db mesos-datomic-conn)))
                                     user-a-job-ent-ids))
                      500
                      60000)
          ;; Expect all jobs to run and complete, expect some of user "a"s jobs
          ;; to be preempted
          (let [job-ents (map (partial d/entity (d/db mesos-datomic-conn)) user-a-job-ent-ids)]
            (is (some #(= (:reason/name (:instance/reason %)) :preempted-by-rebalancer) (mapcat :job/instance job-ents)))
            (doseq [job-ent job-ents]
              (let [instances (:job/instance job-ent)]
                (is (= (:job/state job-ent) :job.state/completed))
                (doseq [instance instances]
                  (is (or (= (:instance/status instance) :instance.status/success)
                          (= (:reason/name (:instance/reason instance)) :preempted-by-rebalancer)))
                  (when (= (:instance/status instance) :instance.status/success)
                    (is (< 0
                           (- (.getTime (:instance/end-time instance))
                              (.getTime (:instance/start-time instance))
                              user-a-job-exec-time-ms)
                           update-status-error-ms)))))))
          (doseq [job-ent-id user-b-job-ent-ids]
            (let [job-ent (d/entity (d/db mesos-datomic-conn) job-ent-id)
                  instances (:job/instance job-ent)
                  instance (first instances)]
              (is (= (count instances) 1))
              (is (= (:job/state job-ent) :job.state/completed))
              (log/info "job i: " (d/touch job-ent))
              (is (< 0
                     (- (.getTime (:instance/end-time instance))
                        (.getTime (:instance/start-time instance))
                        user-b-job-exec-time-ms)
                     update-status-error-ms)
                  (str "Invalid execution time of" job-ent))))
          (when (log/enabled? :debug)
            (dump-jobs-to-csv (pull-all-task-ents (d/db mesos-datomic-conn)) "trace-basic-scheduling-rebalancing-test.csv")))))))
