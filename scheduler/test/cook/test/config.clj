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
(ns cook.test.config
  (:require [clojure.test :refer :all]
            [congestion.limits :as limits]
            [cook.config :refer [config config-settings config-string->fitness-calculator default-pool env read-edn-config default-schedulers-config]
             :as config]
            [cook.test.rest.api :as api]
            [cook.test.testutil :refer [setup]])
  (:import (clojure.lang ExceptionInfo)
           (com.netflix.fenzo VMTaskFitnessCalculator)))

(deftest test-read-edn-config
  (is (= {} (read-edn-config "{}")))
  (is (= {:foo 1} (read-edn-config "{:foo 1}")))
  (with-redefs [env (constantly "something")]
    (is (= {:master "something"} (read-edn-config "{:master #config/env \"MESOS_MASTER\"}"))))
  (with-redefs [env (constantly "12345")]
    (is (= {:port "12345"} (read-edn-config "{:port #config/env \"COOK_PORT\"}")))
    (is (= {:port 12345} (read-edn-config "{:port #config/env-int \"COOK_PORT\"}")))))

(deftest test-redef-config
  (setup :config {:database {:datomic-uri "foo"}})
  (is (= "foo" (-> config :settings :mesos-datomic-uri)))
  (with-redefs [config {:settings {:mesos-datomic-uri "bar"}}]
    (is (= "bar" (-> config :settings :mesos-datomic-uri))))
  (is (= "foo" (-> config :settings :mesos-datomic-uri))))

(deftest test-default-pool
  (with-redefs [config {:settings {:pools {:default "foo"}}}]
    (is (= "foo" (default-pool))))
  (with-redefs [config {:settings {:pools {:default ""}}}]
    (is (nil? (default-pool))))
  (with-redefs [config {:settings {:pools {:default nil}}}]
    (is (nil? (default-pool))))
  (with-redefs [config {:settings {}}]
    (is (nil? (default-pool)))))

(def dummy-fitness-calculator
  "This calculator simply returns 0.0 for every Fenzo fitness calculation."
  (reify VMTaskFitnessCalculator
    (getName [_] "Dummy Fitness Calculator")
    (calculateFitness [_ _ _ _] 0.0)))

(defn make-dummy-fitness-calculator []
  dummy-fitness-calculator)

(deftest test-config-string->fitness-calculator
  (testing "clojure symbol"
    (is (instance? VMTaskFitnessCalculator
                   (config-string->fitness-calculator
                     "cook.test.config/dummy-fitness-calculator"))))
  (testing "java class on classpath"
    (is (instance? VMTaskFitnessCalculator
                   (config-string->fitness-calculator
                     cook.config/default-fitness-calculator))))
  (testing "clojure function"
    (is (instance? VMTaskFitnessCalculator
                   (config-string->fitness-calculator
                    "cook.test.config/make-dummy-fitness-calculator"))))

  (testing "bad input"
    (is (thrown? IllegalArgumentException (config-string->fitness-calculator "not-a-valid-anything"))))

  (testing "something other than a VMTaskFitnessCalculator"
    (is (thrown? IllegalArgumentException (config-string->fitness-calculator
                                            "System/out")))))

(deftest test-config-settings
  (testing "k8s controller lock num shards is in a sane range"
    (let [valid-config (assoc-in (api/minimal-config)
                                 [:config :kubernetes :controller-lock-num-shards]
                                 1)]
      (is (config-settings valid-config)))
    (let [valid-config (assoc-in (api/minimal-config)
                                 [:config :kubernetes :controller-lock-num-shards]
                                 32)]
      (is (config-settings valid-config)))
    (let [bad-config (assoc-in (api/minimal-config)
                               [:config :kubernetes :controller-lock-num-shards]
                               0)]
      (is (thrown? ExceptionInfo (config-settings bad-config))))
    (let [bad-config (assoc-in (api/minimal-config)
                               [:config :kubernetes :controller-lock-num-shards]
                               32778)]
      (is (thrown? ExceptionInfo (config-settings bad-config)))))

  (testing "default pool schedulers config applies"
    (let [valid-config (dissoc (api/minimal-config) :pools)
          applied-config (config-settings valid-config)
          actual (get-in applied-config [:pools :schedulers])]
      (is applied-config)
      (is (= actual
             default-schedulers-config))))

  (testing "default fenzo config merges with partial but valid scheduler-config"
    (let [valid-config (assoc-in (api/minimal-config) [:config :pools :schedulers] 
                                 [{:pool-regex "test-pool"
                                   :scheduler-config {:scheduler "fenzo"
                                                      :fenzo-fitness-calculator "foo"}}])
          applied-config (config-settings valid-config)
          actual (get-in applied-config [:pools :schedulers])]
      (is applied-config)
      (is (= 1 (count actual)))
      (is (= actual
             [{:pool-regex "test-pool"
               :scheduler-config {:scheduler "fenzo"
                                  :good-enough-fitness 0.8
                                  :fenzo-fitness-calculator "foo"
                                  :fenzo-max-jobs-considered 1000
                                  :fenzo-scaleback 0.95
                                  :fenzo-floor-iterations-before-warn 10
                                  :fenzo-floor-iterations-before-reset 1000}}]))))
  
  (testing "default kubernetes config merges with partial but valid scheduler-config"
    (let [valid-config (assoc-in (api/minimal-config) [:config :pools :schedulers]
                                 [{:pool-regex "test-pool"
                                   :scheduler-config {:scheduler "kubernetes"}}])
          applied-config (config-settings valid-config)
          actual (get-in applied-config [:pools :schedulers])]
      (is applied-config)
      (is (= 1 (count actual)))
      (is (= actual
             [{:pool-regex "test-pool"
               :scheduler-config {:scheduler "kubernetes"
                                  :max-jobs-considered 500
                                  :minimum-scheduling-capacity-threshold 50
                                  :scheduling-pause-time-ms 3000}}])))))

(deftest test-valid-schedulers-config
  (testing "empty valid-schedulers-config"
    (is (nil? (config/guard-invalid-schedulers-config []))))

  (testing "valid config"
    (is (nil? (config/guard-invalid-schedulers-config [{:pool-regex "test-pool"
                                                        :scheduler-config {:scheduler "fenzo"}}]))))

  (testing "no valid pool regex"
    (is (thrown-with-msg? ExceptionInfo
                          #"pool-regex key is missing from config"
                          (config/guard-invalid-schedulers-config [{}]))))

  (testing "no scheduler config"
    (is (thrown-with-msg? ExceptionInfo
                          #"scheduler-config key is missing from config"
                          (config/guard-invalid-schedulers-config [{:pool-regex "test-pool"}]))))

  (testing "no scheduler"
    (is (thrown-with-msg? ExceptionInfo
                          #"scheduler key is missing from scheduler-config"
                          (config/guard-invalid-schedulers-config [{:pool-regex "test-pool"
                                                                    :scheduler-config {}}]))))

  (testing "invalid scheduler"
    (is (thrown-with-msg? ExceptionInfo
                          #"scheduler must be fenzo or kubernetes"
                          (config/guard-invalid-schedulers-config [{:pool-regex "test-pool"
                                                                    :scheduler-config {:scheduler ""}}])))))

(deftest test-valid-gpu-models-config-settings
  (testing "empty valid-gpu-models"
    (is (nil? (config/guard-invalid-gpu-config []))))
  (testing "valid default model"
    (is (nil? (config/guard-invalid-gpu-config [{:pool-regex "test-pool"
                                                 :valid-models #{"valid-gpu-model"}
                                                 :default-model "valid-gpu-model"}]))))
  (testing "no valid models"
    (is (thrown-with-msg? ExceptionInfo
                          #"Valid GPU models for pool-regex test-pool is not defined"
                          (config/guard-invalid-gpu-config [{:pool-regex "test-pool"
                                                             :default-model "valid-gpu-model"}]))))
  (testing "no default model"
    (is (thrown-with-msg? ExceptionInfo
                          #"Default GPU model for pool-regex test-pool is not defined"
                          (config/guard-invalid-gpu-config [{:pool-regex "test-pool"
                                                             :valid-models #{"valid-gpu-model"}}]))))
  (testing "invalid default model"
    (is (thrown-with-msg? ExceptionInfo
                          #"Default GPU model for pool-regex test-pool is not listed as a valid GPU model"
                          (config/guard-invalid-gpu-config [{:pool-regex "test-pool"
                                                             :valid-models #{"valid-gpu-model"}
                                                             :default-model "invalid-gpu-model"}])))))

(deftest test-disk-config-settings
  (testing "empty disk"
    (is (nil? (config/guard-invalid-disk-config []))))
  (testing "valid default type"
    (is (nil? (config/guard-invalid-disk-config [{:pool-regex "^test-pool$"
                                                  :valid-types #{"valid-disk-type"}
                                                  :default-type "valid-disk-type"
                                                  :max-size 256000}]))))
  (testing "no valid types"
    (is (thrown-with-msg? ExceptionInfo
                          #"Valid disk types for pool-regex \^test-pool\$ is not defined"
                          (config/guard-invalid-disk-config [{:pool-regex "^test-pool$"
                                                              :default-type "valid-disk-type"
                                                              :max-size 256000}]))))
  (testing "no default type"
    (is (thrown-with-msg? ExceptionInfo
                          #"Default disk type for pool-regex \^test-pool\$ is not defined"
                          (config/guard-invalid-disk-config [{:pool-regex "^test-pool$"
                                                              :valid-types #{"valid-disk-type"}
                                                              :max-size 256000}]))))
  (testing "no max size"
    (is (thrown-with-msg? ExceptionInfo
                          #"Max requestable disk size for pool-regex \^test-pool\$ is not defined"
                          (config/guard-invalid-disk-config [{:pool-regex "^test-pool$"
                                                              :valid-types #{"valid-disk-type"}
                                                              :default-type "valid-disk-type"}]))))
  (testing "invalid default type"
    (is (thrown-with-msg? ExceptionInfo
                          #"Default disk type for pool-regex \^test-pool\$ is not listed as a valid disk type"
                          (config/guard-invalid-disk-config [{:pool-regex "^test-pool$"
                                                              :valid-types #{"valid-disk-type"}
                                                              :default-type "invalid-disk-type"
                                                              :max-size 256000}])))))

(deftest test-user-rate-limit
  (testing "distinct quota for auth bypass requests"
    (let [user-rate-limit (config/->UserRateLimit :user-limit 1 10 nil)]
      (is (= 1 (limits/get-quota user-rate-limit {:authorization/user "sally"})))
      (is (= 10 (limits/get-quota user-rate-limit {:authorization/user nil}))))))
