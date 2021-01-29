(ns cook.test.plugins.submission
  (:require [clj-time.core :as t]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [cook.plugins.definitions :as plugins]
            [cook.plugins.submission :as submit]
            [cook.config :as config]))



(deftest test-composite-plugin
  (let [short-expiry (t/plus (t/now) (t/millis 100))
        long-expiry (t/plus short-expiry (t/millis 1000))
        no-a-plugin (reify plugins/JobSubmissionValidator
                      (plugins/check-job-submission-default [this]
                        {:status :accepted})
                      (plugins/check-job-submission [this {:keys [job/name]} _]
                        (if (str/includes? name "a")
                          {:status :rejected
                           :message "Job name contained an a"
                           :cache-expires-at short-expiry}
                          {:status :accepted
                           :cache-expires-at short-expiry})))
        no-b-plugin (reify plugins/JobSubmissionValidator
                      (plugins/check-job-submission-default [this]
                        {:status :accepted})
                      (plugins/check-job-submission [this {:keys [job/name]} _]
                        (if (str/includes? name "b")
                          {:status :rejected
                           :message "Job name contained a b"
                           :cache-expires-at long-expiry}
                          {:status :accepted
                           :cache-expires-at long-expiry})))
        composite-plugin (submit/->CompositeSubmissionPlugin [no-a-plugin no-b-plugin])]

    (testing "submission default"
      (is (= {:status :accepted} (plugins/check-job-submission-default composite-plugin))))

    (testing "both pass"
      (is (= {:status :accepted
              :cache-expires-at short-expiry}
             (plugins/check-job-submission composite-plugin {:job/name "foo"} nil))))

    (testing "b plugin fails"
      (is (= {:status :rejected
              :message "Job name contained a b"
              :cache-expires-at short-expiry}
             (plugins/check-job-submission composite-plugin {:job/name "b"} nil))))

    (testing "a plugin fails"
      (is (= {:status :rejected
              :message "Job name contained an a"
              :cache-expires-at short-expiry}
             (plugins/check-job-submission composite-plugin {:job/name "a"} nil))))

    (testing "both plugins fail"
      (is (= {:status :rejected
              :message "Job name contained an a\nJob name contained a b"
              :cache-expires-at short-expiry}
             (plugins/check-job-submission composite-plugin {:job/name "ab"} nil))))))

(deftest test-job-shape-validation-plugin
  (let [plugin (submit/->JopShapeValidationPlugin)
        limits-config
        {:node-type->limits {"t1-test-1" {:max-cpus 1
                                          :max-disk 2
                                          :max-mem 3}
                             "t2-test-1" {:max-cpus 4
                                          :max-disk 5
                                          :max-mem 6}
                             "t3-test-1" {:max-cpus 7
                                          :max-disk 8
                                          :max-mem 9}
                             "t3-test-2" {:max-cpus 10
                                          :max-disk 11
                                          :max-mem 12}
                             "t4-test-1" {:max-cpus 13
                                          :max-disk 14
                                          :max-mem 15}}
         :non-gpu-jobs [{:pool-regex "test-pool"
                         :node-type-lookup {nil {nil {nil "t1-test-1"
                                                      "foo-cpu-arch" "t2-test-1"}
                                                 "t3" {nil "t3-test-1"
                                                       "bar-cpu-arch" "t3-test-2"}}
                                            "t4-test-1" {nil {nil "t4-test-1"}}}}]}]
    (with-redefs [config/job-resource-limits (constantly limits-config)]
      (is (= {:status :accepted}
             (plugins/check-job-submission
               plugin
               {}
               "test-pool")))
      (is (= {:status :accepted}
             (plugins/check-job-submission
               plugin
               {:cpus 1 :disk {:request 2} :mem 3}
               "test-pool")))
      (is (= {:status :accepted}
             (plugins/check-job-submission
               plugin
               {:constraints [["cpu-architecture" "EQUALS" "foo-cpu-arch"]]
                :cpus 4
                :disk {:request 5}
                :mem 6}
               "test-pool")))
      (is (= {:status :accepted}
             (plugins/check-job-submission
               plugin
               {:constraints [["node-family" "EQUALS" "t3"]]
                :cpus 7
                :disk {:request 8}
                :mem 9}
               "test-pool")))
      (is (= {:status :accepted}
             (plugins/check-job-submission
               plugin
               {:constraints [["node-family" "EQUALS" "t3"]
                              ["cpu-architecture" "EQUALS" "bar-cpu-arch"]]
                :cpus 10
                :disk {:request 11}
                :mem 12}
               "test-pool")))
      (is (= {:status :accepted}
             (plugins/check-job-submission
               plugin
               {:constraints [["node-type" "EQUALS" "t4-test-1"]]
                :cpus 13
                :disk {:request 14}
                :mem 15}
               "test-pool")))
      (is (= :rejected
             (:status (plugins/check-job-submission
                        plugin
                        {:constraints [["node-type" "EQUALS" "t4-test-1"]]
                         :cpus 14
                         :disk {:request 14}
                         :mem 15}
                        "test-pool"))))
      (is (= :rejected
             (:status (plugins/check-job-submission
                        plugin
                        {:constraints [["node-type" "EQUALS" "t4-test-1"]]
                         :cpus 13
                         :disk {:request 15}
                         :mem 15}
                        "test-pool"))))
      (is (= :rejected
             (:status (plugins/check-job-submission
                        plugin
                        {:constraints [["node-type" "EQUALS" "t4-test-1"]]
                         :cpus 13
                         :disk {:request 14}
                         :mem 16}
                        "test-pool"))))
      (is (= :rejected
             (:status (plugins/check-job-submission
                        plugin
                        {:cpus 13
                         :disk {:request 14}
                         :mem 15}
                        "test-pool")))))))