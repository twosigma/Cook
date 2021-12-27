(ns cook.test.task
  (:require [clojure.test :refer :all]
            [cook.test.postgres]
            [cook.task :as task]
            [cook.test.testutil :refer [restore-fresh-database!
                                        setup]]))

(use-fixtures :once cook.test.postgres/with-pg-db)

(deftest test-build-executor-environment
  (testing "default values"
    (is (= {"EXECUTOR_LOG_LEVEL" "INFO"
            "EXECUTOR_MAX_MESSAGE_LENGTH" 256
            "PROGRESS_REGEX_STRING" "default-regex"
            "PROGRESS_SAMPLE_INTERVAL_MS" 2000}
           (let [executor-config {:command "command"
                                  :default-progress-regex-string "default-regex"
                                  :log-level "INFO"
                                  :max-message-length 256
                                  :progress-sample-interval-ms 2000}
                 job-ent nil]
             (with-redefs [cook.config/executor-config (constantly executor-config)]
               (task/build-executor-environment job-ent))))))

  (testing "default values with environment"
    (is (= {"CUSTOM_FOO" "foo"
            "CUSTOM_VAR" "var"
            "EXECUTOR_LOG_LEVEL" "INFO"
            "EXECUTOR_MAX_MESSAGE_LENGTH" 256
            "EXECUTOR_PROGRESS_OUTPUT_FILE_ENV" "CONFIGURED_PROGRESS_OUTPUT_ENV"
            "PROGRESS_REGEX_STRING" "default-regex"
            "PROGRESS_SAMPLE_INTERVAL_MS" 2000}
           (let [executor-config {:command "command"
                                  :default-progress-regex-string "default-regex"
                                  :environment {"CUSTOM_FOO" "foo"
                                                "CUSTOM_VAR" "var"
                                                "EXECUTOR_LOG_LEVEL" "will-be-overridden"
                                                "EXECUTOR_PROGRESS_OUTPUT_FILE_ENV" "CONFIGURED_PROGRESS_OUTPUT_ENV"}
                                  :log-level "INFO"
                                  :max-message-length 256
                                  :progress-sample-interval-ms 2000}
                 job-ent nil]
             (with-redefs [cook.config/executor-config (constantly executor-config)]
               (task/build-executor-environment job-ent))))))

  (testing "job configured values"
    (is (= {"EXECUTOR_LOG_LEVEL" "INFO"
            "EXECUTOR_MAX_MESSAGE_LENGTH" 256
            "EXECUTOR_PROGRESS_OUTPUT_FILE_ENV" "EXECUTOR_PROGRESS_OUTPUT_FILE_NAME"
            "EXECUTOR_PROGRESS_OUTPUT_FILE_NAME" "progress.out"
            "PROGRESS_REGEX_STRING" "custom-regex"
            "PROGRESS_SAMPLE_INTERVAL_MS" 2000}
           (let [executor-config {:command "command"
                                  :default-progress-regex-string "default-regex"
                                  :log-level "INFO"
                                  :max-message-length 256
                                  :progress-sample-interval-ms 2000}
                 job-ent {:job/executor-log-level "DEBUG"
                          :job/executor-max-message-length 768
                          :job/progress-output-file "progress.out"
                          :job/progress-regex-string "custom-regex"
                          :job/progress-sample-interval-ms 5000}]
             (with-redefs [cook.config/executor-config (constantly executor-config)]
               (task/build-executor-environment job-ent))))))

  (testing "job configured values sanitized"
    (is (= {"CONFIGURED_PROGRESS_OUTPUT_ENV" "progress.conf.out"
            "CUSTOM_FOO" "foo"
            "CUSTOM_VAR" "var"
            "EXECUTOR_LOG_LEVEL" "INFO"
            "EXECUTOR_MAX_MESSAGE_LENGTH" 256
            "EXECUTOR_PROGRESS_OUTPUT_FILE_ENV" "EXECUTOR_PROGRESS_OUTPUT_FILE_NAME"
            "EXECUTOR_PROGRESS_OUTPUT_FILE_NAME" "progress.out"
            "PROGRESS_REGEX_STRING" "custom-regex"
            "PROGRESS_SAMPLE_INTERVAL_MS" 2000}
           (let [executor-config {:command "command"
                                  :default-progress-regex-string "default-regex"
                                  :environment {"CONFIGURED_PROGRESS_OUTPUT_ENV" "progress.conf.out"
                                                "CUSTOM_FOO" "foo"
                                                "CUSTOM_VAR" "var"
                                                "EXECUTOR_LOG_LEVEL" "will-be-overridden"
                                                "EXECUTOR_PROGRESS_OUTPUT_FILE_ENV" "CONFIGURED_PROGRESS_OUTPUT_ENV"}
                                  :log-level "INFO"
                                  :max-message-length 256
                                  :progress-sample-interval-ms 2000}
                 job-ent {:job/executor-log-level "FOOBAR"
                          :job/executor-max-message-length 768000
                          :job/progress-output-file "progress.out"
                          :job/progress-regex-string "custom-regex"
                          :job/progress-sample-interval-ms 5000000}]
             (with-redefs [cook.config/executor-config (constantly executor-config)]
               (task/build-executor-environment job-ent))))))

  (testing "Incremental config environment"
    (setup)
    (is (= {"EXECUTOR_LOG_LEVEL" "INFO"
            "EXECUTOR_MAX_MESSAGE_LENGTH" 256
            "INCREMENTAL_1" "foo"
            "INCREMENTAL_2" "bar"
            "INCREMENTAL_3" ""
            "PROGRESS_REGEX_STRING" "default-regex"
            "PROGRESS_SAMPLE_INTERVAL_MS" 2000}
           (let [executor-config {:command "command"
                                  :default-progress-regex-string "default-regex"
                                  :log-level "INFO"
                                  :max-message-length 256
                                  :progress-sample-interval-ms 2000
                                  :incremental-config-environment {"INCREMENTAL_1" [{:value "foo" :portion 1.0}]
                                                                   "INCREMENTAL_2" :bar
                                                                   "INCREMENTAL_3" :baz}}
                 job-ent {:job/uuid (java.util.UUID/fromString "41062821-b248-4375-82f8-a8256643c94e")}
                 uri "datomic:mem://test-compute-cluster-config"
                 conn (restore-fresh-database! uri)
                 key :bar
                 values [{:value "bar" :portion 1.0}]]
             (with-redefs [cook.config/executor-config (constantly executor-config)
                           cook.config-incremental/get-conn (fn [] conn)]
               (cook.config-incremental/write-configs [{:key key :values values}])
               (task/build-executor-environment job-ent)))))))
