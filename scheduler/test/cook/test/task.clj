(ns cook.test.task
  (:use clojure.test)
  (:require [cook.config :as config]
            [cook.task :as task]))

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
               (task/build-executor-environment job-ent)))))))
