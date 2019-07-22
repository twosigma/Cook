(ns cook.test.plugins.submission
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clj-time.core :as t]
            [cook.plugins.definitions :as plugins]
            [cook.plugins.submission :as submit]))



(deftest test-composite-plugin
  (let [short-expiry (t/plus (t/now) (t/millis 100))
        long-expiry (t/plus short-expiry (t/millis 1000))
        no-a-plugin (reify plugins/JobSubmissionValidator
                      (plugins/check-job-submission-default [this]
                        {:status :accepted})
                      (plugins/check-job-submission [this {:keys [job/name]}]
                        (if (str/includes? name "a")
                          {:status :rejected
                           :message "Job name contained an a"
                           :cache-expires-at short-expiry}
                          {:status :accepted
                           :cache-expires-at short-expiry})))
        no-b-plugin (reify plugins/JobSubmissionValidator
                      (plugins/check-job-submission-default [this]
                        {:status :accepted})
                      (plugins/check-job-submission [this {:keys [job/name]}]
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
             (plugins/check-job-submission composite-plugin {:job/name "foo"}))))

    (testing "b plugin fails"
      (is (= {:status :rejected
              :message "Job name contained a b"
              :cache-expires-at short-expiry}
             (plugins/check-job-submission composite-plugin {:job/name "b"}))))

    (testing "a plugin fails"
      (is (= {:status :rejected
              :message "Job name contained an a"
              :cache-expires-at short-expiry}
             (plugins/check-job-submission composite-plugin {:job/name "a"}))))

    (testing "both plugins fail"
      (is (= {:status :rejected
              :message "Job name contained an a\nJob name contained a b"
              :cache-expires-at short-expiry}
             (plugins/check-job-submission composite-plugin {:job/name "ab"}))))))