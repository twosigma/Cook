(ns cook.test.plugins.job-submission-modifier
  (:require [clojure.test :refer :all]
            [cook.plugins.definitions :as plugins]
            [cook.plugins.job-submission-modifier :as job-mod]))

(deftest test-identity-add-pool
  (let [mod-plugin (job-mod/->IdentityJobSubmissionModifier)
        job {}
        pool-name "my-pool"]
    (is (= "my-pool" (get (plugins/modify-job mod-plugin job pool-name) :pool) ))))

(defrecord TestJobModifier []
  plugins/JobSubmissionModifier
  (modify-job [this job pool-name]
    (throw (IllegalArgumentException. "TestJobModifier always throws"))))

(deftest test-raise-exception
  ; On its own, this test has little value. We are more interested in the overall
  ; behavior when a real job is submitted and the plugin raises an exception.
  (let [mod-plugin (TestJobModifier.)]
    (is (thrown? IllegalArgumentException (plugins/modify-job mod-plugin nil nil)))))