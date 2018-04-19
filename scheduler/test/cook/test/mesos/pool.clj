(ns cook.test.mesos.pool
  (:require [clojure.test :refer :all]
            [cook.config :as config]
            [cook.mesos.pool :as pool])
  (:import (clojure.lang ExceptionInfo)))

(deftest test-guard-invalid-default-pool
  (with-redefs [pool/all-pools (constantly [{:pool/name "foo"}])
                config/default-pool (constantly "foo")]
    (is (nil? (pool/guard-invalid-default-pool nil))))
  (with-redefs [pool/all-pools (constantly [])
                config/default-pool (constantly nil)]
    (is (nil? (pool/guard-invalid-default-pool nil))))
  (with-redefs [pool/all-pools (constantly [{}])
                config/default-pool (constantly nil)]
    (is (thrown? ExceptionInfo (pool/guard-invalid-default-pool nil))))
  (with-redefs [pool/all-pools (constantly [])
                config/default-pool (constantly "foo")]
    (is (thrown? ExceptionInfo (pool/guard-invalid-default-pool nil))))
  (with-redefs [pool/all-pools (constantly [{:pool/name "bar"}])
                config/default-pool (constantly "foo")]
    (is (thrown? ExceptionInfo (pool/guard-invalid-default-pool nil)))))
