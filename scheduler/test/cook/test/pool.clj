(ns cook.test.pool
  (:require [clojure.test :refer :all]
            [cook.config :as config]
            [cook.pool :as pool]
            [cook.test.postgres])
  (:import (clojure.lang ExceptionInfo)))

(use-fixtures :once cook.test.postgres/with-pg-db)

(deftest test-guard-invalid-default-pool
  (with-redefs [pool/all-pools (constantly [{:pool/name "foo"}])
                config/default-pool (constantly "foo")]
    (is (nil? (pool/guard-invalid-default-pool nil))))
  (with-redefs [pool/all-pools (constantly [])
                config/default-pool (constantly nil)]
    (is (nil? (pool/guard-invalid-default-pool nil))))
  (with-redefs [pool/all-pools (constantly [{}])
                config/default-pool (constantly nil)]
    (is (thrown-with-msg? ExceptionInfo
                          #"There are pools in the database, but no default pool is configured"
                          (pool/guard-invalid-default-pool nil))))
  (with-redefs [pool/all-pools (constantly [])
                config/default-pool (constantly "foo")]
    (is (thrown-with-msg? ExceptionInfo
                          #"There is no pool in the database matching the configured default pool"
                          (pool/guard-invalid-default-pool nil))))
  (with-redefs [pool/all-pools (constantly [{:pool/name "bar"}])
                config/default-pool (constantly "foo")]
    (is (thrown-with-msg? ExceptionInfo
                          #"There is no pool in the database matching the configured default pool"
                          (pool/guard-invalid-default-pool nil)))))