(ns cook.test.pool
  (:require [clojure.test :refer :all]
            [cook.config :as config]
            [cook.pool :as pool])
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

(deftest test-guard-invalid-default-gpu-model
  (testing "valid default model"
    (with-redefs [config/valid-gpu-models (constantly [{:pool-regex    "test-pool"
                                                        :valid-models  #{"valid-gpu-model"}
                                                        :default-model "valid-gpu-model"}])]
      (is (nil? (pool/guard-invalid-default-gpu-model nil)))))
  (testing "no default model"
    (with-redefs [config/valid-gpu-models (constantly [{:pool-regex    "test-pool"
                                                        :valid-models  #{"valid-gpu-model"}}])]
      (is (thrown-with-msg?
            ExceptionInfo
            #"Default GPU model is not configured correctly"
            pool/guard-invalid-default-gpu-model nil))))
  (testing "invalid default model"
    (with-redefs [config/valid-gpu-models (constantly [{:pool-regex    "test-pool"
                                                        :valid-models  #{"valid-gpu-model"}
                                                        :default-model "invalid-gpu-model"}])]
      (is (thrown-with-msg?
            ExceptionInfo
            #"Default GPU model is not configured correctly"
            pool/guard-invalid-default-gpu-model nil)))))