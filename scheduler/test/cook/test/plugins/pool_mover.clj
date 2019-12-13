(ns cook.test.plugins.pool-mover
  (:require [clojure.test :refer :all]
            [cook.plugins.definitions :as chd]
            [cook.plugins.pool-mover :as pm]
            [datomic.api :as d]))

(deftest test-adjust-job
  (testing "pool is modified"
    (let [pool-mover (pm/->PoolMoverJobAdjuster {"foo" {:users {"bob" {:portion 1.0}}}})
          job-map {:job/pool {:pool/name "foo"}
                   :job/user "bob"}]
      (with-redefs [d/entity (constantly {:pool/name "bar"})]
        (is (= "bar" (-> pool-mover (chd/adjust-job job-map nil) :job/pool :pool/name)))))))