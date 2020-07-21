(ns cook.test.plugins.pool
  (:require [clojure.test :refer :all]
            [cook.plugins.definitions :as plugins]
            [cook.plugins.pool :as pool]))

(deftest test-attribute-pool-selector
  (let [selector (pool/->AttributePoolSelector "test-attribute" "my-pool")]
    (is (= "my-pool" (plugins/select-pool selector {})))
    (is (= "my-pool" (plugins/select-pool selector {:attributes [{:name "cook-pool"
                                                                  :text "a-pool"}]})))
    (is (= "a-pool" (plugins/select-pool selector {:attributes [{:name "test-attribute"
                                                                 :text "a-pool"}]})))
    (is (= "b-pool" (plugins/select-pool selector {:attributes [{:name "test-attribute"
                                                                 :text "b-pool"}]})))))
