(ns cook.test.components
  (:require [clojure.test :refer :all]
            [cook.components :as components]))

(deftest test-read-edn-config
  (is (= {} (components/read-edn-config "{}")))
  (is (= {:foo 1} (components/read-edn-config "{:foo 1}")))
  (with-redefs [components/env (constantly "something")]
    (is (= {:master "something"} (components/read-edn-config "{:master #config/env \"MESOS_MASTER\"}"))))
  (with-redefs [components/env (constantly "12345")]
    (is (= {:port "12345"} (components/read-edn-config "{:port #config/env \"COOK_PORT\"}")))
    (is (= {:port 12345} (components/read-edn-config "{:port #config/env-int \"COOK_PORT\"}")))))
