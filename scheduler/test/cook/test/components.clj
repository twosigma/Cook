(ns cook.test.components
  (:require [clojure.test :refer :all]
            [cook.components :as components]))

(deftest test-health-check-middleware
  (let [handler (fn [_] "Called handler!")
        debug-request {:uri "/debug"
                       :request-method :get}]
    (testing "always returns 200 if leader-reports-unhealthy is false"
      (let [leadership-atom (atom false)
            middleware (components/health-check-middleware handler
                                                           leadership-atom
                                                           false)]
        (is (= 200 (:status (middleware debug-request))))
        (swap! leadership-atom (constantly true))
        (is (= 200 (:status (middleware debug-request))))))
    (testing "returns 503 when leader"
      (let [leadership-atom (atom false)
            middleware (components/health-check-middleware handler
                                                           leadership-atom
                                                           true)]
        (is (= 200 (:status (middleware debug-request))))
        (swap! leadership-atom (constantly true))
        (is (= 503 (:status (middleware debug-request))))))
    (testing "passes other requests to handler"
      (let [leadership-atom (atom false)
            middleware (components/health-check-middleware handler
                                                           leadership-atom
                                                           true)]
        (is (= "Called handler!" (middleware {:uri "/real-request"})))
        (swap! leadership-atom (constantly true))
        (is (= "Called handler!" (middleware {:uri "/real-request"})))))))

(deftest test-read-edn-config
  (is (= {} (components/read-edn-config "{}")))
  (is (= {:foo 1} (components/read-edn-config "{:foo 1}")))
  (with-redefs [components/env (constantly "something")]
    (is (= {:master "something"} (components/read-edn-config "{:master #config/env \"MESOS_MASTER\"}"))))
  (with-redefs [components/env (constantly "12345")]
    (is (= {:port "12345"} (components/read-edn-config "{:port #config/env \"COOK_PORT\"}")))
    (is (= {:port 12345} (components/read-edn-config "{:port #config/env-int \"COOK_PORT\"}")))))
