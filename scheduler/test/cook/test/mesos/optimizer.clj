(ns cook.test.mesos.optimizer
  (:use clojure.test)
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [cook.mesos.optimizer :as optimizer]))

;; Tests to make sure data flows and validates properly
(deftest test-optimizer-cycle
  (let [host {:count 1
              :instance-type "small"
              :cpus 1
              :mem 1000
              :time-to-start  (-> 30 t/seconds t/in-millis)}
        host-feed (reify optimizer/HostFeed
                         (get-available-host-info [this]
                           [host]))
        optimizer (reify optimizer/Optimizer
                          (produce-schedule [this queue running available [host-info & host-infos]]
                            {0 {:suggested-purchases {host-info 2}
                                :suggested-matches {host-info (map :job/uuid queue)}}}))
        consumer (reify optimizer/ScheduleConsumer
                   (consume-schedule [this schedule]
                     (is (= (count schedule) 1))
                     (is (= (first (keys (get-in schedule [0 :suggested-matches])))
                            host))))
        queue [{:job/uuid (java.util.UUID/randomUUID)} {:job/uuid (java.util.UUID/randomUUID)}]]
    (async/<!! (optimizer/optimizer-cycle! (fn get-queue [] queue)
                                (fn get-running [] [])
                                (fn get-offers [] [])
                                host-feed
                                optimizer
                                consumer))))
