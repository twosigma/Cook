(ns cook.test.mesos.optimizer
  (:use clojure.test)
  (:require [clj-http.client :as http]
            [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.data.json :as json]
            [cook.mesos.optimizer :as optimizer]
            [plumbing.core :as pc])
  (:import (java.util Date UUID)))

(deftest test-optimizer-server
  (let [server-url "http://optimizer.com/cook"
        config {:cpu-share 200
                :default-runtime 120000
                :host-info {:count 100
                            :cpus 40
                            :instance-type "basic"
                            :mem 320000
                            :time-to-start 100}
                :max-waiting 1500
                :mem-share 40000
                :opt-params {"param1" "value1"
                             "param2" "value2"}
                :opt-server server-url
                :step-list [0 30 60]}
        job1-uuid (UUID/fromString "d1efa9cd-568d-450a-9568-d425f29a02ba")
        job2-uuid (UUID/fromString "65a17d4c-6f20-443b-9893-6dfa6af2840b")
        job3-uuid (UUID/fromString "bcbb1f2b-8c3a-4fd3-bb01-ae3a5968a402")
        job4-uuid (UUID/fromString "53487be9-fb2f-42f6-8bb8-97b5ed61ebfc")
        job5-uuid (UUID/fromString "70bf7703-bf7c-49f5-bafb-33db7db82a84")
        job6-uuid (UUID/fromString "eb61f383-0f2b-4024-a45e-67a6124358dc")
        job7-uuid (UUID/fromString "aa8ec30d-c00a-473a-965c-c7daac42a421")
        job8-uuid (UUID/fromString "e112f9d6-69c3-4e06-9460-ae5f7e0087f4")
        submit-time (Date. 12345600)
        start-time (Date. 12567800)
        current-time (Date. 12578900)
        make-resource (fn [cpus mem]
                        [{:resource/type :resource.type/cpus
                          :resource/amount cpus}
                         {:resource/type :resource.type/mem
                          :resource/amount mem}])
        running-tasks [{:instance/hostname "basic1.host"
                        :instance/start-time start-time
                        :job/_instance {:job/expected-runtime 10000
                                        :job/resource (make-resource 1 10)
                                        :job/submit-time submit-time
                                        :job/user "user-1"
                                        :job/uuid job1-uuid}}
                       {:instance/hostname "basic2.host"
                        :instance/start-time start-time
                        :job/_instance {:job/expected-runtime 20000
                                        :job/resource (make-resource 2 20)
                                        :job/submit-time submit-time
                                        :job/user "user-2"
                                        :job/uuid job2-uuid}}
                       {:instance/hostname "basic3.host"
                        :instance/start-time start-time
                        :job/_instance {:job/expected-runtime 30000
                                        :job/resource (make-resource 3 30)
                                        :job/submit-time submit-time
                                        :job/user "user-3"
                                        :job/uuid job3-uuid}}]
        queued-jobs [{:job/expected-runtime 10000
                      :job/resource (make-resource 4 40)
                      :job/submit-time submit-time
                      :job/user "user-1"
                      :job/uuid job4-uuid}
                     {:job/expected-runtime 20000
                      :job/resource (make-resource 5 50)
                      :job/submit-time submit-time
                      :job/user "user-2"
                      :job/uuid job5-uuid}
                     {:job/expected-runtime 30000
                      :job/resource (make-resource 6 60)
                      :job/submit-time submit-time
                      :job/user "user-3"
                      :job/uuid job6-uuid}
                     {:job/resource (make-resource 7 70)
                      :job/submit-time submit-time
                      :job/user "user-1"
                      :job/uuid job7-uuid}
                     {:job/expected-runtime 50000
                      :job/resource (make-resource 8 80)
                      :job/submit-time submit-time
                      :job/user "user-2"
                      :job/uuid job8-uuid}]
        expected-request-body {"cpu_share" 200
                               "host_groups" [{"count" 100 "cpus" 40 "instance-type" "basic" "mem" 320000 "time-to-start" 100}]
                               "mem_share" 40000
                               "now" (.getTime current-time)
                               "opt_jobs" [{"batch" (str job1-uuid)
                                            "count" 1
                                            "cpus" 1
                                            "dur" 10000
                                            "expected_complete_time" 1100
                                            "mem" 10
                                            "ports" 0
                                            "running_host_group" 0
                                            "state" "running"
                                            "submit_time" (.getTime submit-time)
                                            "user" "user-1"
                                            "uuid" (str job1-uuid)}
                                           {"batch" (str job2-uuid)
                                            "count" 1
                                            "cpus" 2
                                            "dur" 20000
                                            "expected_complete_time" 8900
                                            "mem" 20
                                            "ports" 0
                                            "running_host_group" 0
                                            "state" "running"
                                            "submit_time" (.getTime submit-time)
                                            "user" "user-2"
                                            "uuid" (str job2-uuid)}
                                           {"batch" (str job3-uuid)
                                            "count" 1
                                            "cpus" 3
                                            "dur" 30000
                                            "expected_complete_time" 18900
                                            "mem" 30
                                            "ports" 0
                                            "running_host_group" 0
                                            "state" "running"
                                            "submit_time" (.getTime submit-time)
                                            "user" "user-3"
                                            "uuid" (str job3-uuid)}
                                           {"batch" (str job4-uuid)
                                            "count" 1
                                            "cpus" 4
                                            "dur" 10000
                                            "expected_complete_time" -1
                                            "mem" 40
                                            "ports" 0
                                            "running_host_group" -1
                                            "state" "waiting"
                                            "submit_time" (.getTime submit-time)
                                            "user" "user-1"
                                            "uuid" (str job4-uuid)}
                                           {"batch" (str job5-uuid)
                                            "count" 1
                                            "cpus" 5
                                            "dur" 20000
                                            "expected_complete_time" -1
                                            "mem" 50
                                            "ports" 0
                                            "running_host_group" -1
                                            "state" "waiting"
                                            "submit_time" (.getTime submit-time)
                                            "user" "user-2"
                                            "uuid" (str job5-uuid)}
                                           {"batch" (str job6-uuid)
                                            "count" 1
                                            "cpus" 6
                                            "dur" 30000
                                            "expected_complete_time" -1
                                            "mem" 60
                                            "ports" 0
                                            "running_host_group" -1
                                            "state" "waiting"
                                            "submit_time" (.getTime submit-time)
                                            "user" "user-3"
                                            "uuid" (str job6-uuid)}
                                           {"batch" (str job7-uuid)
                                            "count" 1
                                            "cpus" 7
                                            "dur" 120000
                                            "expected_complete_time" -1
                                            "mem" 70
                                            "ports" 0
                                            "running_host_group" -1
                                            "state" "waiting"
                                            "submit_time" (.getTime submit-time)
                                            "user" "user-1"
                                            "uuid" (str job7-uuid)}
                                           {"batch" (str job8-uuid)
                                            "count" 1
                                            "cpus" 8
                                            "dur" 50000
                                            "expected_complete_time" -1
                                            "mem" 80
                                            "ports" 0
                                            "running_host_group" -1
                                            "state" "waiting"
                                            "submit_time" (.getTime submit-time)
                                            "user" "user-2"
                                            "uuid" (str job8-uuid)}]
                               "opt_params" {"param1" "value1"
                                             "param2" "value2"}
                               "seed" (.getTime current-time)
                               "step_list" [0 30 60]
                               "user_to_ema_usage" {"user-1" 1.25E-4
                                                    "user-2" 2.5E-4
                                                    "user-3" 3.75E-4}}
        result-job-ids [(str job4-uuid) (str job6-uuid) (str job8-uuid)]]
    (with-redefs [http/post (fn [in-server-url {:keys [as body content-type]}]
                              (is (= server-url in-server-url))
                              (is (= as :json-string-keys))
                              (is (= content-type :json))
                              (is (= expected-request-body
                                     (-> body json/read-str)))
                              {:body {"schedule" {"0" {"suggested-matches" {"0" result-job-ids}}}}})
                  t/now (constantly (tc/from-long current-time))]
      (let [server-optimizer (optimizer/create-optimizer-server config)
            optimizer-schedule (optimizer/produce-schedule server-optimizer queued-jobs running-tasks)]
        (is (= {0 {:suggested-matches (map #(UUID/fromString %) result-job-ids)}}
               optimizer-schedule))))))

;; Tests to make sure data flows and validates properly
(deftest test-pool-optimizer-cycle
  (let [optimizer (reify optimizer/Optimizer
                    (produce-schedule [_ queue _]
                      {0 {:suggested-matches (map :job/uuid queue)}}))
        pool-name "no-pool"
        queue [{:job/uuid (UUID/randomUUID)}
               {:job/uuid (UUID/randomUUID)}
               {:job/uuid (UUID/randomUUID)}]
        schedule (optimizer/pool-optimizer-cycle
                   pool-name
                   (fn get-queue [in-pool-name]
                     (is (= pool-name in-pool-name))
                     queue)
                   (fn get-running [in-pool-name]
                     (is (= pool-name in-pool-name))
                     [])
                   optimizer)]
    (is (= {0 {:suggested-matches (map :job/uuid queue)}} schedule))))

(deftest test-optimizer-cycle!
  (let [pool-names ["foo" "bar" "baz"]
        pool-name->optimizer-schedule-job-ids (pc/map-from-keys
                                                (fn [_]
                                                  {0 {:suggested-matches [(UUID/randomUUID) (UUID/randomUUID)]}})
                                                pool-names)
        optimizer (Object.)
        pool-name->optimizer-schedule-job-ids-atom (atom {})]
    (with-redefs [optimizer/pool-optimizer-cycle
                  (fn [pool-name & _]
                    (pool-name->optimizer-schedule-job-ids pool-name))]
      (optimizer/optimizer-cycle!
        optimizer
        (constantly pool-names)
        nil
        nil
        pool-name->optimizer-schedule-job-ids-atom)
      (is (= (pc/map-vals #(get-in % [0 :suggested-matches] []) pool-name->optimizer-schedule-job-ids)
             @pool-name->optimizer-schedule-job-ids-atom)))))
