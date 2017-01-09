;;
;; Copyright (c) Two Sigma Open Source, LLC
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;  http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(ns cook.test.mesos.unscheduled
  (:use clojure.test)
  (:require [cook.mesos.unscheduled :as u]
            [datomic.api :as d]
            [cook.mesos.scheduler :as scheduler]
            [cook.mesos.util :as util]
            [cook.test.testutil :refer (create-dummy-instance
                                        create-dummy-job
                                        restore-fresh-database!)]
            [cook.mesos.quota :as quota]))

(defn resource-map->list
  [m]
  (map (fn [[k v]]
         {:resource/type (keyword "resource.type" (name k))
          :resource/amount v}) m))

(deftest test-how-job-would-exceed-resource-limits
  (let [quota {:mem 6000 :cpus 8 :count 5}
        running-jobs [{:job/resource (resource-map->list {:mem 3000 :cpus 4})}
                      {:job/resource (resource-map->list {:mem 2000 :cpus 2})}]]

    (testing "exceeding both mem and cpu"
      (is (= (u/how-job-would-exceed-resource-limits
              quota running-jobs
              {:job/resource (resource-map->list {:mem 4000 :cpus 4})})
             {:mem {:limit 6000, :usage 9000}, :cpus {:limit 8, :usage 10}})))

    (testing "exceeding just 1 resource"
      (is (= (u/how-job-would-exceed-resource-limits
              quota running-jobs
              {:job/resource (resource-map->list {:mem 2000 :cpus 1})})
             {:mem {:limit 6000, :usage 7000}})))

    (testing "exceeding cpus and count"
      (is (= (u/how-job-would-exceed-resource-limits
              (merge quota {:count 2}) running-jobs
              {:job/resource (resource-map->list {:mem 500 :cpus 4})})
             {:cpus {:limit 8, :usage 10} :count {:limit 2 :usage 3}})))))

(def datomic-uri "datomic:mem://test-unscheduled")

(deftest test-reasons
  (let [conn (restore-fresh-database! datomic-uri)
        _ (quota/set-quota! conn "mforsyth" :count 2)
        running-job-id1 (-> (create-dummy-job conn :user "mforsyth"
                                              :ncpus 1.0 :memory 3.0
                                              :job-state :job.state/running))
        running-job-id2 (-> (create-dummy-job conn :user "mforsyth"
                                              :ncpus 1.0 :memory 3.0
                                              :job-state :job.state/running))
        waiting-job-id (-> (create-dummy-job conn :user "mforsyth"
                                             :ncpus 1.0 :memory 3.0
                                             :job-state :job.state/waiting
                                             :retry-count 2))
        _ (dotimes [n 2]  (create-dummy-instance conn running-job-id1))
        _ (dotimes [n 2]  (create-dummy-instance conn waiting-job-id
                                                 :instance-status :instance.status/failed))]

    (testing "If job isn't actually waiting, don't examine any other reasons"
      @(d/transact conn [[:db/add waiting-job-id :job/state :job.state/running]])
      (let [db (d/db conn)
            waiting-job-ent (d/touch (d/entity db waiting-job-id))]
        (is (= (u/reasons conn waiting-job-ent)
               [["The job is running now." {}]]))))

    (testing "Waiting job returns multiple reasons, and is placed under investigation."
      @(d/transact conn [[:db/add waiting-job-id :job/state :job.state/waiting]])
      (let [db (d/db conn)
            waiting-job-ent (d/touch (d/entity db waiting-job-id))]
        (is (= (u/reasons conn waiting-job-ent)
               '(["Job has exhausted its maximum number of retries."
                  {:max-retries 2, :instance-count 2}]
                 ["The job is now under investigation. Check back in a minute for more details!"
                  {}])))))))
