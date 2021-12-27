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
(ns cook.test.unscheduled
  (:require [clojure.test :refer :all]
            [cook.test.postgres]
            [cook.quota :as quota]
            [cook.rate-limit :as rate-limit]
            [cook.test.testutil :refer [create-dummy-instance create-dummy-job restore-fresh-database! setup]]
            [cook.tools :as tools]
            [cook.unscheduled :as u]
            [datomic.api :as d]))

(use-fixtures :once cook.test.postgres/with-pg-db)

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

(deftest test-fenzo-placement
  (let [conn (restore-fresh-database! "datomic:mem://test-fenzo-placement")
        placement-failures (str {:constraints {"novel_host_constraint" 3}
                                 :resources {"mem" 14
                                             "cpus" 8}})]
    (is (= (u/check-fenzo-placement conn
                                    {:db/id (d/tempid :db.part/user)
                                     :job/under-investigation true
                                     :job/last-fenzo-placement-failure placement-failures})
           ["The job couldn't be placed on any available hosts."
            {:reasons
             [{:reason "Not enough mem available." :host_count 14}
              {:reason "Not enough cpus available." :host_count 8}
              {:reason "Job already ran on this host." :host_count 3}]}]))))

(deftest test-reasons
  (setup)
  (let [conn (restore-fresh-database! "datomic:mem://test-unscheduled")
        _ (quota/set-quota! conn "mforsythr" nil "test-reasons" :count 2)
        running-job-id1 (-> (create-dummy-job conn :user "mforsythr"
                                              :ncpus 1.0 :memory 3.0
                                              :job-state :job.state/running))
        running-job-id2 (-> (create-dummy-job conn :user "mforsythr"
                                              :ncpus 1.0 :memory 3.1
                                              :job-state :job.state/running))
        uncommitted-job-id (create-dummy-job conn :user "mforsythr"
                                             :job-state :job.state/waiting
                                             :committed? false)
        waiting-job-id (-> (create-dummy-job conn :user "mforsythr"
                                             :ncpus 1.0 :memory 3.0
                                             :job-state :job.state/waiting
                                             :retry-count 2))
        _ (dotimes [n 2]  (create-dummy-instance conn running-job-id1))
        _ (dotimes [n 2]  (create-dummy-instance conn running-job-id2))
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
            running-job-ent1 (d/touch (d/entity db running-job-id1))
            running-job-ent2 (d/touch (d/entity db running-job-id2))
            waiting-job-ent (d/touch (d/entity db waiting-job-id))
            running-job-uuids [(-> running-job-ent1 :job/uuid str)
                               (-> running-job-ent2 :job/uuid str)]
            reasons (u/reasons conn waiting-job-ent)]

        (is (= (nth reasons 0)
               ["Job has exhausted its maximum number of retries."
                {:max-retries 2, :instance-count 2}]))

        (is (= (nth reasons 1)
               ["The job would cause you to exceed resource quotas."
                {:count {:limit 2 :usage 3}}]))

        (is (= (nth reasons 2)
               ["You have 2 other jobs ahead in the queue."
                {:jobs running-job-uuids}]))

        (is (= (nth reasons 3)
               ["The job is now under investigation. Check back in a minute for more details!"
                {}]))))

    (testing "Waiting job returns multiple reasons, including an enforced launch rate limit."
      @(d/transact conn [[:db/add waiting-job-id :job/state :job.state/waiting]])
      (let [db (d/db conn)
            running-job-ent1 (d/touch (d/entity db running-job-id1))
            running-job-ent2 (d/touch (d/entity db running-job-id2))
            waiting-job-ent (d/touch (d/entity db waiting-job-id))
            running-job-uuids [(-> running-job-ent1 :job/uuid str)
                               (-> running-job-ent2 :job/uuid str)]
            reasons
            (with-redefs [rate-limit/enforce? (constantly true)
                          tools/pool->user->num-rate-limited-jobs (atom {:pool0 {"mforsythr" 3}})]
              (u/reasons conn waiting-job-ent))]
        (is (= reasons [["Job has exhausted its maximum number of retries."
                         {:max-retries 2, :instance-count 2}]
                        ["The job would cause you to exceed resource quotas."
                         {:count {:limit 2 :usage 3}}]
                        ["You are currently rate limited on how many jobs you launch per minute." {:num-ratelimited 3}]
                        ["You have 2 other jobs ahead in the queue."
                         {:jobs running-job-uuids}]
                        ["The job is now under investigation. Check back in a minute for more details!" {}]]))))

    (testing "Waiting job returns multiple reasons, including an unenforced rate launch rate limit."
      @(d/transact conn [[:db/add waiting-job-id :job/state :job.state/waiting]])
      (let [db (d/db conn)
            running-job-ent1 (d/touch (d/entity db running-job-id1))
            running-job-ent2 (d/touch (d/entity db running-job-id2))
            waiting-job-ent (d/touch (d/entity db waiting-job-id))
            running-job-uuids [(-> running-job-ent1 :job/uuid str)
                               (-> running-job-ent2 :job/uuid str)]
            reasons
            (with-redefs [rate-limit/time-until-out-of-debt-millis! (constantly 1999)]
              (u/reasons conn waiting-job-ent))]

        ; Note: No launch rate limit reason is returned here, because not enforcing.
        (is (= reasons [["Job has exhausted its maximum number of retries."
                         {:max-retries 2, :instance-count 2}]
                        ["The job would cause you to exceed resource quotas."
                         {:count {:limit 2 :usage 3}}]
                        ["You have 2 other jobs ahead in the queue."
                         {:jobs running-job-uuids}]
                        ["The job is now under investigation. Check back in a minute for more details!" {}]]))))))


(deftest test-check-queue-position
  (let [conn (restore-fresh-database! "datomic:mem://test-check-queue-position")
        waiting-job-ids (doall (for [x (range 0 500)]
                                 (create-dummy-job conn :user "mforsythq"
                                                   :ncpus 1.0 :memory 3.0
                                                   :job-state :job.state/waiting)))
        waiting-jobs (map #(d/entity (d/db conn) %) waiting-job-ids)
        running-job-ids (doall (for [x (range 0 50)]
                                 (let [job-id (create-dummy-job conn :user "mforsythq"
                                                                :ncpus 1.0 :memory 3.0
                                                             :job-state :job.state/running)]
                                   (create-dummy-instance conn job-id
                                                          :instance-status :instance.status/running)
                                   job-id)))
        running-jobs (map #(d/entity (d/db conn) %) running-job-ids)
        first-running-uuid (str (:job/uuid (first running-jobs)))]

    (let [reason (u/check-queue-position conn (second running-jobs) running-jobs (take 100 waiting-jobs))]
      (is (= "You have 1 other jobs ahead in the queue."))
      (is (= [first-running-uuid] (-> reason second :jobs))))
    (let [reason (u/check-queue-position conn (first waiting-jobs) running-jobs (take 100 waiting-jobs))]
      (is (= (str "You have " (count running-jobs) " other jobs ahead in the queue.")
             (first reason)))
      (is (= first-running-uuid (-> reason second :jobs first))))

    (let [reason (u/check-queue-position conn (last waiting-jobs) running-jobs (take 100 waiting-jobs))]
      (is (= "You have at least 150 other jobs ahead in the queue."
             (first reason)))
      (is (= first-running-uuid (-> reason second :jobs first))))))
