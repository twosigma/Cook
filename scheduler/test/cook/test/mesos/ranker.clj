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

(ns cook.test.mesos.ranker
  (:use [clojure.test])
  (:require [clojure.core.async :as async]
            [cook.mesos.ranker :as ranker]
            [cook.mesos.share :as share]
            [cook.mesos.util :as util]
            [cook.test.testutil :as tu]
            [datomic.api :as d]))

(deftest test-sort-jobs-by-dru-category
  (let [uri "datomic:mem://test-sort-jobs-by-dru"
        conn (tu/restore-fresh-database! uri)
        j1 (tu/create-dummy-job conn :user "u1" :ncpus 1.0 :memory 3.0 :job-state :job.state/running)
        j2 (tu/create-dummy-job conn :user "u1" :ncpus 1.0 :memory 5.0)
        j3 (tu/create-dummy-job conn :user "u1" :ncpus 1.0 :memory 2.0)
        j4 (tu/create-dummy-job conn :user "u1" :ncpus 5.0 :memory 5.0)
        j5 (tu/create-dummy-job conn :user "u2" :ncpus 6.0 :memory 6.0 :job-state :job.state/running)
        j6 (tu/create-dummy-job conn :user "u2" :ncpus 5.0 :memory 5.0)
        j7 (tu/create-dummy-job conn :user "sunil" :ncpus 5.0 :memory 10.0 :job-state :job.state/running)
        j8 (tu/create-dummy-job conn :user "sunil" :ncpus 5.0 :memory 10.0)
        _ (tu/create-dummy-instance conn j1)
        _ (tu/create-dummy-instance conn j5)
        _ (tu/create-dummy-instance conn j7)]

    (testing "sort-jobs-by-dru default share for everyone"
      (let [_ (share/set-share! conn "default" nil
                                "limits for new cluster"
                                :mem 10.0 :cpus 10.0)
            db (d/db conn)]
        (is (= [j2 j3 j6 j4 j8] (map :db/id (:normal (ranker/sort-jobs-by-dru-category db)))))))

    (testing "sort-jobs-by-dru one user has non-default share"
      (let [_ (share/set-share! conn "default" nil "limits for new cluster" :mem 10.0 :cpus 10.0)
            _ (share/set-share! conn "sunil" nil "needs more resources" :mem 100.0 :cpus 100.0)
            db (d/db conn)]
        (is (= [j8 j2 j3 j6 j4] (map :db/id (:normal (ranker/sort-jobs-by-dru-category db))))))))

  (testing "test-sort-jobs-by-dru:normal-jobs"
    (let [uri "datomic:mem://test-sort-jobs-by-dru-normal-jobs"
          conn (tu/restore-fresh-database! uri)
          j1n (tu/create-dummy-job conn :user "u1" :job-state :job.state/waiting :memory 1000 :ncpus 1.0)
          j2n (tu/create-dummy-job conn :user "u1" :job-state :job.state/waiting :memory 1000 :ncpus 1.0 :priority 90)
          j3n (tu/create-dummy-job conn :user "u2" :job-state :job.state/waiting :memory 1500 :ncpus 1.0)
          j4n (tu/create-dummy-job conn :user "u2" :job-state :job.state/waiting :memory 1500 :ncpus 1.0 :priority 30)
          test-db (d/db conn)]
      (is (= [j2n j3n j1n j4n] (map :db/id (:normal (ranker/sort-jobs-by-dru-category test-db)))))
      (is (empty? (:gpu (ranker/sort-jobs-by-dru-category test-db))))))

  (testing "test-sort-jobs-by-dru:gpu-jobs"
    (let [uri "datomic:mem://test-sort-jobs-by-dru-gpu-jobs"
          conn (tu/restore-fresh-database! uri)
          j1g (tu/create-dummy-job conn :user "u1" :job-state :job.state/waiting :memory 1000 :ncpus 1.0 :gpus 10.0)
          j2g (tu/create-dummy-job conn :user "u1" :job-state :job.state/waiting :memory 1000 :ncpus 1.0 :gpus 25.0 :priority 90)
          j3g (tu/create-dummy-job conn :user "u2" :job-state :job.state/waiting :memory 1500 :ncpus 1.0 :gpus 20.0)
          j4g (tu/create-dummy-job conn :user "u2" :job-state :job.state/waiting :memory 1500 :ncpus 1.0 :gpus 10.0 :priority 30)
          test-db (d/db conn)]
      (is (empty? (:normal (ranker/sort-jobs-by-dru-category test-db))))
      (is (= [j3g j2g j4g j1g] (map :db/id (:gpu (ranker/sort-jobs-by-dru-category test-db)))))))

  (testing "test-sort-jobs-by-dru:mixed-jobs"
    (let [uri "datomic:mem://test-sort-jobs-by-dru-mixed-jobs"
          conn (tu/restore-fresh-database! uri)
          j1n (tu/create-dummy-job conn :user "u1" :job-state :job.state/waiting :memory 1000 :ncpus 1.0)
          j2n (tu/create-dummy-job conn :user "u1" :job-state :job.state/waiting :memory 1000 :ncpus 1.0 :priority 90)
          j3n (tu/create-dummy-job conn :user "u2" :job-state :job.state/waiting :memory 1500 :ncpus 1.0)
          j4n (tu/create-dummy-job conn :user "u2" :job-state :job.state/waiting :memory 1500 :ncpus 1.0 :priority 30)
          j1g (tu/create-dummy-job conn :user "u1" :job-state :job.state/waiting :memory 1000 :ncpus 1.0 :gpus 10.0)
          j2g (tu/create-dummy-job conn :user "u1" :job-state :job.state/waiting :memory 1000 :ncpus 1.0 :gpus 25.0 :priority 90)
          j3g (tu/create-dummy-job conn :user "u2" :job-state :job.state/waiting :memory 1500 :ncpus 1.0 :gpus 20.0)
          j4g (tu/create-dummy-job conn :user "u2" :job-state :job.state/waiting :memory 1500 :ncpus 1.0 :gpus 10.0 :priority 30)
          test-db (d/db conn)]
      (is (= [j2n j3n j1n j4n] (map :db/id (:normal (ranker/sort-jobs-by-dru-category test-db)))))
      (is (= [j3g j2g j4g j1g] (map :db/id (:gpu (ranker/sort-jobs-by-dru-category test-db))))))))

(deftest test-gpu-share-prioritization
  (let [uri "datomic:mem://test-gpu-shares"
        conn (tu/restore-fresh-database! uri)
        j1 (tu/create-dummy-job conn :user "u1" :ncpus 5.0 :memory 5.0 :gpus 1.0)
        j2 (tu/create-dummy-job conn :user "u1" :ncpus 5.0 :memory 5.0 :gpus 1.0)
        j3 (tu/create-dummy-job conn :user "u1" :ncpus 5.0 :memory 5.0 :gpus 1.0)
        j4 (tu/create-dummy-job conn :user "u1" :ncpus 5.0 :memory 5.0 :gpus 1.0)
        j5 (tu/create-dummy-job conn :user "u2" :ncpus 5.0 :memory 5.0 :gpus 1.0)
        j6 (tu/create-dummy-job conn :user "u2" :ncpus 5.0 :memory 5.0 :gpus 1.0)
        ; Update ljin-1 to running
        inst (tu/create-dummy-instance conn j1 :instance-status :instance.status/unknown)
        _ @(d/transact conn [[:instance/update-state inst :instance.status/running [:reason/name :unknown]]])
        _ (share/set-share! conn "default" nil
                            "limits for new cluster"
                            :cpus 1.0 :mem 2.0 :gpus 1.0)]
    (testing "one user has double gpu share"
      (let [_ (share/set-share! conn "u1" nil
                                "Needs some GPUs"
                                :gpus 2.0)
            db (d/db conn)]
        (is (= [j2 j5 j3 j4 j6] (map :db/id (:gpu (ranker/sort-jobs-by-dru-category db)))))))
    (testing "one user has single gpu share"
      (let [_ (share/set-share! conn "u1" nil
                                "Doesn't need lots of gpus"
                                :gpus 1.0)
            db (d/db conn)]
        (is (= [j5 j6 j2 j3 j4] (map :db/id (:gpu (ranker/sort-jobs-by-dru-category db)))))))))

(deftest test-filter-offensive-jobs
  (let [uri "datomic:mem://test-filter-offensive-jobs"
        conn (tu/restore-fresh-database! uri)
        constraints {:memory-gb 10.0
                     :cpus 5.0}
        ;; a job which breaks the memory constraint
        job-id-1 (tu/create-dummy-job conn :user "tsram"
                                      :job-state :job.state/waiting
                                      :memory (* 1024 (+ (:memory-gb constraints) 2.0))
                                      :ncpus (- (:cpus constraints) 1.0))
        ;; a job which breaks the cpu constraint
        job-id-2 (tu/create-dummy-job conn :user "tsram"
                                      :job-state :job.state/waiting
                                      :memory (* 1024 (- (:memory-gb constraints) 2.0))
                                      :ncpus (+ (:cpus constraints) 1.0))
        ;; a job which follows all constraints
        job-id-3 (tu/create-dummy-job conn :user "tsram"
                                      :job-state :job.state/waiting
                                      :memory (* 1024 (- (:memory-gb constraints) 2.0))
                                      :ncpus (- (:cpus constraints) 1.0))
        test-db (d/db conn)
        job-entity-1 (d/entity test-db job-id-1)
        job-entity-2 (d/entity test-db job-id-2)
        job-entity-3 (d/entity test-db job-id-3)
        jobs [job-entity-1 job-entity-2 job-entity-3]
        offensive-jobs-ch (async/chan (count jobs))
        offensive-jobs #{job-entity-1 job-entity-2}]
    (is (= #{job-entity-3} (set (ranker/filter-offensive-jobs constraints offensive-jobs-ch jobs))))
    (let [received-offensive-jobs (async/<!! offensive-jobs-ch)]
      (is (= (set received-offensive-jobs) offensive-jobs))
      (async/close! offensive-jobs-ch))))

(deftest test-rank-jobs
  (let [uri "datomic:mem://test-rank-jobs"
        conn (tu/restore-fresh-database! uri)
        constraints {:memory-gb 10.0
                     :cpus 5.0}
        ;; a job which breaks the memory constraint
        job-id-1 (tu/create-dummy-job conn :user "tsram"
                                      :job-state :job.state/waiting
                                      :memory (* 1024 (+ (:memory-gb constraints) 2.0))
                                      :ncpus (- (:cpus constraints) 1.0))
        ;; a job which follows all constraints
        job-id-2 (tu/create-dummy-job conn :user "tsram"
                                      :job-state :job.state/waiting
                                      :memory (* 1024 (- (:memory-gb constraints) 2.0))
                                      :ncpus (- (:cpus constraints) 1.0))
        test-db (d/db conn)
        _ (d/entity test-db job-id-1)
        job-entity-2 (d/entity test-db job-id-2)
        offensive-jobs-ch (ranker/make-offensive-job-stifler conn)
        offensive-job-filter (partial ranker/filter-offensive-jobs constraints offensive-jobs-ch)]
    (is (= {:normal (list (util/job-ent->map job-entity-2))
            :gpu ()}
           (ranker/rank-jobs test-db offensive-job-filter)))))
