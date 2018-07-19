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
            [clojure.set :as set]
            [cook.mesos.ranker :as ranker]
            [cook.mesos.share :as share]
            [cook.mesos.util :as util]
            [cook.test.testutil :as tu]
            [datomic.api :as d]
            [plumbing.core :as pc]))

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

(deftest test-fairly-sort-jobs-by-dru-category
  (let [uri "datomic:mem://test-fairly-sort-jobs-by-dru-category"
        conn (tu/restore-fresh-database! uri)
        total-resources {:cpus 45 :mem 30}
        j1 (tu/create-dummy-job conn :name "j1a" :user "u1" :ncpus 1.0 :memory 8.0 :job-state :job.state/running)
        j2a (tu/create-dummy-job conn :name "j2a" :user "u1" :ncpus 2.0 :memory 1.0)
        j2b (tu/create-dummy-job conn :name "j2b" :user "u1" :ncpus 2.0 :memory 1.0)
        j2c (tu/create-dummy-job conn :name "j2c" :user "u1" :ncpus 2.0 :memory 1.0)
        _ (tu/create-dummy-job conn :name "j2d" :user "u1" :ncpus 2.0 :memory 1.0)
        _ (tu/create-dummy-job conn :name "j2e" :user "u1" :ncpus 2.0 :memory 1.0)
        _ (tu/create-dummy-job conn :name "j2f" :user "u1" :ncpus 2.0 :memory 1.0)
        j3 (tu/create-dummy-job conn :name "j3" :user "u1" :ncpus 1.0 :memory 7.0 :job-state :job.state/running)
        j4 (tu/create-dummy-job conn :name "j4" :user "u2" :ncpus 9.0 :memory 2.0 :priority 75)
        j5 (tu/create-dummy-job conn :name "j5" :user "u2" :ncpus 8.0 :memory 1.0 :job-state :job.state/running)
        j6 (tu/create-dummy-job conn :name "j6" :user "u2" :ncpus 4.0 :memory 4.0 :job-state :job.state/running)
        j7 (tu/create-dummy-job conn :name "j7" :user "u3" :ncpus 3.0 :memory 3.0)
        j8 (tu/create-dummy-job conn :name "j8" :user "u4" :ncpus 4.0 :memory 2.0)
        j9 (tu/create-dummy-job conn :name "j9" :user "u4" :ncpus 5.0 :memory 1.5)
        ja (tu/create-dummy-job conn :name "ja" :user "u5" :ncpus 3.0 :memory 1.0)
        jb (tu/create-dummy-job conn :name "jb" :user "u6" :ncpus 2.0 :memory 1.0)
        jc (tu/create-dummy-job conn :name "jc" :user "u7" :ncpus 1.0 :memory 1.0)]

    (tu/create-dummy-instance conn j1)
    (tu/create-dummy-instance conn j3)
    (tu/create-dummy-instance conn j5)
    (tu/create-dummy-instance conn j6)

    (share/set-share! conn "default" nil "test" :mem 10.0 :cpus 8.0)
    (share/set-share! conn "u1" nil "test" :mem 60.0 :cpus 16.0)

    (let [unfiltered-db (d/db conn)
          {:keys [all-tasks pending-tasks running-tasks]} (ranker/get-running-and-pending-tasks unfiltered-db)
          user->tasks (group-by #(-> % :job/_instance :job/user) all-tasks)
          user->requested-resources (ranker/user->tasks-to-user->requested-resources user->tasks)
          user->resource-weights (ranker/compute-user->resource-weights unfiltered-db (keys user->tasks))
          {:keys [under-share-users user->allocated-resources]}
          (ranker/compute-fair-share-resources total-resources user->requested-resources user->resource-weights)
          task-comparator (util/same-user-task-comparator)
          user->sorted-tasks (pc/map-vals #(sort task-comparator %) user->tasks)
          user->scheduled-job-ids (ranker/allocate-tasks user->sorted-tasks user->allocated-resources under-share-users)
          scheduled-job-ids (reduce set/union #{} (vals user->scheduled-job-ids))
          scheduled-pending-tasks (filter #(contains? scheduled-job-ids (-> % :job/_instance :db/id)) pending-tasks)
          category->sorted-jobs (ranker/sort-jobs-by-dru-category unfiltered-db scheduled-pending-tasks running-tasks)]

      (is (= {:pending {:scheduled 10
                        :total 13}
              :running {:total 4}}
             {:pending {:scheduled (count scheduled-pending-tasks)
                        :total (count pending-tasks)}
              :running {:total (count running-tasks)}}))

      (is (= {"u1" {:cpus 14.0 :mem 21.0}
              "u2" {:cpus 21.0 :mem 7.0}
              "u3" {:cpus 3.0 :mem 3.0}
              "u4" {:cpus 9.0 :mem 3.5}
              "u5" {:cpus 3.0 :mem 1.0}
              "u6" {:cpus 2.0 :mem 1.0}
              "u7" {:cpus 1.0 :mem 1.0}}
             user->requested-resources))

      (is (= {"u1" {:cpus 14.0 :mem 18.0}
              "u2" {:cpus 13.0 :mem 3.0}
              "u3" {:cpus 3.0 :mem 3.0}
              "u4" {:cpus 9.0 :mem 3.0}
              "u5" {:cpus 3.0 :mem 1.0}
              "u6" {:cpus 2.0 :mem 1.0}
              "u7" {:cpus 1.0 :mem 1.0}}
             user->allocated-resources))

      (is (= {:normal [jc jb j2a j2b j7 ja j2c j8 j9 j4]
              :gpu []}
             (pc/map-vals #(map :db/id %) category->sorted-jobs))))))

(defn exponential-cdf->x
  "https://en.wikipedia.org/wiki/Exponential_distribution#Cumulative_distribution_function"
  [lambda max-val]
  (int (min max-val (* lambda (Math/log (/ 1.0 (- 1.0 (Math/random))))))))

(comment
  deftest test-ranking-performance-comparison
  "Example output:
test-ranking-performance-comparison: set the share...
test-ranking-performance-comparison: creating 30000 jobs...
test-ranking-performance-comparison: created 30000 jobs.
test-ranking-performance-comparison: total-resources: {:cpus 100000.0, :mem 2.2E8}
test-ranking-performance-comparison: running-resources: {:cpus 78874.0, :mem 1.58763177E8}
test-ranking-performance-comparison: pending-resources: {:cpus 89831.0, :mem 1.81177645E8}
test-ranking-performance-comparison: preempt-resources: {:cpus 68178.0, :mem 1.37695167E8}
test-ranking-performance-comparison: non-preempt-resources: {:cpus 10696.0, :mem 2.106801E7}
test-ranking-performance-comparison: scheduled-resources: {:cpus 100527.0, :mem 2.02245655E8}
test-ranking-performance-comparison: currently running jobs: 3136
test-ranking-performance-comparison: preempt jobs: 2718
test-ranking-performance-comparison: num ideal-scheduled-job-ids: 3962
test-ranking-performance-comparison: num normal job-ids: 3544
...
  4 sort-jobs-by-dru-category:
{:normal 26864, :gpu 0}
Elapsed time: 996.155086msecs
  4 fairly-sort-jobs-by-dru-category:
{:normal 3544, :gpu 0}
Elapsed time: 650.760561msecs
"
  (let [uri "datomic:mem://test-ranking-performance-comparison"
        conn (tu/restore-fresh-database! uri)
        num-total-jobs 30000
        percent-running-jobs 0.10
        total-resources {:cpus 100000.0 :mem 220000000.0}
        num-users 100]

    (println "test-ranking-performance-comparison: set the share...")
    (share/set-share! conn "default" nil "test" :mem 1000000.0 :cpus 200.0)

    (println "test-ranking-performance-comparison: creating" num-total-jobs "jobs...")
    (dotimes [j num-total-jobs]
      (let [user (str "u" (exponential-cdf->x 12 num-users))
            cpus (inc (int (* 50 (Math/random))))
            mem (+ 256 (int (* 100000 (Math/random))))
            job-running? (< (Math/random) percent-running-jobs)
            job-state (if job-running? :job.state/running :job.state/waiting)]
        (cond->> (tu/create-dummy-job
                   conn
                   :job-state job-state
                   :memory mem
                   :name (str "job-" j)
                   :ncpus cpus
                   :priority (int (* 100 (Math/random)))
                   :user user)
                 job-running?
                 (tu/create-dummy-instance conn))))
    (println "test-ranking-performance-comparison: created" num-total-jobs "jobs.")

    (let [unfiltered-db (d/db conn)
          running-tasks (util/get-running-task-ents unfiltered-db)
          running-resources (->> (map #(-> % :job/_instance ranker/job->resources) running-tasks)
                                 (apply merge-with +))]
      (let [{:keys [category->sorted-pending-jobs ideal-scheduled-job-ids]} (ranker/fairly-sort-jobs-by-dru-category total-resources unfiltered-db)
            pending-resources (->> (map ranker/job->resources (-> category->sorted-pending-jobs :normal))
                                   (apply merge-with +))
            preempt-tasks (remove #(contains? ideal-scheduled-job-ids (-> % :job/_instance :db/id)) running-tasks)
            preempt-resources (->> preempt-tasks
                                   (map #(-> % :job/_instance ranker/job->resources))
                                   (apply merge-with +))
            non-preempt-resources (->> running-tasks
                                       (filter #(contains? ideal-scheduled-job-ids (-> % :job/_instance :db/id)))
                                       (map #(-> % :job/_instance ranker/job->resources))
                                       (apply merge-with +))
            scheduled-resources (merge-with + pending-resources non-preempt-resources)]
        (println "test-ranking-performance-comparison: total-resources:" total-resources)
        (println "test-ranking-performance-comparison: running-resources:" running-resources)
        (println "test-ranking-performance-comparison: pending-resources:" pending-resources)
        (println "test-ranking-performance-comparison: preempt-resources:" preempt-resources)
        (println "test-ranking-performance-comparison: non-preempt-resources:" non-preempt-resources)
        (println "test-ranking-performance-comparison: scheduled-resources:" scheduled-resources)
        (println "test-ranking-performance-comparison: currently running jobs:" (count running-tasks))
        (println "test-ranking-performance-comparison: preempt jobs:" (count preempt-tasks))
        (println "test-ranking-performance-comparison: num ideal-scheduled-job-ids:" (count ideal-scheduled-job-ids))
        (println "test-ranking-performance-comparison: num normal job-ids:" (-> category->sorted-pending-jobs :normal count)))

      (dotimes [i 5]
        (println "  " i "sort-jobs-by-dru-category:")
        (time
          (->> (ranker/sort-jobs-by-dru-category unfiltered-db)
               (pc/map-vals count)
               (println)))
        (println "  " i "fairly-sort-jobs-by-dru-category:")
        (time
          (->> (ranker/fairly-sort-jobs-by-dru-category total-resources unfiltered-db)
               :category->sorted-pending-jobs
               (pc/map-vals count)
               (println)))))))
