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

(ns cook.test.schema
  (:require [clojure.test :refer :all]
            [cook.test.postgres]
            [cook.test.testutil :refer [create-dummy-instance create-dummy-job restore-fresh-database!]]
            [datomic.api :as d :refer [db q]]))

(use-fixtures :once cook.test.postgres/with-pg-db)

(def datomic-uri "datomic:mem://test")

(deftest test-instance-update-state
  (let [uri datomic-uri
        conn (restore-fresh-database! uri)
        job (create-dummy-job conn)
        verify-state-transition (fn [old-state target-state new-state]
                                  (let [instance (create-dummy-instance conn job :instance-status old-state)]
                                    @(d/transact conn [[:instance/update-state instance target-state [:reason/name :unknown]]])
                                    (is (= new-state
                                           (ffirst (q '[:find ?status
                                                        :in $ ?i
                                                        :where
                                                        [?i :instance/status ?s]
                                                        [?s :db/ident ?status]]
                                                      (db conn) instance))))))]
    (testing "UUU"
        (verify-state-transition :instance.status/unknown :instance.status/unknown :instance.status/unknown))
    (testing "URU"
        (verify-state-transition :instance.status/unknown :instance.status/running :instance.status/running))
    (testing "USU"
        (verify-state-transition :instance.status/unknown :instance.status/success :instance.status/success))
    (testing "UFF"
        (verify-state-transition :instance.status/unknown :instance.status/failed :instance.status/failed))

    (testing "RUR"
        (verify-state-transition :instance.status/running :instance.status/unknown :instance.status/running))
    (testing "RRR"
        (verify-state-transition :instance.status/running :instance.status/running :instance.status/running))
    (testing "RSS"
        (verify-state-transition :instance.status/running :instance.status/success :instance.status/success))
    (testing "RFF"
        (verify-state-transition :instance.status/running :instance.status/failed :instance.status/failed))

    (testing "SUS"
        (verify-state-transition :instance.status/success :instance.status/unknown :instance.status/success))
    (testing "SRS"
        (verify-state-transition :instance.status/success :instance.status/running :instance.status/success))
    (testing "SSS"
        (verify-state-transition :instance.status/success :instance.status/success :instance.status/success))
    (testing "SFS"
        (verify-state-transition :instance.status/success :instance.status/failed :instance.status/success))

    (testing "FUF"
        (verify-state-transition :instance.status/failed :instance.status/unknown :instance.status/failed))
    (testing "FRF"
        (verify-state-transition :instance.status/failed :instance.status/running :instance.status/failed))
    (testing "FSF"
        (verify-state-transition :instance.status/failed :instance.status/success :instance.status/failed))
    (testing "FFF"
        (verify-state-transition :instance.status/failed :instance.status/failed :instance.status/failed))))

(defn verify-job-state-transition
  [old-instance-states target-instance-states
   & {:keys [old-job-state new-job-state retry-count old-reasons new-reasons disable-mea-culpa-retries]
      :or {retry-count 5
           old-reasons nil
           new-reasons nil
           disable-mea-culpa-retries false}
      :as job-keys}]
  (let [uri datomic-uri
        conn (restore-fresh-database! uri)
        job (create-dummy-job conn :job-state old-job-state :retry-count retry-count :disable-mea-culpa-retries disable-mea-culpa-retries)
        n-instances (count old-instance-states)
        old-reasons (if (nil? old-reasons) (repeat n-instances :unknown) old-reasons)
        new-reasons (if (nil? new-reasons) (repeat n-instances :unknown) new-reasons)
        instances (map (fn [old-state old-reason] (create-dummy-instance conn job :instance-status old-state :reason old-reason)) old-instance-states old-reasons)
        instance-updates (remove nil?
                                 (map (fn [instance target-state reason]
                                        ;; Use task id to ensure we don't break lookup refs again
                                        (let [task-id (:instance/task-id (d/entity (db conn) instance))]
                                          (if (nil? target-state)
                                            nil
                                              [:instance/update-state [:instance/task-id task-id] target-state [:reason/name reason]])))
                                      instances target-instance-states new-reasons))]
    @(d/transact conn (into [] instance-updates))
    (is (= new-job-state
           (ffirst (q '[:find ?status
                        :in $ ?j
                        :where
                        [?j :job/state ?s]
                        [?s :db/ident ?status]]
                      (db conn) job))))))

(deftest test-mea-culpa-retry-on-mesos-executor-terminated
  (testing "single retry with disable-mea-culpa-retries true"
    (verify-job-state-transition
     [:instance.status/running]
     [:instance.status/failed]
     :old-reasons [:mesos-executor-terminated]
     :new-reasons [:mesos-executor-terminated]
     :disable-mea-culpa-retries true
     :retry-count 1
     :old-job-state :job.state/running
     :new-job-state :job.state/completed))

  (testing "multiple retries with disable-mea-culpa retries true"
    (verify-job-state-transition
      [:instance.status/running]
      [:instance.status/failed]
      :old-reasons [:preempted-by-rebalancer]
      :new-reasons [:preempted-by-rebalancer]
      :disable-mea-culpa-retries false
      :retry-count 2
      :old-job-state :job.state/running
      :new-job-state :job.state/waiting)

    (verify-job-state-transition
     [:instance.status/running]
     [:instance.status/failed]
     :old-reasons [:mesos-executor-terminated]
     :new-reasons [:mesos-executor-terminated]
     :disable-mea-culpa-retries true
     :retry-count 2
     :old-job-state :job.state/running
     :new-job-state :job.state/waiting)

    (verify-job-state-transition
     [:instance.status/failed :instance.status/running]
     [nil :instance.status/failed]
     :old-reasons [:mesos-executor-terminated nil]
     :new-reasons [:mesos-executor-terminated :preempted-by-rebalancer]
     :disable-mea-culpa-retries true
     :retry-count 2
     :old-job-state :job.state/running
     :new-job-state :job.state/completed))

  (testing "single retry with disable-mea-culpa-retries false"
    (doseq [n (range 1 4)]
      (verify-job-state-transition
       (-> n dec (repeat :instance.status/failed) vec (conj :instance.status/running))
       (-> n dec (repeat nil) vec (conj :instance.status/failed))
       :old-reasons (repeat n :mesos-executor-terminated)
       :new-reasons (repeat n :mesos-executor-terminated)
       :disable-mea-culpa-retries false
       :retry-count 1
       :old-job-state :job.state/running
       :new-job-state :job.state/waiting))
    (verify-job-state-transition
     [:instance.status/failed :instance.status/failed :instance.status/failed :instance.status/running]
     [nil nil nil :instance.status/failed]
     :old-reasons (repeat 4 :mesos-executor-terminated)
     :new-reasons (repeat 4 :mesos-executor-terminated)
     :disable-mea-culpa-retries false
     :retry-count 1
     :old-job-state :job.state/running
     :new-job-state :job.state/completed))

  (testing "multiple retries with disable-mea-culpa-retrues false"
    (doseq [n (range 1 5)]
      (verify-job-state-transition
       (-> n dec (repeat :instance.status/failed) vec (conj :instance.status/running))
       (-> n dec (repeat nil) vec (conj :instance.status/failed))
       :old-reasons (repeat n :mesos-executor-terminated)
       :new-reasons (repeat n :mesos-executor-terminated)
       :disable-mea-culpa-retries false
       :retry-count 2
       :old-job-state :job.state/running
       :new-job-state :job.state/waiting))
    (verify-job-state-transition
     [:instance.status/failed :instance.status/failed :instance.status/failed :instance.status/failed :instance.status/running]
     [nil nil nil nil :instance.status/failed]
     :old-reasons (repeat 5 :mesos-executor-terminated)
     :new-reasons (repeat 5 :mesos-executor-terminated)
     :disable-mea-culpa-retries false
     :retry-count 2
     :old-job-state :job.state/running
     :new-job-state :job.state/completed)))


(deftest test-reasons->attempts-consumed
  (let [uri "datomic:mem://test-reasons-attempts-consumed"
        conn (restore-fresh-database! uri)
        db (d/db conn)
        reasons [{:reason/code 100 :reason/mea-culpa? true}  ; A
                 {:reason/code 100 :reason/mea-culpa? true}  ; B
                 {:reason/code 100 :reason/mea-culpa? true} ; C
                 {:reason/code 101 :reason/mea-culpa? false} ; D
                 {:reason/code 101 :reason/mea-culpa? false} ; E
                 nil ; F
                 {:reason/code 102 :reason/mea-culpa? false} ; G
                 {:reason/code 103 :reason/mea-culpa? true :reason/failure-limit 1} ; H
                 {:reason/code 103 :reason/mea-culpa? true :reason/failure-limit 1} ; I
                 {:reason/code 104 :reason/mea-culpa? true :reason/failure-limit -1} ; J
                 {:reason/code 104 :reason/mea-culpa? true :reason/failure-limit -1}]] ; K
    (is (= (d/invoke db :job/reasons->attempts-consumed 0 true reasons) 11)) ; ABCDEFGHIJK (all of them)
    (is (= (d/invoke db :job/reasons->attempts-consumed 0 false reasons) 8)) ; ABCDEFGI (H is skipped: 1 retry for reason 103 and JK are skipped due to -1 limit)
    (is (= (d/invoke db :job/reasons->attempts-consumed 1 false reasons) 7)) ; BCDEFGI (A is skipped due to mea culpa failure limit 1)
    (is (= (d/invoke db :job/reasons->attempts-consumed 2 false reasons) 6)) ; CDEFGI (B is skipped due to mea culpa failure limit 2)
    (is (= (d/invoke db :job/reasons->attempts-consumed 3 false reasons) 5)) ; DEFGI (C is skipped due to mea culpa failure limit 3)
    (is (= (d/invoke db :job/reasons->attempts-consumed 5 false reasons) 5)))) ; Unchanged

(deftest test-instance-update-state-with-job-state
    (testing "Instance initially unknown"
        (verify-job-state-transition [:instance.status/unknown] [:instance.status/unknown] :old-job-state :job.state/running :new-job-state :job.state/running)
        (verify-job-state-transition [:instance.status/unknown] [:instance.status/running] :old-job-state :job.state/running :new-job-state :job.state/running)
        (verify-job-state-transition [:instance.status/unknown] [:instance.status/success] :old-job-state :job.state/running :new-job-state :job.state/completed)
        (verify-job-state-transition [:instance.status/unknown] [:instance.status/failed] :old-job-state :job.state/running :new-job-state :job.state/waiting))

    (testing "Instance intially running"
        (verify-job-state-transition [:instance.status/running] [:instance.status/success] :old-job-state :job.state/running :new-job-state :job.state/completed)
        (verify-job-state-transition [:instance.status/running] [:instance.status/failed] :old-job-state :job.state/running :new-job-state :job.state/waiting)
        (verify-job-state-transition [:instance.status/running] [:instance.status/running] :old-job-state :job.state/running :new-job-state :job.state/running)
        (verify-job-state-transition [:instance.status/running] [:instance.status/unknown] :old-job-state :job.state/running :new-job-state :job.state/running))

    (testing "Instance initially success"
        (verify-job-state-transition [:instance.status/success] [:instance.status/success] :old-job-state :job.state/completed :new-job-state :job.state/completed)
        (verify-job-state-transition [:instance.status/success] [:instance.status/failed] :old-job-state :job.state/completed :new-job-state :job.state/completed)
        (verify-job-state-transition [:instance.status/success] [:instance.status/running] :old-job-state :job.state/completed :new-job-state :job.state/completed)
        (verify-job-state-transition [:instance.status/success] [:instance.status/unknown] :old-job-state :job.state/completed :new-job-state :job.state/completed))

    (testing "Instance initially failed"
        (verify-job-state-transition [:instance.status/failed] [:instance.status/success] :old-job-state :job.state/waiting :new-job-state :job.state/waiting)
        (verify-job-state-transition [:instance.status/failed] [:instance.status/failed] :old-job-state :job.state/waiting :new-job-state :job.state/waiting)
        (verify-job-state-transition [:instance.status/failed] [:instance.status/running] :old-job-state :job.state/waiting :new-job-state :job.state/waiting)
        (verify-job-state-transition [:instance.status/failed] [:instance.status/unknown] :old-job-state :job.state/waiting :new-job-state :job.state/waiting))

    ;; Multi-instance cases
    (testing "Instances initially failed and running"
      (verify-job-state-transition [:instance.status/failed :instance.status/running] [nil :instance.status/success] :old-job-state :job.state/running :new-job-state :job.state/completed)
      (verify-job-state-transition [:instance.status/failed :instance.status/running] [nil :instance.status/failed] :old-job-state :job.state/running :new-job-state :job.state/waiting)
      (verify-job-state-transition [:instance.status/failed :instance.status/running] [nil :instance.status/unknown] :old-job-state :job.state/running :new-job-state :job.state/running)
      (verify-job-state-transition [:instance.status/failed :instance.status/running] [nil :instance.status/running] :old-job-state :job.state/running :new-job-state :job.state/running))

    (testing "Instances initially failed and unknown"
      (verify-job-state-transition [:instance.status/failed :instance.status/unknown] [nil :instance.status/success] :old-job-state :job.state/running :new-job-state :job.state/completed)
      (verify-job-state-transition [:instance.status/failed :instance.status/unknown] [nil :instance.status/failed] :old-job-state :job.state/running :new-job-state :job.state/waiting)
      (verify-job-state-transition [:instance.status/failed :instance.status/unknown] [nil :instance.status/unknown] :old-job-state :job.state/running :new-job-state :job.state/running)
      (verify-job-state-transition [:instance.status/failed :instance.status/unknown] [nil :instance.status/running] :old-job-state :job.state/running :new-job-state :job.state/running))

    ;; Cases demonstrating necessity of only changing one instance's status per transaction
    (testing "Multiple instances running to failed in same transaction"
      ;; State should change to waiting but because of the transaction issue, it stays as running!
      (verify-job-state-transition [:instance.status/running :instance.status/running] [:instance.status/failed :instance.status/failed] :old-job-state :job.state/running :new-job-state :job.state/running))
    (testing "Multiple instances running to failed in same transaction exhausting retry count"
      ;; State should change to completed but because of the transaction issue, it stays as running!
      (verify-job-state-transition [:instance.status/running :instance.status/running] [:instance.status/failed :instance.status/failed] :old-job-state :job.state/running :new-job-state :job.state/running :retry-count 2))
    ;; It's not clear what will happen here since the running->failed will insert a job change to waiting but running->success will add a job change to completed (in the same transaction)
    ;(testing
      ;(verify-state-transition [:instance.status/running :instance.status/running] [:instance.status/success :instance.status/failed] {:old-job-state :job.state/running :new-job-state :job.state/running}))

    (testing "Running out of retries, no mea-culpas"
      (verify-job-state-transition
        [:instance.status/failed :instance.status/failed :instance.status/running]
        [nil nil :instance.status/failed]
        :old-reasons [:unknown :unknown :unknown]
        :new-reasons [nil nil :unknown] ; Unknowns are counted against retries
        :retry-count 3
        :old-job-state :job.state/running
        :new-job-state :job.state/completed))

    (testing "Running out of retries, some mea-culpas"
      (verify-job-state-transition
        [:instance.status/failed :instance.status/failed :instance.status/failed :instance.status/running]
        [nil nil nil :instance.status/failed]
        :old-reasons [:preempted-by-rebalancer :unknown :unknown :unknown]
        :new-reasons [nil nil nil :unknown]
        :retry-count 3
        :old-job-state :job.state/running
        :new-job-state :job.state/completed))

    (testing "Still more retries left, thanks to mea-culpas"
      (verify-job-state-transition
        [:instance.status/failed :instance.status/failed :instance.status/running]
        [nil nil :instance.status/failed]
        :old-reasons [:preempted-by-rebalancer :preempted-by-rebalancer :unknown]
        :new-reasons [nil nil :unknown]
        :retry-count 3
        :old-job-state :job.state/running
        :new-job-state :job.state/waiting))

    (testing "Still more retries left, no mea-culpas involved"
      (verify-job-state-transition
        [:instance.status/failed :instance.status/failed :instance.status/running]
        [nil nil :instance.status/failed]
        :old-reasons [:unknown :unknown :unknown]
        :new-reasons [nil nil :unknown]
        :retry-count 4
        :old-job-state :job.state/running
        :new-job-state :job.state/waiting))



    (testing "Still more retries left, thanks to mea-culpas, edge case"
      (verify-job-state-transition
        [:instance.status/failed :instance.status/failed :instance.status/running]
        [nil nil :instance.status/failed]
        :old-reasons [:preempted-by-rebalancer :preempted-by-rebalancer :unknown]
        :new-reasons [nil nil :unknown]
        :retry-count 2
        :old-job-state :job.state/running
        :new-job-state :job.state/waiting))

    (testing "Do or do not. There is no try."
      (verify-job-state-transition
        [:instance.status/failed :instance.status/failed :instance.status/running]
        [nil nil :instance.status/failed]
        :old-reasons [:preempted-by-rebalancer :preempted-by-rebalancer :unknown]
        :new-reasons [nil nil :unknown]
        :retry-count 1
        :old-job-state :job.state/running
        :new-job-state :job.state/completed))
  (testing "No more retries with mea-culpas due to disable-mea-culpa-retries"
    (verify-job-state-transition
     [:instance.status/running]
     [:instance.status/failed]
     :old-reasons [:unknown]
     :new-reasons [:preempted-by-rebalancer]
     :retry-count 1
     :disable-mea-culpa-retries true
     :old-job-state :job.state/running
     :new-job-state :job.state/completed)))

(comment
  (run-tests))
