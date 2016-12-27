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

(ns cook.test.mesos.schema
  (:use clojure.test)
  (:require [cook.mesos.schema :as schema]
            [cook.test.testutil :as testutil :refer (create-dummy-instance create-dummy-job restore-fresh-database!)]
            [datomic.api :as d :refer (q db)]))

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
        (verify-state-transition :instance.status/unknown :instance.status/success :instance.status/unknown))
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
   & {:keys [old-job-state new-job-state retry-count old-reasons new-reasons]
      :or {retry-count 5
           old-reasons nil
           new-reasons nil}
      :as job-keys}]
  (let [uri datomic-uri
        conn (restore-fresh-database! uri)
        job (create-dummy-job conn :job-state old-job-state :retry-count retry-count)
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


(deftest test-instance-update-state-with-job-state
    (testing "Instance initially unknown"
        (verify-job-state-transition [:instance.status/unknown] [:instance.status/unknown] :old-job-state :job.state/running :new-job-state :job.state/running)
        (verify-job-state-transition [:instance.status/unknown] [:instance.status/running] :old-job-state :job.state/running :new-job-state :job.state/running)
        (verify-job-state-transition [:instance.status/unknown] [:instance.status/success] :old-job-state :job.state/running :new-job-state :job.state/running)
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
      (verify-job-state-transition [:instance.status/failed :instance.status/unknown] [nil :instance.status/success] :old-job-state :job.state/running :new-job-state :job.state/running)
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
        :new-job-state :job.state/completed)))

(comment
  (run-tests))
