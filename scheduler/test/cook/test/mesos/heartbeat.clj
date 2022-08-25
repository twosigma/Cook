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

(ns cook.test.mesos.heartbeat
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [cook.mesos.heartbeat :as heartbeat]
            [cook.test.postgres]
            [cook.test.testutil :refer [create-dummy-instance create-dummy-job restore-fresh-database! setup]]
            [datomic.api :as d :refer [q]]))

(use-fixtures :once cook.test.postgres/with-pg-db)

(deftest test-update-heartbeat
  (testing
      (let [task-id "task-0"
            old-ch (async/chan)
            new-ch (async/chan)
            state [#{old-ch} {task-id old-ch} {old-ch task-id}]]
        (is (= [#{new-ch} {task-id new-ch} {new-ch task-id}]
               (heartbeat/update-heartbeat state task-id new-ch))))))

(deftest test-handle-timeout
  (testing
      (let [instance-id 0
            task-id "task-0"
            timeout-ch (async/chan)
            state [#{timeout-ch} {task-id timeout-ch} {timeout-ch task-id}]
            db [[instance-id :instance/task-id task-id]]]
        (is (= [[#{} {} {}] [:instance/update-state instance-id :instance.status/failed [:reason/name :heartbeat-lost]]])
            (heartbeat/handle-timeout state db timeout-ch)))))

(deftest test-sync-with-datomic
  (setup)
  (testing
    (let [datomic-uri "datomic:mem://test-sync-with-datomic"
          conn (restore-fresh-database! datomic-uri)
          job-id-0 (create-dummy-job conn)
          job-id-1 (create-dummy-job conn)
          job-id-2 (create-dummy-job conn :custom-executor? false)
          task-id-0 "task-0"
          task-id-1 "task-1"
          task-id-2 "task-2"
          ;; a task / an instance has been tracked.
          instance-id-0 (create-dummy-instance conn job-id-0 :instance-status :instance.status/running :task-id task-id-0)
          ;; a task / an instance has not be tracked and it uses a custom executor by default
          instance-id-1 (create-dummy-instance conn job-id-1 :instance-status :instance.status/running :task-id task-id-1)
          ;; a task / an instance has not be tracked but it does not use any custom executor.
          instance-id-2 (create-dummy-instance conn job-id-2 :instance-status :instance.status/running :task-id task-id-2)
          test-db (d/db conn)
          timeout-ch-0 (async/chan)
          timeout-ch-1 (async/chan)
          state [#{timeout-ch-0} {task-id-0 timeout-ch-0} {timeout-ch-0 task-id-0}]
          [new-timeout-chs new-task->ch new-ch->task] (heartbeat/sync-with-datomic state test-db)]
      (is (= #{task-id-0 task-id-1} (set (keys new-task->ch))))
      (doseq [task-id [task-id-0 task-id-1]]
        (is (= task-id (-> task-id (new-task->ch) (new-ch->task)))))
      (doseq [ch new-timeout-chs]
        (is (= ch (-> ch (new-ch->task) (new-task->ch)))))
      (is (= new-timeout-chs (set (vals new-task->ch)) (set (keys new-ch->task)))))))

;(run-tests)
