(ns cook.test.group
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clojure.test :refer :all]
            [cook.group :as group]
            [cook.test.postgres]
            [cook.test.testutil :refer [create-dummy-group create-dummy-instance create-dummy-job restore-fresh-database!]]
            [datomic.api :as d :refer [db q]]))

(use-fixtures :once cook.test.postgres/with-pg-db)

(deftest test-find-stragglers
  (let [uri "datomic:mem://test-find-stragglers"
        conn (restore-fresh-database! uri)]
    (testing "no straggler-handling"
      (let [group-ent-id (create-dummy-group conn)
            job-a (create-dummy-job conn :group group-ent-id)
            _ (create-dummy-instance conn job-a :instance-status :instance.status/success
                                     :start-time (tc/to-date (t/ago (t/hours 3)))
                                     :end-time (tc/to-date (t/ago (t/hours 2))))
            job-b (create-dummy-job conn :group group-ent-id)
            _ (create-dummy-instance conn job-b :instance-status :instance.status/running
                                     :start-time (tc/to-date (t/ago (t/minutes 30))))
            group-ent (d/entity (d/db conn) group-ent-id)]
        (is (nil? (seq (group/find-stragglers group-ent))))))
    (testing "quantile deviation straggler-handling, no stragglers"
      (let [straggler-handling {:straggler-handling/type :straggler-handling.type/quantile-deviation
                                :straggler-handling/parameters
                                {:straggler-handling.quantile-deviation/quantile 0.5
                                 :straggler-handling.quantile-deviation/multiplier 2.0}}
            group-ent-id (create-dummy-group conn :straggler-handling straggler-handling)
            job-a (create-dummy-job conn :group group-ent-id)
            _ (create-dummy-instance conn job-a :instance-status :instance.status/success
                                     :start-time (tc/to-date (t/ago (t/hours 3)))
                                     :end-time (tc/to-date (t/ago (t/hours 2))))
            job-b (create-dummy-job conn :group group-ent-id)
            _ (create-dummy-instance conn job-b :instance-status :instance.status/running
                                     :start-time (tc/to-date (t/ago (t/minutes 90))))
            group-ent (d/entity (d/db conn) group-ent-id)]
        (is (nil? (seq (group/find-stragglers group-ent))))))
    (testing "quantile deviation straggler-handling, stragglers found"
      (let [straggler-handling {:straggler-handling/type :straggler-handling.type/quantile-deviation
                                :straggler-handling/parameters
                                {:straggler-handling.quantile-deviation/quantile 0.5
                                 :straggler-handling.quantile-deviation/multiplier 2.0}}
            group-ent-id (create-dummy-group conn :straggler-handling straggler-handling)

            _ (create-dummy-instance conn
                                     (create-dummy-job conn :group group-ent-id)
                                     :instance-status :instance.status/success
                                     :start-time (tc/to-date (t/ago (t/hours 3)))
                                     :end-time (tc/to-date (t/ago (t/hours 2))))
            _ (create-dummy-instance conn
                                     (create-dummy-job conn :group group-ent-id)
                                     :instance-status :instance.status/success
                                     :start-time (tc/to-date (t/ago (t/hours 3)))
                                     :end-time (tc/to-date (t/ago (t/hours 2))))
            _ (create-dummy-instance conn
                                     (create-dummy-job conn :group group-ent-id)
                                     :instance-status :instance.status/success
                                     :start-time (tc/to-date (t/ago (t/hours 3)))
                                     :end-time (tc/to-date (t/ago (t/hours 2))))
            _ (create-dummy-instance conn
                                     (create-dummy-job conn :group group-ent-id)
                                     :instance-status :instance.status/success
                                     :start-time (tc/to-date (t/ago (t/hours 3)))
                                     :end-time (tc/to-date (t/ago (t/hours 2))))
            _ (create-dummy-instance conn
                                     (create-dummy-job conn :group group-ent-id)
                                     :instance-status :instance.status/success
                                     :start-time (tc/to-date (t/ago (t/hours 3)))
                                     :end-time (tc/to-date (t/ago (t/hours 2))))
            _ (create-dummy-instance conn
                                     (create-dummy-job conn :group group-ent-id)
                                     :instance-status :instance.status/success
                                     :start-time (tc/to-date (t/ago (t/hours 3)))
                                     :end-time (tc/to-date (t/ago (t/hours 2))))
            job-not-straggler (create-dummy-job conn :group group-ent-id)
            _ (create-dummy-instance conn job-not-straggler :instance-status :instance.status/running
                                     :start-time (tc/to-date (t/ago (t/minutes 100))))
            job-straggler-a (create-dummy-job conn :group group-ent-id)
            straggler-a (create-dummy-instance conn job-straggler-a :instance-status :instance.status/running
                                             :start-time (tc/to-date (t/ago (t/minutes 190))))
            job-straggler-b (create-dummy-job conn :group group-ent-id)
            straggler-b (create-dummy-instance conn job-straggler-b :instance-status :instance.status/running
                                             :start-time (tc/to-date (t/ago (t/minutes 121))))
            db (d/db conn)
            group-ent (d/entity db group-ent-id)
            straggler-task-a (d/entity db straggler-a)
            straggler-task-b (d/entity db straggler-b)]
        (is (= #{straggler-task-a straggler-task-b} (set (group/find-stragglers group-ent))))))
    (testing "quantile deviation straggler-handling, not enough jobs complete"
      (let [straggler-handling {:straggler-handling/type :straggler-handling.type/quantile-deviation
                                :straggler-handling/parameters
                                {:straggler-handling.quantile-deviation/quantile 0.5
                                 :straggler-handling.quantile-deviation/multiplier 2.0}}
            group-ent-id (create-dummy-group conn :straggler-handling straggler-handling)
            job-a (create-dummy-job conn :group group-ent-id)
            _ (create-dummy-instance conn job-a :instance-status :instance.status/running
                                     :start-time (tc/to-date (t/ago (t/hours 3))))
            job-b (create-dummy-job conn :group group-ent-id)
            straggler (create-dummy-instance conn job-b :instance-status :instance.status/running
                                             :start-time (tc/to-date (t/ago (t/minutes 190))))
            job-c (create-dummy-job conn :group group-ent-id)
            _ (create-dummy-instance conn job-b :instance-status :instance.status/success
                                     :start-time (tc/to-date (t/ago (t/hours 3)))
                                     :end-time (tc/to-date (t/ago (t/hours 2))))
            db (d/db conn)
            group-ent (d/entity db group-ent-id)
            straggler-task (d/entity db straggler)]
        (is (nil? (seq (group/find-stragglers group-ent))))))))
