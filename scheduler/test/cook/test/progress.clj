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

(ns cook.test.progress
  (:require [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clojure.test :refer :all]
            [cook.test.postgres]
            [cook.progress :as progress]
            [cook.test.testutil :refer [create-dummy-instance create-dummy-job poll-until restore-fresh-database!]]
            [datomic.api :as d]
            [plumbing.core :as pc]))

(use-fixtures :once cook.test.postgres/with-pg-db)

(defn- progress-entry
  [message percent sequence & {:keys [instance-id]}]
  (cond-> {:progress-message message
           :progress-percent percent
           :progress-sequence sequence}
    instance-id (assoc :instance-id instance-id)))

(deftest test-progress-aggregator
  (let [pending-threshold 10
        sequence-cache-store-fn (fn [& {:keys [data threshold] :or {data {} threshold 100}}]
                                  (atom (cache/lru-cache-factory data :threshold threshold)))]
    (testing "basic update from initial state"
      (is (= {"i1" (progress-entry "i1.m1" 10 1)}
             (progress/progress-aggregator
               pending-threshold
               (sequence-cache-store-fn)
               {}
               (progress-entry "i1.m1" 10 1 :instance-id "i1")))))

    (testing "update state for known instance"
      (let [sequence-cache-store (sequence-cache-store-fn :data {"i1" 1})]
        (is (= {"i1" (progress-entry "i1.m2" 20 2)}
               (progress/progress-aggregator
                 pending-threshold
                 sequence-cache-store
                 {"i1" (progress-entry "i1.m1" 10 1)}
                 (progress-entry "i1.m2" 20 2 :instance-id "i1"))))
        (is (= {"i1" 2} (.cache @sequence-cache-store)))))

    (testing "skip update state when missing progress-sequence"
      (let [sequence-cache-store (sequence-cache-store-fn :data {"i1" 1})]
        (is (= {"i1" (progress-entry "i1.m1" 10 1)}
               (progress/progress-aggregator
                 pending-threshold
                 sequence-cache-store
                 {"i1" (progress-entry "i1.m1" 10 1)}
                 (progress-entry "i1.m2" 20 nil :instance-id "i1"))))
        (is (= {"i1" 1} (.cache @sequence-cache-store)))))

    (testing "do not update state for outdated message"
      (let [sequence-cache-store (sequence-cache-store-fn :data {"i1" 2})]
        (is (= {"i1" (progress-entry "i1.m2" 20 2)}
               (progress/progress-aggregator
                 pending-threshold
                 sequence-cache-store
                 {"i1" (progress-entry "i1.m2" 20 2)}
                 (progress-entry "i1.m1" 10 1 :instance-id "i1"))))
        (is (= {"i1" 2} (.cache @sequence-cache-store)))))

    (testing "handle threshold exceeded"
      (let [sequence-cache-store (sequence-cache-store-fn :data {"i1" 1})]
        (is (= {"i1" (progress-entry "i1.m1" 10 1)}
               (progress/progress-aggregator
                 1
                 sequence-cache-store
                 {"i1" (progress-entry "i1.m1" 10 1)}
                 (progress-entry "i2.m2" 20 2 :instance-id "i2"))))
        (is (= {"i1" 1} (.cache @sequence-cache-store)))))

    (testing "handle threshold limit reached"
      (let [sequence-cache-store (sequence-cache-store-fn :data {"i1" 1})]
        (is (= {"i1" (progress-entry "i1.m1" 10 1)
                "i2" (progress-entry "i2.m2" 20 2)}
               (progress/progress-aggregator
                 2
                 sequence-cache-store
                 {"i1" (progress-entry "i1.m1" 10 1)}
                 (progress-entry "i2.m2" 20 2 :instance-id "i2"))))
        (is (= {"i1" 1, "i2" 2} (.cache @sequence-cache-store)))))))

(deftest test-progress-update-aggregator
  (let [progress-config {:pending-threshold 10
                         :publish-interval-ms 10000
                         :sequence-cache-threshold 10}
        actual-progress-aggregator progress/progress-aggregator
        redef-progress-aggregator (fn redef-progress-aggregator
                                    [pending-threshold sequence-cache-store instance-id->progress-state {:keys [response-chan] :as data}]
                                    (let [result (actual-progress-aggregator pending-threshold sequence-cache-store instance-id->progress-state data)]
                                      (when response-chan
                                        (async/close! response-chan))
                                      result))]
    (with-redefs [progress/progress-aggregator redef-progress-aggregator]
      (letfn [(send [progress-aggregator-chan message & {:keys [sync] :or {sync false}}]
                (let [message (cond-> message
                                sync (assoc :response-chan (async/promise-chan)))
                      put-result (async/>!! progress-aggregator-chan message)]
                  (when sync
                    (or (not put-result)
                        (-> message :response-chan async/<!!)))))]

        (testing "no progress to publish"
          (let [progress-state-chan (async/chan 1)
                progress-aggregator-chan
                (progress/progress-update-aggregator progress-config progress-state-chan)]

            (is (= {} (async/<!! progress-state-chan)))
            (is (= {} (async/<!! progress-state-chan)))

            (async/close! progress-state-chan)
            (async/close! progress-aggregator-chan)
            (poll-until #(nil? (async/<!! progress-state-chan)) 100 1000)))

        (testing "basic progress publishing"
          (let [progress-state-chan (async/chan)
                progress-aggregator-chan
                (progress/progress-update-aggregator progress-config progress-state-chan)]
            (send progress-aggregator-chan (progress-entry "i1.m1" 10 1 :instance-id "i1"))
            (send progress-aggregator-chan (progress-entry "i2.m1" 10 1 :instance-id "i2"))
            (send progress-aggregator-chan (progress-entry "i3.m1" 10 1 :instance-id "i3"))
            (send progress-aggregator-chan (progress-entry "i2.m2" 25 2 :instance-id "i2"))
            (send progress-aggregator-chan (progress-entry "i1.m2" 45 2 :instance-id "i1") :sync true)

            (is (= {"i1" (progress-entry "i1.m2" 45 2)
                    "i2" (progress-entry "i2.m2" 25 2)
                    "i3" (progress-entry "i3.m1" 10 1)}
                   (async/<!! progress-state-chan)))
            (is (= {} (async/<!! progress-state-chan)))

            (async/close! progress-state-chan)
            (async/close! progress-aggregator-chan)
            (poll-until #(nil? (async/<!! progress-state-chan)) 100 1000)))

        (testing "basic progress overflow on channel"
          (let [progress-aggregator-counter (atom 0)]
            (with-redefs [progress/progress-aggregator
                          (fn progress-aggregator
                            [pending-threshold sequence-cache-store instance-id->progress-state {:keys [response-chan] :as data}]
                            (swap! progress-aggregator-counter inc)
                            (Thread/sleep 100) ;; delay processing of message to cause queue to build up
                            (redef-progress-aggregator pending-threshold sequence-cache-store instance-id->progress-state data))]

              (let [progress-state-chan (async/chan)
                    progress-aggregator-chan
                    (progress/progress-update-aggregator progress-config progress-state-chan)]
                (dotimes [n 100]
                  (send progress-aggregator-chan (progress-entry (str "i1.m" n) n n :instance-id "i1")))
                (send progress-aggregator-chan (progress-entry "i1.m100" 100 100 :instance-id "i1") :sync true)

                (is (< @progress-aggregator-counter 100))
                (is (= {"i1" (progress-entry "i1.m100" 100 100)}
                       (async/<!! progress-state-chan)))
                (is (= {} (async/<!! progress-state-chan)))

                (async/close! progress-state-chan)
                (async/close! progress-aggregator-chan)
                (poll-until #(nil? (async/<!! progress-state-chan)) 100 1000)))))))))

(deftest test-progress-update-transactor
  (let [uri "datomic:mem://test-progress-update-transactor"
        conn (restore-fresh-database! uri)]
    (testing "update-progress single instance"
      (let [j1 (create-dummy-job conn :user "user1" :ncpus 1.0 :memory 3.0 :job-state :job.state/running)
            i1 (create-dummy-instance conn j1)
            publish-progress-trigger-chan (async/chan)
            batch-size 2
            {:keys [cancel-handle progress-state-chan]} (progress/progress-update-transactor publish-progress-trigger-chan batch-size conn)
            response-chan (async/promise-chan)]
        ;; publish the data
        (async/>!! publish-progress-trigger-chan response-chan)
        (async/>!! progress-state-chan {i1 (progress-entry "i1.m1" 10 1)})
        ;; force db transactions
        (async/<!! response-chan)
        ;; assert the state of the db
        (let [test-db (d/db conn)
              {:keys [:instance/progress :instance/progress-message] :as instance}
              (->> i1 (d/entity test-db) d/touch)]
          (is instance)
          (is (= 10 progress))
          (is (= "i1.m1" progress-message)))

        (cancel-handle)
        (async/close! progress-state-chan)))

    (testing "update-progress multiple instances"
      (let [num-jobs-or-instances 1500
            all-jobs (map (fn [index] (create-dummy-job conn :user (str "user" index) :ncpus 1.0 :memory 3.0 :job-state :job.state/running))
                          (range num-jobs-or-instances))
            all-instance-ids (map (fn [job] (create-dummy-instance conn job))
                                  all-jobs)
            publish-progress-trigger-chan (async/chan)
            batch-size 100
            {:keys [cancel-handle progress-state-chan]} (progress/progress-update-transactor publish-progress-trigger-chan batch-size conn)
            instance-id->progress-state (pc/map-from-keys (fn [instance-id]
                                                            (let [progress (rand-int 100)]
                                                              {:progress-message (str instance-id ".m" progress)
                                                               :progress-percent progress}))
                                                          all-instance-ids)
            response-chan (async/promise-chan)]
        ;; publish the data
        (async/>!! publish-progress-trigger-chan response-chan)
        (async/>!! progress-state-chan instance-id->progress-state)
        ;; force db transactions
        (async/<!! response-chan)
        ;; assert the state of the db
        (let [test-db (d/db conn)
              actual-instance-id->progress-state (pc/map-from-keys (fn [instance-id]
                                                                     (let [{:keys [:instance/progress :instance/progress-message] :as instance}
                                                                           (->> instance-id (d/entity test-db) d/touch)]
                                                                       {:progress-message progress-message
                                                                        :progress-percent progress}))
                                                                   all-instance-ids)]
          (is (= instance-id->progress-state actual-instance-id->progress-state)))

        (cancel-handle)
        (async/close! progress-state-chan)))

    (testing "update-progress publish latest data"
      (let [num-jobs-or-instances 100
            all-jobs (map (fn [index] (create-dummy-job conn :user (str "user" index) :ncpus 1.0 :memory 3.0 :job-state :job.state/running))
                          (range num-jobs-or-instances))
            all-instance-ids (map (fn [job] (create-dummy-instance conn job))
                                  all-jobs)
            publish-progress-trigger-chan (async/chan)
            batch-size 10
            {:keys [cancel-handle progress-state-chan]} (progress/progress-update-transactor publish-progress-trigger-chan batch-size conn)
            instance-id->progress-state (pc/map-from-keys (fn [instance-id]
                                                            (let [progress (rand-int 100)]
                                                              {:progress-message (str instance-id ".m" progress)
                                                               :progress-percent progress}))
                                                          all-instance-ids)
            response-chan-1 (async/promise-chan)
            response-chan-2 (async/promise-chan)]
        ;; generate half the data
        (async/>!! publish-progress-trigger-chan response-chan-1)
        (let [n (int (/ num-jobs-or-instances 2))
              instance-id->progress-state' (into {} (take n instance-id->progress-state))]
          (async/>!! progress-state-chan instance-id->progress-state'))
        ;; force a publish
        (async/<!! response-chan-1)
        ;; generate rest of the data
        (async/>!! publish-progress-trigger-chan response-chan-2)
        (let [n num-jobs-or-instances
              instance-id->progress-state' (into {} (take n instance-id->progress-state))]
          (async/>!! progress-state-chan instance-id->progress-state'))
        ;; force a publish
        (async/<!! response-chan-2)
        ;; assert the state of the db
        (let [test-db (d/db conn)
              actual-instance-id->progress-state (pc/map-from-keys (fn [instance-id]
                                                                     (let [{:keys [:instance/progress :instance/progress-message] :as instance}
                                                                           (->> instance-id (d/entity test-db) d/touch)]
                                                                       {:progress-message progress-message
                                                                        :progress-percent progress}))
                                                                   all-instance-ids)]
          (is (= instance-id->progress-state actual-instance-id->progress-state)))

        (cancel-handle)
        (async/close! progress-state-chan)))))


