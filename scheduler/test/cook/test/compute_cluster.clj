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
(ns cook.test.compute-cluster
  (:require [clojure.test :refer :all]
            [cook.mesos.mesos-compute-cluster :as mcc]
            [cook.compute-cluster :as cc]
            [cook.test.testutil :refer [create-dummy-instance
                                        create-dummy-job
                                        create-dummy-job-with-instances
                                        create-pool
                                        flush-caches!
                                        restore-fresh-database!] :as testutil]
            [datomic.api :as d]))


(deftest test-creates-compute-clusters
  (let [conn (restore-fresh-database! "datomic:mem://compute-cluster-factory")
        mesos-1 {:compute-cluster-name "mesos-1" :framework-id "mesos-1a"}
        mesos-2 {:compute-cluster-name "mesos-2" :framework-id "mesos-1a"}]
    (testing "Start with no clusters"
      (is (= nil (mcc/get-mesos-cluster-entity-id (d/db conn) mesos-1)))
      (is (= nil (mcc/get-mesos-cluster-entity-id (d/db conn) mesos-2))))

    (testing "Create a cluster. Should be a new cluster"
      (let [{id1a :db-id :as fetch-mesos-1a} (mcc/get-mesos-compute-cluster conn
                                                                            testutil/create-dummy-mesos-compute-cluster
                                                                            mesos-1)]
        ; This should create one cluster in the DB, but not the other.
        (is (not= nil (mcc/get-mesos-cluster-entity-id (d/db conn) mesos-1)))
        (is (= nil (mcc/get-mesos-cluster-entity-id (d/db conn) mesos-2)))
        (let [{id2a :db-id :as fetch-mesos-2a} (mcc/get-mesos-compute-cluster conn
                                                                              testutil/create-dummy-mesos-compute-cluster
                                                                              mesos-2)
              {id1b :db-id :as fetch-mesos-1b} (mcc/get-mesos-compute-cluster conn
                                                                              testutil/create-dummy-mesos-compute-cluster
                                                                              mesos-1)
              {id2b :db-id :as fetch-mesos-2b} (mcc/get-mesos-compute-cluster conn
                                                                              testutil/create-dummy-mesos-compute-cluster
                                                                              mesos-2)]
          ; Should see both clusters created.
          (is (not= nil (mcc/get-mesos-cluster-entity-id (d/db conn) mesos-1)))
          (is (not= nil (mcc/get-mesos-cluster-entity-id (d/db conn) mesos-2)))
          (is (not= (mcc/get-mesos-cluster-entity-id (d/db conn) mesos-1)
                    (mcc/get-mesos-cluster-entity-id (d/db conn) mesos-2)))
          ; Now, we should only have two unique db-id's.
          (is (= id1a id1b))
          (is (= id2a id2b))

          (is (and id1a (< 0 id1a)))
          (is (and id2a (< 0 id2a)))
          (is (and id1b (< 0 id1b)))
          (is (and id2b (< 0 id2b)))

          (is (= (select-keys fetch-mesos-1a [:compute-cluster-name :framework-id])
                 {:compute-cluster-name "mesos-1"
                  :framework-id "mesos-1a"})))))))

(deftest setup-cluster-map-config
  (let [conn (restore-fresh-database! "datomic:mem://compute-cluster")]
    (testing "Reject with multiple clusters with the same name"
      (with-redefs [mcc/get-mesos-clusters-from-config
                    (constantly [{:compute-cluster-name "foo-1"
                                  :framework-id "foo-1a"}
                                 {:compute-cluster-name "foo-2"
                                  :framework-id "foo-1a"}
                                 {:compute-cluster-name "foo-1"
                                  :framework-id "foo-1a"}])]
        (is (thrown-with-msg? IllegalArgumentException #"Multiple"
                              (mcc/setup-compute-cluster-map-from-config conn nil
                                                                         testutil/create-dummy-mesos-compute-cluster)))))
    (testing "Install the clusters per the configuration read and make sure that they can be found"
      (with-redefs [mcc/get-mesos-clusters-from-config
                    (constantly [{:compute-cluster-name "foo-3"
                                  :framework-id "foo-1a"}
                                 {:compute-cluster-name "foo-4"
                                  :framework-id "foo-1a"}])]
        (mcc/setup-compute-cluster-map-from-config conn nil testutil/create-dummy-mesos-compute-cluster)
        (is (< 0 (-> "foo-3" cc/compute-cluster-name->ComputeCluster cc/db-id)))
        (is (< 0 (-> "foo-4" cc/compute-cluster-name->ComputeCluster cc/db-id)))
        (is (= nil (cc/compute-cluster-name->ComputeCluster "foo-5")))))))
