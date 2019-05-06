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
      (is (= nil (cc/get-mesos-cluster-entity-id (d/db conn) mesos-1)))
      (is (= nil (cc/get-mesos-cluster-entity-id (d/db conn) mesos-2))))

    (testing "Create a cluster. Should be a new cluster"
      (let [{id1a :db-id :as fetch-mesos-1a} (cc/get-mesos-cluster-map conn mesos-1)]
        ; This should create one cluster in the DB, but not the other.
        (is (not= nil (cc/get-mesos-cluster-entity-id (d/db conn) mesos-1)))
        (is (= nil (cc/get-mesos-cluster-entity-id (d/db conn) mesos-2)))
        (let [{id2a :db-id :as fetch-mesos-2a} (cc/get-mesos-cluster-map conn mesos-2)
              {id1b :db-id :as fetch-mesos-1b} (cc/get-mesos-cluster-map conn mesos-1)
              {id2b :db-id :as fetch-mesos-2b} (cc/get-mesos-cluster-map conn mesos-2)]
          ; Should see both clusters created.
          (is (not= nil (cc/get-mesos-cluster-entity-id (d/db conn) mesos-1)))
          (is (not= nil (cc/get-mesos-cluster-entity-id (d/db conn) mesos-2)))
          (is (not= (cc/get-mesos-cluster-entity-id (d/db conn) mesos-1)
                    (cc/get-mesos-cluster-entity-id (d/db conn) mesos-2)))
          ; Now, we should only have two unique db-id's.
          (is (= fetch-mesos-1a fetch-mesos-1b))
          (is (= fetch-mesos-2a fetch-mesos-2b))

          (is (and id1a (< 0 id1a)))
          (is (and id2a (< 0 id2a)))
          (is (and id1b (< 0 id1b)))
          (is (and id2b (< 0 id2b)))

          (is (= (dissoc fetch-mesos-1a :db-id)
                 {:compute-cluster-type :mesos-cluster
                  :compute-cluster-name "mesos-1"
                  :mesos-framework-id "mesos-1a"})))))))

(deftest setup-cluster-map-config
  (let [conn (restore-fresh-database! "datomic:mem://compute-cluster")]
    (testing "Reject with multiple clusters with the same name"
      (with-redefs [cc/get-mesos-clusters-from-config
                    (constantly [{:compute-cluster-name "foo-1"
                                  :framework-id "foo-1a"}
                                 {:compute-cluster-name "foo-2"
                                  :framework-id "foo-1a"}
                                 {:compute-cluster-name "foo-1"
                                  :framework-id "foo-1a"}])]
        (is (thrown-with-msg? IllegalArgumentException #"Multiple" (cc/setup-cluster-map-config conn nil)))))
    (testing "Install the clusters per the configuration read and make sure that they can be found"
      (with-redefs [cc/get-mesos-clusters-from-config
                    (constantly [{:compute-cluster-name "foo-3"
                                  :framework-id "foo-1a"}
                                 {:compute-cluster-name "foo-4"
                                  :framework-id "foo-1a"}])]
        (cc/setup-cluster-map-config conn nil)
        (is (< 0 (cc/cluster-name->db-id "foo-3")))
        (is (< 0 (cc/cluster-name->db-id "foo-4")))
        (is (thrown-with-msg? IllegalStateException #"Was asked to lookup" (cc/cluster-name->db-id "foo-5")))))))
