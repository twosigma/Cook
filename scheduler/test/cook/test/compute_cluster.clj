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
            [cook.compute-cluster :refer :all]
            [cook.config :as config]
            [cook.test.testutil :refer [create-dummy-job-with-instances restore-fresh-database!]]
            [datomic.api :as d]))

(deftest test-get-job-instance-ids-for-cluster-name
  (let [uri "datomic:mem://test-compute-cluster-config"
        conn (restore-fresh-database! uri)
        name "cluster1"
        cluster-db-id (write-compute-cluster conn {:compute-cluster/cluster-name name})
        make-instance (fn [status]
                        (let [[_ [inst]] (create-dummy-job-with-instances
                                           conn
                                           :job-state :job.state/running
                                           :instances [{:instance-status status
                                                        :compute-cluster (reify ComputeCluster
                                                                           (db-id [_] cluster-db-id)
                                                                           (compute-cluster-name [_] name))}])]
                          inst))]
    (let [_ (make-instance :instance.status/success)
          db (d/db conn)]
      (is (= [] (get-job-instance-ids-for-cluster-name db name))))
    (let [inst (make-instance :instance.status/running)
          db (d/db conn)]
      (is (= [inst] (get-job-instance-ids-for-cluster-name db name))))))

(deftest test-cluster-state-change-valid?
  (with-redefs [get-job-instance-ids-for-cluster-name
                (fn [_ _] [])]
    (let [test-fn (fn [current-state new-state] (cluster-state-change-valid? nil current-state new-state nil))]
      (is (= false (test-fn :running :invalid)))
      (is (= false (test-fn :invalid :running)))
      (is (= true (test-fn :running :running)))
      (is (= true (test-fn :running :draining)))
      (is (= false (test-fn :running :deleted)))
      (is (= true (test-fn :draining :running)))
      (is (= true (test-fn :draining :draining)))
      (is (= true (test-fn :draining :deleted)))
      (is (= false (test-fn :deleted :running)))
      (is (= false (test-fn :deleted :draining)))
      (is (= true (test-fn :deleted :deleted)))))
  (with-redefs [get-job-instance-ids-for-cluster-name
                (fn [_ _] [1])]
    (let [test-fn (fn [current-state new-state] (cluster-state-change-valid? nil current-state new-state nil))]
      (is (= false (test-fn :running :invalid)))
      (is (= false (test-fn :invalid :running)))
      (is (= true (test-fn :running :running)))
      (is (= true (test-fn :running :draining)))
      (is (= false (test-fn :running :deleted)))
      (is (= true (test-fn :draining :running)))
      (is (= true (test-fn :draining :draining)))
      (is (= false (test-fn :draining :deleted)))
      (is (= false (test-fn :deleted :running)))
      (is (= false (test-fn :deleted :draining)))
      (is (= true (test-fn :deleted :deleted))))))

(deftest test-compute-dynamic-config-update
  (let [state-change-valid-atom (atom true)]
    (with-redefs [cluster-state-change-valid? (fn [db current-state new-state cluster-name] @state-change-valid-atom)]
      (testing "invalid state change"
        (reset! state-change-valid-atom false)
        (is (= {:changed? false
                :error true
                :reason "Cluster state transition from  to  is not valid."
                :update-value {}
                :valid? false} (compute-dynamic-config-update nil {} {} false)))
        (reset! state-change-valid-atom true))
      (testing "locked state"
        (is (= {:changed? true
                :error false
                :reason "Attempting to change cluster state from :running to :draining but not able because it is locked."
                :update-value {:state :draining}
                :valid? false} (compute-dynamic-config-update nil {:state-locked? true :state :running} {:state :draining} false))))
      (testing "non-state change"
        (is (= {:changed? true
                :error true
                :reason "Attempting to change something other than state when force? is false. Diff is ({:a :a} {:a :b} nil)"
                :update-value {:a :b}
                :valid? false} (compute-dynamic-config-update nil {:a :a} {:a :b} false))))
      (testing "locked state - forced"
        (is (= {:changed? true
                :update-value {:state :draining}
                :valid? true} (compute-dynamic-config-update nil {:state-locked? true :state :running} {:state :draining} true))))
      (testing "non-state change - forced"
        (is (= {:changed? true
                :update-value {:a :b}
                :valid? true} (compute-dynamic-config-update nil {:a :a} {:a :b} true))))
      (testing "valid changed"
        (is (= {:changed? true
                :update-value {:a :a :state :draining}
                :valid? true} (compute-dynamic-config-update nil {:a :a :state :running} {:a :a :state :draining} false)))
        (is (= {:changed? true
                :update-value {:a :b}
                :valid? true} (compute-dynamic-config-update nil {:a :a} {:a :b} true))))
      (testing "valid unchanged"
        (is (= {:changed? false
                :update-value {:a :a}
                :valid? true} (compute-dynamic-config-update nil {:a :a} {:a :a} false)))
        (is (= {:changed? false
                :update-value {:a :a}
                :valid? true} (compute-dynamic-config-update nil {:a :a} {:a :a} true)))))))

(deftest test-compute-dynamic-config-insert
  (with-redefs [config/compute-cluster-templates (constantly {"template1" {:a :bb :c :dd}
                                                              "template2" {:a :bb :c :dd :factory-fn :factory-fn}})]
    (testing "bad template"
      (is (= {:changed? true
              :error true
              :insert-value {:a :b}
              :reason "Attempting to create cluster with unknown template: "
              :valid? false}
             (compute-dynamic-config-insert {:a :b})))
      (is (= {:changed? true
              :error true
              :insert-value {:a :b
                             :template "missing"}
              :reason "Attempting to create cluster with unknown template: missing"
              :valid? false}
             (compute-dynamic-config-insert {:a :b :template "missing"}))))
    (testing "bad template"
      (is (= {:changed? true
              :error true
              :insert-value {:a :b
                             :template "template1"}
              :reason "Template for cluster has no factory-fn: {:a :bb, :c :dd}"
              :valid? false}
             (compute-dynamic-config-insert {:a :b :template "template1"}))))
    (testing "good template"
      (is (= {:changed? true
              :insert-value {:a :b
                             :template "template2"}
              :valid? true}
             (compute-dynamic-config-insert {:a :b :template "template2"}))))))


(deftest test-compute-dynamic-config-updates
  (with-redefs [compute-dynamic-config-update (fn [_ current new _] {:changed? (not= current new)
                                                                     :update-value new
                                                                     :valid? true})
                compute-dynamic-config-insert (fn [new] {:changed? true
                                                         :insert-value new
                                                         :valid? true})]
    (is (= (set [{:changed? true
                  :update-value {:a :a
                                 :name :left
                                 :state :deleted}
                  :valid? true}
                 {:changed? true
                  :update-value {:a :b
                                 :name :both2}
                  :valid? true}
                 {:changed? true
                  :insert-value {:a :a
                                 :name :right}
                  :valid? true}])
           (set (compute-dynamic-config-updates
                  nil
                  {:left {:name :left
                          :a :a}
                   :both1 {:name :both1
                           :a :a}
                   :both2 {:name :both2
                           :a :a}}
                  {:both1 {:name :both1
                           :a :a}
                   :both2 {:name :both2
                           :a :b}
                   :right {:name :right
                           :a :a}}
                  nil))))))

(deftest test-add-config
  (let [uri "datomic:mem://test-compute-cluster-config"
        conn (restore-fresh-database! uri)
        db-id (d/tempid :db.part/user)
        new-config [[:compute-cluster-config/name "cluster-1"]
                    [:compute-cluster-config/template "cluster-1-template"]
                    [:compute-cluster-config/base-path "https://35.224.254.75"]
                    [:compute-cluster-config/ca-cert "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURERENDQWZTZ0F3SUJBZ0lSQUtYaGhOVjlKdHRoYm9QYm1IVlFDMmd3RFFZSktvWklodmNOQVFFTEJRQXcKTHpFdE1Dc0dBMVVFQXhNa01UVTVPR1V5T0RBdE1EUTVPUzAwWWpSa0xUaGhNekV0TmpjM1pHRmlNRFJpTm1NMQpNQjRYRFRJd01EZ3hNREU1TVRJd01Wb1hEVEkxTURnd09USXdNVEl3TVZvd0x6RXRNQ3NHQTFVRUF4TWtNVFU1Ck9HVXlPREF0TURRNU9TMDBZalJrTFRoaE16RXROamMzWkdGaU1EUmlObU0xTUlJQklqQU5CZ2txaGtpRzl3MEIKQVFFRkFBT0NBUThBTUlJQkNnS0NBUUVBNHFLUXplRVNmN2kwMjBKcGhVeXdCN1pUTHVXRkFIMEk4VCtMWnRGMQp1TEwzMDdqMVJQN1lYeWxjcURBSm5TM0FiaEpTLytsMFRySUZGRkpnbEN5bGdIQjJSczV0Y2FjdlYrWmpQNkZXCmFSNExOTkNEZWJvM1JBRE5RM2ZPWGpIV3JZdnVFbTBKS1JsSk9QU1Q1Zmw1UnBXNERkK1djQ1FaclFoOWpMaG0KUExyUDBaMjlyTmI0RDBpeEk3enkwb09aUFVJd3k0N1A3eVVoYVE1TkszUWNtL0ZueWJlc1FxWXFpN0w5SGtCTgpsd0xxSjlVL3dIZDlGSG4zM0ZFUy8rZGhuV3pSa3pzejJrQlNYbHdybWxqMkFKbGNSRjlDdEFlNjU1RHJ1YlMrCk5ibENlNzUrdmxWcVl2RkNOcTNFUUc5Qlk1b1lwVDc4b3BIVVRlcER2V2NldVFJREFRQUJveU13SVRBT0JnTlYKSFE4QkFmOEVCQU1DQWdRd0R3WURWUjBUQVFIL0JBVXdBd0VCL3pBTkJna3Foa2lHOXcwQkFRc0ZBQU9DQVFFQQpMNmNKUXRTUngwTDUxeVVra0FFQTQzK05QbWxWMFRvTUJCa3BOVHBveVRDU3l4N2pmLzMrVGs3cWUyYlk2ck1vCkJMVFBpeFYxaVZsRXIwL05yY25wQjhrU090YjJYY2p4UjBUdWpWd1JGUmN1STc0eFFRcndMVkUvZjdmNURlOUsKcWNtOHBYcWxFblYwM1JrQUdDbGZLUzBmSDJSTjRxbHRaLzJjL21MRkJTbmFZWWEreVUwMUFJSjNOYm5CQld2WQpqTFlMd28yQ3Bvdm5zQnV6cmkxZi9VZWRpUTBIUTFuL2ttUlVZbjIxU0JNU1NqcCsxTExKdDVBd0NEb01oYWw0CjIvOGRqOVR0NytrVE1wK0VjSUhTVWNzT2JobVA5cmRXS3ZobGtXaEZQOTFzb1V4MVJrWDMxdWFpZDFhcUN1cHMKREdoeUl3elJKWVJZdjJBQ2c0d2tIQT09Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K"]
                    ]
        _ (println (into [] (map #(into [] (concat [:db/add db-id] %)) new-config)))
        xx @(d/transact conn (into [] (map #(into [] (concat [:db/add db-id] %)) new-config)))
        db-id (d/tempid :db.part/user)]
    (println xx)


    (println @(d/transact conn [{:db/id (d/tempid :db.part/user) :compute-cluster-config/name "cluster-2" :compute-cluster-config/template "cluster-2-template"}]))
    (let [db (d/db conn)
          current-configs (map #(let [e (d/entity db %)]
                                  {:name (:compute-cluster-config/name e)
                                   :template (:compute-cluster-config/template e)})
                               (d/q '[:find [?compute-cluster-config ...]
                                      :where
                                      [?compute-cluster-config :compute-cluster-config/name ?name]]
                                    db))
          ]
      (println current-configs)
      )

    (let [db (d/db conn)
          zzz (d/q '[:find [?compute-cluster-config ...]
                     :in $ ?cluster-name
                     :where
                     [?compute-cluster-config :compute-cluster-config/name ?cluster-name]]
                   db "cluster-2")]

      (println zzz)

      (println @(d/transact conn [{:db/id (first zzz) :compute-cluster-config/name "cluster-2" :compute-cluster-config/template "cxxxluster-2-template"}]))
      (let [db (d/db conn)
            current-configs (map #(let [e (d/entity db %)]
                                    {:name (:compute-cluster-config/name e)
                                     :template (:compute-cluster-config/template e)
                                     :xxx (:db/id e)})
                                 (d/q '[:find [?compute-cluster-config ...]
                                        :where
                                        [?compute-cluster-config :compute-cluster-config/name ?name]]
                                      db))
            ]
        (println current-configs)
        ))

    ))