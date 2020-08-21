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
            [datomic.api :as d]
            [plumbing.core :refer [map-vals map-from-vals]])
  (:import (clojure.lang ExceptionInfo)))

(deftest test-diff-map-keys
  (is (= [#{:b} #{:c} #{:a :d}]
         (diff-map-keys {:a {:a :a}
                         :b {:b :b}
                         :d {:d :d}}
                        {:a {:a :a}
                         :c {:c :c}
                         :d {:d :e}}))))

(deftest test-datomic-entity-conversion
  (let [config {:name "name"
                :template "template"
                :base-path "base-path"
                :ca-cert "ca-cert"
                :state :running
                :state-locked? true}
        config-ent (compute-cluster-config->compute-cluster-config-ent config)
        _ (is (= {:compute-cluster-config/name "name"
                  :compute-cluster-config/base-path "base-path"
                  :compute-cluster-config/ca-cert "ca-cert"
                  :compute-cluster-config/state :compute-cluster-config.state/running
                  :compute-cluster-config/state-locked? true
                  :compute-cluster-config/template "template"} config-ent))]
    (is (= config (compute-cluster-config-ent->compute-cluster-config config-ent)))))

(deftest test-db-config-ents
  (let [uri "datomic:mem://test-compute-cluster-config"
        conn (restore-fresh-database! uri)
        temp-db-id (d/tempid :db.part/user)
        ent (compute-cluster-config->compute-cluster-config-ent
              {:name "name"
               :template "template"
               :base-path "base-path"
               :ca-cert "ca-cert"
               :state :running
               :state-locked? true})
        _ (is (= {} (db-config-ents (d/db conn))))
        tempids (-> @(d/transact conn [(assoc ent :db/id temp-db-id)]) :tempids)
        db (d/db conn)]
    (is (= {"name" {:db/id (d/resolve-tempid db tempids temp-db-id)
                    :compute-cluster-config/name "name"
                    :compute-cluster-config/base-path "base-path"
                    :compute-cluster-config/ca-cert "ca-cert"
                    :compute-cluster-config/state :compute-cluster-config.state/running
                    :compute-cluster-config/state-locked? true
                    :compute-cluster-config/template "template"}}
           (map-vals
             #(select-keys % [:db/id
                              :compute-cluster-config/name
                              :compute-cluster-config/base-path
                              :compute-cluster-config/ca-cert
                              :compute-cluster-config/state
                              :compute-cluster-config/state-locked?
                              :compute-cluster-config/template])
             (db-config-ents (d/db conn)))))))

(deftest test-compute-cluster->compute-cluster-config
  (let [cluster {:name "name"
                 :compute-cluster-starting-config {:factory-fn 'cook.kubernetes.compute-cluster/factory-fn
                                                   :name "name-starting"
                                                   :template "template"
                                                   :base-path "base-path"
                                                   :ca-cert "ca-cert"
                                                   :state :running
                                                   :state-locked? false}
                 :state-atom (atom :deleted)
                 :state-locked?-atom (atom true)}]
    (is (= {:base-path "base-path"
            :ca-cert "ca-cert"
            :name "name"
            :state :deleted
            :state-locked? true
            :template "template"}
           (compute-cluster->compute-cluster-config cluster)))))
(def expected-in-mem-config
  {"name" {:base-path "base-path"
           :ca-cert "ca-cert"
           :name "name"
           :state :deleted
           :state-locked? true
           :template "template"}})
(def sample-clusters
  {"name" {:name "name"
           :compute-cluster-starting-config {:factory-fn 'cook.kubernetes.compute-cluster/factory-fn
                                             :name "name"
                                             :template "template"
                                             :base-path "base-path"
                                             :ca-cert "ca-cert"
                                             :state :running
                                             :state-locked? false}
           :state-atom (atom :deleted)
           :state-locked?-atom (atom true)}
   "name-no-state" {:name "name"
                    :compute-cluster-starting-config {:factory-fn 'cook.kubernetes.compute-cluster/factory-fn
                                                      :name "name"
                                                      :template "template"
                                                      :base-path "base-path"
                                                      :ca-cert "ca-cert"
                                                      :state :running
                                                      :state-locked? false}}})
(deftest test-in-mem-configs
  (reset! cluster-name->compute-cluster-atom {})
  (is (= {} (in-mem-configs)))
  (reset! cluster-name->compute-cluster-atom sample-clusters)
  (is (= expected-in-mem-config (in-mem-configs))))

(deftest test-get-dynamic-clusters
  (reset! cluster-name->compute-cluster-atom {})
  (is (= {} (in-mem-configs)))
  (reset! cluster-name->compute-cluster-atom sample-clusters)
  (is (= (->> [(sample-clusters "name")] (map-from-vals #(-> % :name)))
         (get-dynamic-clusters))))

(deftest test-compute-current-configs
  (is (= {:a {:a :a}
          :b {:b :b}
          :c {:c :c}
          :d {:d :d}}
         (compute-current-configs
           {:a {:a :a}
            :b {:b :b}
            :d {:d :d}}
           {:a {:a :a}
            :c {:c :c}
            :d {:d :e}})))
  (is (= {:a {:a :a}
          :c {:c :c}}
         (compute-current-configs
           {}
           {:a {:a :a}
            :c {:c :c}})))
  (is (= {} (compute-current-configs {} {})))
  (is (= {"name" {:base-path "base-path"
                  :ca-cert "ca-cert"
                  :name "name"
                  :state :deleted
                  :state-locked? true
                  :template "template"}} (compute-current-configs {} expected-in-mem-config))))

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
        (is (= {:cluster-name nil
                :changed? false
                :reason "Cluster state transition from  to  is not valid."
                :goal-config {}
                :valid? false} (compute-config-update nil {} {} false)))
        (reset! state-change-valid-atom true))
      (testing "locked state"
        (is (= {:cluster-name "name"
                :changed? true
                :reason "Attempting to change cluster state from :running to :draining but not able because it is locked."
                :goal-config {:name "name" :state :draining}
                :valid? false}
               (compute-config-update nil {:name "name" :state-locked? true :state :running} {:name "name" :state :draining} false))))
      (testing "non-state change"
        (is (= {:cluster-name "name"
                :changed? true
                :reason "Attempting to change something other than state when force? is false. Diff is ({:a :a} {:a :b} {:name \"name\"})"
                :goal-config {:name "name" :a :b}
                :valid? false} (compute-config-update nil {:name "name" :a :a} {:name "name" :a :b} false))))
      (testing "locked state - forced"
        (is (= {:cluster-name "name"
                :changed? true
                :goal-config {:name "name" :state :draining}
                :valid? true} (compute-config-update nil {:name "name" :state-locked? true :state :running} {:name "name" :state :draining} true))))
      (testing "non-state change - forced"
        (is (= {:cluster-name "name"
                :changed? true
                :goal-config {:name "name" :a :b}
                :valid? true} (compute-config-update nil {:name "name" :a :a} {:name "name" :a :b} true))))
      (testing "valid changed"
        (is (= {:cluster-name "name"
                :changed? true
                :goal-config {:name "name" :a :a :state :draining}
                :valid? true} (compute-config-update nil {:name "name" :a :a :state :running} {:name "name" :a :a :state :draining} false)))
        (is (= {:cluster-name "name"
                :changed? true
                :goal-config {:name "name" :a :b}
                :valid? true} (compute-config-update nil {:name "name" :a :a} {:name "name" :a :b} true))))
      (testing "valid unchanged"
        (is (= {:cluster-name "name"
                :changed? false
                :goal-config {:name "name" :a :a}
                :valid? true} (compute-config-update nil {:name "name" :a :a} {:name "name" :a :a} false)))
        (is (= {:cluster-name "name"
                :changed? false
                :goal-config {:name "name" :a :a}
                :valid? true} (compute-config-update nil {:name "name" :a :a} {:name "name" :a :a} true)))))))

(deftest test-compute-dynamic-config-insert
  (with-redefs [config/compute-cluster-templates (constantly {"template1" {:a :bb :c :dd}
                                                              "template2" {:a :bb :c :dd :factory-fn :factory-fn}})]
    (testing "bad template"
      (is (= {:cluster-name "name"
              :changed? true
              :goal-config {:a :b
                            :name "name"}
              :reason "Attempting to create cluster with unknown template: "
              :valid? false}
             (compute-config-insert {:name "name" :a :b})))
      (is (= {:cluster-name "name"
              :changed? true
              :goal-config {:a :b
                            :name "name"
                            :template "missing"}
              :reason "Attempting to create cluster with unknown template: missing"
              :valid? false}
             (compute-config-insert {:name "name" :a :b :template "missing"}))))
    (testing "bad template"
      (is (= {:cluster-name "name"
              :changed? true
              :goal-config {:a :b
                            :name "name"
                            :template "template1"}
              :reason "Template for cluster has no factory-fn: {:a :bb, :c :dd}"
              :valid? false}
             (compute-config-insert {:name "name" :a :b :template "template1"}))))
    (testing "good template"
      (is (= {:cluster-name "name"
              :changed? true
              :goal-config {:a :b
                            :name "name"
                            :template "template2"}
              :valid? true}
             (compute-config-insert {:name "name" :a :b :template "template2"}))))))


(deftest test-compute-dynamic-config-updates
  (with-redefs [compute-config-update (fn [_ current new _] {:changed? (not= current new)
                                                             :update? true
                                                             :goal-config new
                                                             :valid? true
                                                             :cluster-name (:name new)})
                compute-config-insert (fn [new] {:changed? true
                                                 :insert? true
                                                 :goal-config new
                                                 :valid? true
                                                 :cluster-name (:name new)})]
    (let [expected (set [
                         {:active? false
                          :changed? false
                          :cluster-name "base-path-collide-existing"
                          :goal-config {:base-path "path-collide"
                                        :ca-cert 7
                                        :name "base-path-collide-existing"
                                        :state :deleted}
                          :update? true
                          :valid? true}
                         {:active? false
                          :changed? true
                          :cluster-name "left"
                          :goal-config {:a :a
                                        :base-path "left-base-path"
                                        :ca-cert 1
                                        :name "left"
                                        :state :deleted}
                          :update? true
                          :valid? true}
                         {:active? true
                          :changed? false
                          :cluster-name "both1"
                          :goal-config {:a :a
                                        :base-path "both1-base-path"
                                        :ca-cert 2
                                        :name "both1"}
                          :update? true
                          :valid? true}
                         {:active? true
                          :changed? false
                          :cluster-name "both5"
                          :goal-config {:a :b
                                        :base-path "both5-base-path"
                                        :ca-cert 6
                                        :name "both5"}
                          :reason ":base-path is not unique between clusters #{\"both5\" \"right2\"}"
                          :update? true
                          :valid? false}
                         {:active? false
                          :changed? true
                          :update? true
                          :cluster-name "left"
                          :goal-config {:a :a
                                        :name "left"
                                        :ca-cert 1
                                        :base-path "left-base-path"
                                        :state :deleted}
                          :valid? true}
                         {:active? true
                          :changed? true
                          :update? true
                          :cluster-name "both2"
                          :goal-config {:a :b
                                        :ca-cert 3
                                        :base-path "both2-base-path"
                                        :name "both2"}
                          :valid? true}
                         {:active? true
                          :changed? true
                          :insert? true
                          :cluster-name "right"
                          :goal-config {:a :a
                                        :ca-cert 12
                                        :base-path "right-base-path"
                                        :name "right"}
                          :valid? true}
                         {:active? true
                          :changed? true
                          :cluster-name "both3"
                          :goal-config {:a :b
                                        :ca-cert 4
                                        :base-path "both4-base-path"
                                        :name "both3"}
                          :update? true
                          :valid? true}
                         {:active? true
                          :changed? true
                          :cluster-name "both4"
                          :goal-config {:a :b
                                        :ca-cert 5
                                        :base-path "both3-base-path"
                                        :name "both4"}
                          :update? true
                          :valid? true}
                         {:active? true
                          :changed? true
                          :cluster-name "right2"
                          :insert? true
                          :goal-config {:a :a
                                        :ca-cert 13
                                        :base-path "both5-base-path"
                                        :name "right2"}
                          :reason ":base-path is not unique between clusters #{\"both5\" \"right2\"}"
                          :valid? false}
                         {:active? true
                          :changed? true
                          :cluster-name "base-path-collide-new"
                          :insert? true
                          :goal-config {:base-path "path-collide"
                                        :ca-cert 7
                                        :name "base-path-collide-new"}
                          :valid? true}
                         ])
          actual (set (compute-config-updates
                        nil
                        {"left" {:name "left"
                                 :a :a
                                 :ca-cert 1
                                 :base-path "left-base-path"}
                         "both1" {:name "both1"
                                  :a :a
                                  :ca-cert 2
                                  :base-path "both1-base-path"}
                         "both2" {:name "both2"
                                  :a :a
                                  :ca-cert 3
                                  :base-path "both2-base-path"}
                         "both3" {:name "both3"
                                  :a :b
                                  :ca-cert 4
                                  :base-path "both3-base-path"}
                         "both4" {:name "both4"
                                  :a :b
                                  :ca-cert 5
                                  :base-path "both4-base-path"}
                         "both5" {:name "both5"
                                  :a :b
                                  :ca-cert 6
                                  :base-path "both5-base-path"}

                         "base-path-collide-existing"
                         {:name "base-path-collide-existing"
                          :ca-cert 7
                          :state :deleted
                          :base-path "path-collide"}

                         }
                        {"both1" {:name "both1"
                                  :a :a
                                  :ca-cert 2
                                  :base-path "both1-base-path"}
                         "both2" {:name "both2"
                                  :a :b
                                  :ca-cert 3
                                  :base-path "both2-base-path"}
                         "both3" {:name "both3"
                                  :a :b
                                  :ca-cert 4
                                  :base-path "both4-base-path"}
                         "both4" {:name "both4"
                                  :a :b
                                  :ca-cert 5
                                  :base-path "both3-base-path"}
                         "both5" {:name "both5"
                                  :a :b
                                  :ca-cert 6
                                  :base-path "both5-base-path"}
                         "right" {:name "right"
                                  :a :a
                                  :ca-cert 12
                                  :base-path "right-base-path"}
                         "right2" {:name "right2"
                                   :a :a
                                   :ca-cert 13
                                   :base-path "both5-base-path"}

                         "base-path-collide-existing"
                         {:name "base-path-collide-existing"
                          :ca-cert 7
                          :state :deleted
                          :base-path "path-collide"}
                         "base-path-collide-new"
                         {:name "base-path-collide-new"
                          :ca-cert 7
                          :base-path "path-collide"}

                         }
                        nil))]
      (is (= expected actual)))))

(def initialize-cluster-fn-invocations-atom (atom []))

(defn cluster-factory-fn
  [{:keys [name
           state
           state-locked?]
    :as compute-cluster-config} _]
  (when (= "fail" (:name compute-cluster-config)) (throw (ex-info "fail" {})))
  (let [backing-map {:name name
                     :state-atom (atom state)
                     :state-locked?-atom (atom state-locked?)
                     :compute-cluster-starting-config (assoc compute-cluster-config :factory-fn 'cook.kubernetes.compute-cluster/factory-fn)}
        compute-cluster (reify ComputeCluster
                          (compute-cluster-name [cluster] (:name cluster))
                          (initialize-cluster [cluster _]
                            (swap! initialize-cluster-fn-invocations-atom conj (:name cluster)))
                          java.util.Map
                          (get [_ val] (backing-map val))
                          clojure.lang.IFn
                          (invoke [_ val] (backing-map val)))]
    (register-compute-cluster! compute-cluster)
    compute-cluster))

(deftest test-initialize-cluster!
  (with-redefs [config/compute-cluster-templates
                (constantly {"template1" {:goal-config {:a :bb :c :dd}
                                          :e :ff
                                          :factory-fn 'cook.test.compute-cluster/cluster-factory-fn}})]
    (testing "normal initialize"
      (reset! cluster-name->compute-cluster-atom {})
      (reset! initialize-cluster-fn-invocations-atom [])
      (deliver exit-code-syncer-state-promise nil)
      (deliver scheduler-promise nil)
      (is (= {} (in-mem-configs)))
      (is (= ["name"]
             (initialize-cluster!
               {:a :a
                :name "name"
                :template "template1"
                :base-path "base-path"
                :ca-cert "ca-cert"
                :state :running
                :state-locked? true})))
      (is (= ["name"] @initialize-cluster-fn-invocations-atom))
      (is (= {"name" {:base-path "base-path"
                      :ca-cert "ca-cert"
                      :name "name"
                      :state :running
                      :state-locked? true
                      :template "template1"}} (in-mem-configs))))
    (testing "exception"
      (reset! cluster-name->compute-cluster-atom {})
      (reset! initialize-cluster-fn-invocations-atom [])
      (deliver exit-code-syncer-state-promise nil)
      (deliver scheduler-promise nil)
      (is (= {} (in-mem-configs)))
      (is (thrown? ExceptionInfo (initialize-cluster! {:name "fail" :a :a :template "template1"})))
      (is (= [] @initialize-cluster-fn-invocations-atom))
      (is (= {} (in-mem-configs))))))

(deftest test-execute-update!
  (with-redefs [config/compute-cluster-templates
                (constantly {"template1" {:goal-config {:a :bb :c :dd}
                                          :e :ff
                                          :factory-fn 'cook.test.compute-cluster/cluster-factory-fn}})]
    (testing "normal add and no change update with missing cluster"
      (let [uri "datomic:mem://test-compute-cluster-config"
            conn (restore-fresh-database! uri)]
        (reset! cluster-name->compute-cluster-atom {})
        (reset! initialize-cluster-fn-invocations-atom [])
        (deliver exit-code-syncer-state-promise nil)
        (deliver scheduler-promise nil)
        (is (= {} (db-config-ents (d/db conn))))
        (is (= {} (in-mem-configs)))
        ; normal add
        (is (= {:update-succeeded true}
               (execute-update! conn
                                {:goal-config
                                 {:a :a
                                  :name "name"
                                  :template "template1"
                                  :base-path "base-path"
                                  :ca-cert "ca-cert"
                                  :state :running
                                  :state-locked? true}
                                 :valid? true
                                 :changed? true
                                 :active? true}
                                (db-config-ents (d/db conn))
                                (in-mem-configs))))
        (is (= ["name"] @initialize-cluster-fn-invocations-atom))
        (is (= {:base-path "base-path"
                :ca-cert "ca-cert"
                :name "name"
                :state :running
                :state-locked? true
                :template "template1"}
               (-> (db-config-ents (d/db conn)) (get "name") compute-cluster-config-ent->compute-cluster-config)))
        (is (= {"name" {:base-path "base-path"
                        :ca-cert "ca-cert"
                        :name "name"
                        :state :running
                        :state-locked? true
                        :template "template1"}} (in-mem-configs)))
        ; no change update with missing cluster
        (reset! cluster-name->compute-cluster-atom {})
        (is (= {} (in-mem-configs)))
        (is (= {:update-succeeded true}
               (execute-update! conn
                                {:goal-config
                                 {:a :a
                                  :name "name"
                                  :template "template1"
                                  :base-path "base-path"
                                  :ca-cert "ca-cert"
                                  :state :running
                                  :state-locked? true}
                                 :valid? true
                                 :changed? false
                                 :active? true}
                                (db-config-ents (d/db conn))
                                (in-mem-configs))))
        (is (= ["name" "name"] @initialize-cluster-fn-invocations-atom))
        (is (= {:base-path "base-path"
                :ca-cert "ca-cert"
                :name "name"
                :state :running
                :state-locked? true
                :template "template1"}
               (-> (db-config-ents (d/db conn)) (get "name") compute-cluster-config-ent->compute-cluster-config)))
        (is (= {"name" {:base-path "base-path"
                        :ca-cert "ca-cert"
                        :name "name"
                        :state :running
                        :state-locked? true
                        :template "template1"}} (in-mem-configs)))))
    (testing "exceptions"
      (let [uri "datomic:mem://test-compute-cluster-config"
            conn (restore-fresh-database! uri)]
        (reset! cluster-name->compute-cluster-atom {})
        (reset! initialize-cluster-fn-invocations-atom [])
        (deliver exit-code-syncer-state-promise nil)
        (deliver scheduler-promise nil)
        (is (= {} (db-config-ents (d/db conn))))
        (is (= {} (in-mem-configs)))
        (is (= {:error-message "clojure.lang.ExceptionInfo: fail {}"
                :update-succeeded false}
               (execute-update! nil {:goal-config {:name "fail" :a :a :template "template1"} :valid? true :changed? true :active? true}
                                (db-config-ents (d/db conn))
                                (in-mem-configs))))
        (is (= [] @initialize-cluster-fn-invocations-atom))
        (is (= {} (db-config-ents (d/db conn))))
        (is (= {} (in-mem-configs)))

        (reset! initialize-cluster-fn-invocations-atom [])
        (is (= {:error-message "java.lang.NullPointerException"
                :update-succeeded false}
               (execute-update! nil {:goal-config {:name "fail" :a :a :template "template1"} :valid? true :changed? true :active? true} nil nil)))
        (is (= [] @initialize-cluster-fn-invocations-atom)))))

  (testing "normal update"
    (let [uri "datomic:mem://test-compute-cluster-config"
          conn (restore-fresh-database! uri)]
      (reset! initialize-cluster-fn-invocations-atom [])
      (deliver exit-code-syncer-state-promise nil)
      (deliver scheduler-promise nil)
      (is (= {} (db-config-ents (d/db conn))))
      (is (= {} (in-mem-configs)))
      (with-redefs [config/compute-cluster-templates
                    (constantly {"template1" {:goal-config {:a :bb :c :dd}
                                              :e :ff
                                              :factory-fn 'cook.test.compute-cluster/cluster-factory-fn}})]
        (is (= {:update-succeeded true}
               (execute-update! conn
                                {:goal-config {:a :a
                                               :name "name"
                                               :template "template1"
                                               :base-path "base-path"
                                               :ca-cert "ca-cert"
                                               :state :running
                                               :state-locked? true}
                                 :valid? true
                                 :changed? true
                                 :active? true}
                                (db-config-ents (d/db conn))
                                (in-mem-configs)))))
      (is (= {:base-path "base-path"
              :ca-cert "ca-cert"
              :name "name"
              :state :running
              :state-locked? true
              :template "template1"}
             (-> (db-config-ents (d/db conn)) (get "name") compute-cluster-config-ent->compute-cluster-config)))
      (is (= {"name" {:base-path "base-path"
                      :ca-cert "ca-cert"
                      :name "name"
                      :state :running
                      :state-locked? true
                      :template "template1"}} (in-mem-configs)))
      (is (= {:update-succeeded true}
             (execute-update! conn
                              {:goal-config {:a :a
                                             :name "name"
                                             :template "template1"
                                             :base-path "base-path-2"
                                             :ca-cert "ca-cert"
                                             :state :draining
                                             :state-locked? true}
                               :valid? true
                               :changed? true
                               :active? true}
                              (db-config-ents (d/db conn))
                              (in-mem-configs))))
      (is (= ["name"] @initialize-cluster-fn-invocations-atom))
      (is (= {:base-path "base-path-2"
              :ca-cert "ca-cert"
              :name "name"
              :state :draining
              :state-locked? true
              :template "template1"}
             (-> (db-config-ents (d/db conn)) (get "name") compute-cluster-config-ent->compute-cluster-config)))
      (is (= {"name" {:base-path "base-path"
                      :ca-cert "ca-cert"
                      :name "name"
                      :state :draining
                      :state-locked? true
                      :template "template1"}} (in-mem-configs))))))

(deftest test-update-compute-clusters
  (with-redefs [d/db (fn [_])
                db-config-ents (fn [_])
                in-mem-configs (constantly nil)
                config/compute-cluster-templates
                (constantly {"template1" {:a :bb
                                          :c :dd
                                          :factory-fn 'cook.test.compute-cluster/cluster-factory-fn}})
                compute-current-configs (fn [_ _] {"current" {:name "current" :a :b :state :running :ca-cert 1 :base-path 1}})
                execute-update! (fn [_ config _ _] (if (= "fail" (:name config)) {:update-succeeded false} {:update-succeeded true}))]
    (testing "single"
      (is (= '({:active? true
                :changed? true
                :cluster-name nil
                :goal-config {:a :a
                              :base-path 2
                              :ca-cert 2
                              :template "template1"}
                :update-result {:update-succeeded true}
                :valid? true}
               {:active? true
                :changed? false
                :cluster-name "current"
                :goal-config {:a :b
                              :base-path 1
                              :ca-cert 1
                              :name "current"
                              :state :running}
                :update-result {:update-succeeded true}
                :valid? true})
             (update-compute-clusters nil {:a :a :template "template1" :ca-cert 2 :base-path 2} nil false))))
    (testing "single - error"
      (is (= '({:active? true
                :changed? true
                :cluster-name "current"
                :goal-config {:base-path 2
                              :ca-cert 1
                              :name "current"
                              :state :running}
                :reason "Attempting to change something other than state when force? is false. Diff is ({:base-path 1, :a :b} {:base-path 2} {:ca-cert 1, :name \"current\"})"
                :update-result nil
                :valid? false})
             (update-compute-clusters nil {:name "current" :state :running :ca-cert 1 :base-path 2} nil false))))
    (testing "single - edit base-path"
      (is (= '({:active? true
                :changed? true
                :cluster-name "current"
                :goal-config {:base-path 2
                              :ca-cert 1
                              :name "current"
                              :state :running}
                :update-result {:update-succeeded true}
                :valid? true})
             (update-compute-clusters nil {:name "current" :state :running :ca-cert 1 :base-path 2} nil true))))
    (testing "multiple"
      (is (= '({:active? true
                :changed? true
                :cluster-name nil
                :goal-config {:a :a
                              :base-path 2
                              :ca-cert 2
                              :template "template1"}
                :update-result {:update-succeeded true}
                :valid? true}
               {:active? true
                :changed? false
                :cluster-name "current"
                :goal-config {:a :b
                              :base-path 1
                              :ca-cert 1
                              :name "current"
                              :state :running}
                :update-result {:update-succeeded true}
                :valid? true})
             (update-compute-clusters nil nil
                                      {"a"
                                       {:a :a :template "template1" :ca-cert 2 :base-path 2}
                                       "current"
                                       {:name "current" :a :b :state :running :ca-cert 1 :base-path 1}} false))))
    (testing "single and multiple"
      (is (thrown? AssertionError (update-compute-clusters nil {:a :a :template "template1"} {"a" {:a :a :template "template1"}} false))))
    (testing "errors"
      (is (= '({:active? true
                :changed? true
                :cluster-name nil
                :goal-config {:a :a
                              :template "template1"}
                :update-result {:update-succeeded true}
                :valid? true}
               {:active? true
                :changed? true
                :cluster-name "bad1"
                :goal-config {:name "bad1"}
                :reason "Attempting to create cluster with unknown template: "
                :update-result nil
                :valid? false}
               {:active? true
                :changed? true
                :cluster-name "current"
                :reason "Attempting to change something other than state when force? is false. Diff is ({:a :b} {:a :a} {:base-path 1, :ca-cert 1, :name \"current\"})"
                :update-result nil
                :goal-config {:name "current"
                              :a :a
                              :base-path 1
                              :ca-cert 1
                              :state :running}
                :valid? false})
             (update-compute-clusters nil nil {"a" {:a :a :template "template1"}
                                               "bad1" {:name "bad1"}
                                               "current" {:name "current" :a :a :state :running :ca-cert 1 :base-path 1}} false))))))

(deftest testing-get-compute-clusters
  (with-redefs [d/db (fn [_])
                db-config-ents (fn [_] {"name" {:compute-cluster-config/name "name"
                                                :compute-cluster-config/base-path "base-path"
                                                :compute-cluster-config/ca-cert "ca-cert"
                                                :compute-cluster-config/state :compute-cluster-config.state/running
                                                :compute-cluster-config/state-locked? true
                                                :compute-cluster-config/template "template"}})
                get-dynamic-clusters (constantly (->> [(sample-clusters "name")] (map-from-vals #(-> % :name))))]
    (is (= {:db-configs '({:base-path "base-path"
                           :ca-cert "ca-cert"
                           :name "name"
                           :state :running
                           :state-locked? true
                           :template "template"})
            :in-mem-configs '({:base-path "base-path"
                               :ca-cert "ca-cert"
                               :name "name"
                               :state :deleted
                               :state-locked? true
                               :template "template"
                               :compute-cluster-starting-config {:factory-fn cook.kubernetes.compute-cluster/factory-fn
                                                                 :base-path "base-path"
                                                                 :ca-cert "ca-cert"
                                                                 :name "name"
                                                                 :state :running
                                                                 :state-locked? false
                                                                 :template "template"}})}
           (get-compute-clusters nil)))))

(deftest test-delete-compute-cluster
  (let [uri "datomic:mem://test-compute-cluster-config"
        conn (restore-fresh-database! uri)
        temp-db-id (d/tempid :db.part/user)
        ent (compute-cluster-config->compute-cluster-config-ent
              {:name "name"
               :template "template"
               :base-path "base-path"
               :ca-cert "ca-cert"
               :state :running
               :state-locked? true})
        _ (is (= {} (db-config-ents (d/db conn))))
        tempids (-> @(d/transact conn [(assoc ent :db/id temp-db-id)]) :tempids)
        db (d/db conn)]
    (is (= {"name" {:db/id (d/resolve-tempid db tempids temp-db-id)
                    :compute-cluster-config/name "name"
                    :compute-cluster-config/base-path "base-path"
                    :compute-cluster-config/ca-cert "ca-cert"
                    :compute-cluster-config/state :compute-cluster-config.state/running
                    :compute-cluster-config/state-locked? true
                    :compute-cluster-config/template "template"}}
           (map-vals
             #(select-keys % [:db/id
                              :compute-cluster-config/name
                              :compute-cluster-config/base-path
                              :compute-cluster-config/ca-cert
                              :compute-cluster-config/state
                              :compute-cluster-config/state-locked?
                              :compute-cluster-config/template])
             (db-config-ents (d/db conn)))))
    (is (delete-compute-cluster conn {:name "name"}))
    (is (= {} (db-config-ents (d/db conn))))))

(deftest simulate-startup
  (let [uri "datomic:mem://test-compute-cluster-config"
        conn (restore-fresh-database! uri)
        new-cluster {:name "name"
                     :template "template1"
                     :base-path "base-path"
                     :ca-cert "ca-cert"
                     :state :running
                     :state-locked? true}
        ent (compute-cluster-config->compute-cluster-config-ent new-cluster)
        new-cluster-2 {:name "name-2"
                       :template "template1"
                       :base-path "base-path"
                       :ca-cert "ca-cert"
                       :state :deleted
                       :state-locked? true}
        ent-2 (compute-cluster-config->compute-cluster-config-ent new-cluster-2)]
    (is (= {} (in-mem-configs)))
    (is (= {} (db-config-ents (d/db conn))))
    @(d/transact conn [(assoc ent :db/id (d/tempid :db.part/user)) (assoc ent-2 :db/id (d/tempid :db.part/user))])
    (is (= {"name" new-cluster "name-2" new-cluster-2} (db-config-ents->configs (db-config-ents (d/db conn)))))
    (is (= {} (in-mem-configs)))
    (with-redefs [config/compute-cluster-templates
                  (constantly {"template1" {:a :bb
                                            :c :dd
                                            :factory-fn 'cook.test.compute-cluster/cluster-factory-fn}})]
      (deliver exit-code-syncer-state-promise nil)
      (deliver scheduler-promise nil)
      (is (= (set
               '({:active? true
                  :changed? false
                  :cluster-name "name"
                  :goal-config {:base-path "base-path"
                                :ca-cert "ca-cert"
                                :name "name"
                                :state :running
                                :state-locked? true
                                :template "template1"}
                  :update-result {:update-succeeded true}
                  :valid? true}
                 {:active? false
                  :changed? false
                  :cluster-name "name-2"
                  :goal-config {:base-path "base-path"
                                :ca-cert "ca-cert"
                                :name "name-2"
                                :state :deleted
                                :state-locked? true
                                :template "template1"}
                  :update-result {:update-succeeded true}
                  :valid? true}))
             (set (update-compute-clusters conn nil (db-config-ents->configs (db-config-ents (d/db conn))) false))))
      (is (= {"name" new-cluster "name-2" new-cluster-2} (db-config-ents->configs (db-config-ents (d/db conn)))))
      (is (= {"name" {:base-path "base-path"
                      :ca-cert "ca-cert"
                      :name "name"
                      :state :running
                      :state-locked? true
                      :template "template1"}}
             (in-mem-configs))))))
