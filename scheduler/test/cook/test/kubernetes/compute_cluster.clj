(ns cook.test.kubernetes.compute-cluster
  (:require [clojure.core.cache :as cache]
            [clojure.test :refer :all]
            [cook.compute-cluster :as cc]
            [cook.kubernetes.api :as api]
            [cook.kubernetes.compute-cluster :as kcc]
            [cook.mesos.task :as task]
            [cook.scheduler.scheduler :as sched]
            [cook.test.testutil :as tu]
            [cook.tools :as util]
            [datomic.api :as d])
  (:import (com.netflix.fenzo SimpleAssignmentResult)))

(deftest test-get-or-create-cluster-entity-id
  (let [conn (tu/restore-fresh-database! "datomic:mem://test-get-or-create-cluster-entity-id")]
    (testing "successfully creates clusters"
      (let [eid (kcc/get-or-create-cluster-entity-id conn "test-a")
            entity (d/entity (d/db conn) eid)]
        (is (= "test-a" (:compute-cluster/cluster-name entity)))
        (is (= :compute-cluster.type/kubernetes (:compute-cluster/type entity)))))
    (testing "does not create duplicate clusters"
      (let [eid (kcc/get-or-create-cluster-entity-id conn "test-b")
            eid2 (kcc/get-or-create-cluster-entity-id conn "test-b")]
        (is eid)
        (is eid2)
        (is (= eid eid2))))))

(deftest test-namespace-config
  (tu/setup)
  (let [conn (tu/restore-fresh-database! "datomic:mem://test-namespace-config")
        make-task-request (fn make-task-request [user]
                            (let [job-id (tu/create-dummy-job conn :user user)
                                  db (d/db conn)
                                  job-ent (d/entity (d/db conn) job-id)
                                  considerable->task-id (plumbing.core/map-from-keys (fn [_] (str (d/squuid)))
                                                                                     [job-ent])
                                  running-cotask-cache (atom (cache/fifo-cache-factory {} :threshold 1))]
                              (sched/make-task-request db
                                                       job-ent
                                                       :guuid->considerable-cotask-ids
                                                       (util/make-guuid->considerable-cotask-ids considerable->task-id)
                                                       :reserved-hosts []
                                                       :running-cotask-cache running-cotask-cache
                                                       :task-id (considerable->task-id job-ent))))
        make-task-asignment-result (fn make-task-assignment-result [user]
                                     (let [task-request (make-task-request user)]
                                       (SimpleAssignmentResult. [] nil task-request)))
        launched-pod-atom (atom nil)]
    (with-redefs [api/launch-task (fn [api {:keys [launch-pod]}]
                                    (reset! launched-pod-atom launch-pod))]
      (testing "static namespace"
        (let [compute-cluster (kcc/->KubernetesComputeCluster nil "kubecompute" nil nil nil
                                                              (atom {}) (atom {}) (atom {}) (atom {}) (atom nil)
                                                              {:kind :static :namespace "cook"})
              task-metadata (task/TaskAssignmentResult->task-metadata (d/db conn)
                                                                      nil
                                                                      compute-cluster
                                                                      (make-task-asignment-result "testuser"))]

          (cc/launch-tasks compute-cluster [] [task-metadata])
          (is (= "cook" (:namespace @launched-pod-atom)))))

      (testing "per-user namespace"
        (let [compute-cluster (kcc/->KubernetesComputeCluster nil "kubecompute" nil nil nil
                                                              (atom {}) (atom {}) (atom {}) (atom {}) (atom nil)
                                                              {:kind :per-user})
              task-metadata (task/TaskAssignmentResult->task-metadata (d/db conn)
                                                                      nil
                                                                      compute-cluster
                                                                      (make-task-asignment-result "testuser"))]
          (cc/launch-tasks compute-cluster [] [task-metadata])
          (is (= "testuser" (:namespace @launched-pod-atom))))))))

(deftest test-generate-offers
  (let [compute-cluster (kcc/->KubernetesComputeCluster nil "kubecompute" nil nil nil
                                                        (atom {}) (atom {}) (atom {}) (atom {}) (atom nil)
                                                        {:kind :static :namespace "cook"})
        node-name->node {"nodeA" (tu/node-helper "nodeA" 1.0 1000.0)
                         "nodeB" (tu/node-helper "nodeB" 1.0 1000.0)
                         "nodeC" (tu/node-helper "nodeC" 1.0 1000.0)}
        pod-name->pod {"podA" (tu/pod-helper "podA" "nodeA"
                                          {:cpus 0.25 :mem 250.0}
                                          {:cpus 0.1 :mem 100.0})
                       "podB" (tu/pod-helper "podB" "nodeA"
                                          {:cpus 0.25 :mem 250.0})
                       "podC" (tu/pod-helper "podC" "nodeB"
                                          {:cpus 1.0 :mem 1100.0})}
        offers (kcc/generate-offers node-name->node pod-name->pod compute-cluster)]
    (is (= 3 (count offers)))
    (let [offer (first (filter #(= "nodeA" (:hostname %))
                                     offers))]
      (is (not (nil? offer)))
      (is (= "kubecompute" (:framework-id offer)))
      (is (= {:value "nodeA"} (:slave-id offer)))
      (is (= [{:name "mem" :type :value-scalar :scalar 400.0}
              {:name "cpus" :type :value-scalar :scalar 0.4}
              {:name "disk" :type :value-scalar :scalar 0.0}]
             (:resources offer)))
      (is (:reject-after-match-attempt offer)))

    (let [offer (first (filter #(= "nodeB" (:hostname %))
                                     offers))]
      (is (= {:value "nodeB"} (:slave-id offer)))
      (is (= [{:name "mem" :type :value-scalar :scalar 0.0}
              {:name "cpus" :type :value-scalar :scalar 0.0}
              {:name "disk" :type :value-scalar :scalar 0.0}]
             (:resources offer))))))

(deftest determine-expected-state
  ; TODO
  )
