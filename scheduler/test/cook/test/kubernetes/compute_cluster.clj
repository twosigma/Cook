(ns cook.test.kubernetes.compute-cluster
  (:require [clojure.test :refer :all]
            [cook.kubernetes.api :as api]
            [cook.kubernetes.compute-cluster :as kcc]
            [cook.test.testutil :as tu]
            [datomic.api :as d])
  (:import (io.kubernetes.client.models V1Pod V1PodStatus V1ContainerStatus V1ContainerState V1ContainerStateWaiting)))

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

(deftest test-generate-offers
  (let [compute-cluster (kcc/->KubernetesComputeCluster nil "kubecompute" nil nil nil (atom {}) (atom {}) (atom {}) (atom {}))
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
