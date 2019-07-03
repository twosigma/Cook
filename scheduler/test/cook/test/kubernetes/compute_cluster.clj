(ns cook.test.kubernetes.compute-cluster
  (:require [clojure.test :refer :all]
            [cook.kubernetes.compute-cluster :as kcc]
            [cook.test.testutil :as tu]
            [datomic.api :as d])
  (:import (io.kubernetes.client.models V1Pod V1ObjectMeta V1PodSpec V1Container V1ResourceRequirements V1Node V1NodeStatus V1EnvVar V1PodStatus V1ContainerStatus V1ContainerState V1ContainerStateWaiting)
           (io.kubernetes.client.custom Quantity Quantity$Format)
           (java.math BigDecimal)))

(defn pod-helper [pod-name node-name & requests]
  (let [pod (V1Pod.)
        metadata (V1ObjectMeta.)
        spec (V1PodSpec.)]
    (doall (for [{:keys [mem cpus]} requests]
             (let [container (V1Container.)
                   resources (V1ResourceRequirements.)]
               (when mem
                 (.putRequestsItem resources
                                   "memory"
                                   (Quantity. (BigDecimal. ^double (* 1024 1024 mem))
                                              Quantity$Format/DECIMAL_SI)))
               (when cpus
                 (.putRequestsItem resources
                                   "cpu"
                                   (Quantity. (BigDecimal. cpus)
                                              Quantity$Format/DECIMAL_SI)))
               (.setResources container resources)
               (.addContainersItem spec container))))
    (.setNodeName spec node-name)
    (.setName metadata pod-name)
    (.setMetadata pod metadata)
    (.setSpec pod spec)
    pod))

(defn node-helper [node-name cpus mem]
  (let [node (V1Node.)
        status (V1NodeStatus.)
        metadata (V1ObjectMeta.)]
    (when cpus
      (.putCapacityItem status "cpu" (Quantity. (BigDecimal. cpus)
                                                Quantity$Format/DECIMAL_SI)))
    (when mem
      (.putCapacityItem status "memory" (Quantity. (BigDecimal. (* 1024 1024 mem))
                                                   Quantity$Format/DECIMAL_SI)))
    (.setStatus node status)
    (.setName metadata node-name)
    (.setMetadata node metadata)
    node))

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

(deftest test-get-consumption
  (testing "correctly computes consumption for a single pod"
    (let [pod-name->pod {"podA" (pod-helper "podA" "hostA" {:cpus 1.0 :mem 100.0})}]
      (is (= {"hostA" {:cpus 1.0
                       :mem 100.0}}
             (kcc/get-consumption pod-name->pod)))))

  (testing "correctly computes consumption for a pod with multiple containers"
    (let [pod-name->pod {"podA" (pod-helper "podA" "hostA"
                                            {:cpus 1.0 :mem 100.0}
                                            {:cpus 1.0 :mem 0.0}
                                            {:mem 100.0})}]
      (is (= {"hostA" {:cpus 2.0
                       :mem 200.0}}
             (kcc/get-consumption pod-name->pod)))))

  (testing "correctly aggregates pods by node name"
    (let [pod-name->pod {"podA" (pod-helper "podA" "hostA"
                                            {:cpus 1.0 :mem 100.0})
                         "podB" (pod-helper "podB" "hostA"
                                            {:cpus 1.0})
                         "podC" (pod-helper "podC" "hostB"
                                            {:cpus 1.0}
                                            {:mem 100.0})
                         "podD" (pod-helper "podD" "hostC"
                                            {:cpus 1.0})}]
      (is (= {"hostA" {:cpus 2.0 :mem 100.0}
              "hostB" {:cpus 1.0 :mem 100.0}
              "hostC" {:cpus 1.0 :mem 0.0}}
             (kcc/get-consumption pod-name->pod))))))

(deftest test-get-capacity
  (let [node-name->node {"nodeA" (node-helper "nodeA" 1.0 100.0)
                         "nodeB" (node-helper "nodeB" 1.0 nil)
                         "nodeC" (node-helper "nodeC" nil 100.0)
                         "nodeD" (node-helper "nodeD" nil nil)}]
    (is (= {"nodeA" {:cpus 1.0 :mem 100.0}
            "nodeB" {:cpus 1.0 :mem 0.0}
            "nodeC" {:cpus 0.0 :mem 100.0}
            "nodeD" {:cpus 0.0 :mem 0.0}}
           (kcc/get-capacity node-name->node)))))

(deftest test-generate-offers
  (let [compute-cluster (kcc/->KubernetesComputeCluster nil "kubecompute" nil nil nil)
        node-name->node {"nodeA" (node-helper "nodeA" 1.0 1000.0)
                         "nodeB" (node-helper "nodeB" 1.0 1000.0)
                         "nodeC" (node-helper "nodeC" 1.0 1000.0)}
        pod-name->pod {"podA" (pod-helper "podA" "nodeA"
                                          {:cpus 0.25 :mem 250.0}
                                          {:cpus 0.1 :mem 100.0})
                       "podB" (pod-helper "podB" "nodeA"
                                          {:cpus 0.25 :mem 250.0})
                       "podC" (pod-helper "podC" "nodeB"
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
      (is (:reject-after-match offer)))

    (let [offer (first (filter #(= "nodeB" (:hostname %))
                                     offers))]
      (is (= {:value "nodeB"} (:slave-id offer)))
      (is (= [{:name "mem" :type :value-scalar :scalar 0.0}
              {:name "cpus" :type :value-scalar :scalar 0.0}
              {:name "disk" :type :value-scalar :scalar 0.0}]
             (:resources offer))))))

(deftest test-task-metadata->pod
  (let [task-metadata {:task-id "my-task"
                       :command {:value "foo && bar"
                                 :environment {"FOO" "BAR"}}
                       :container {:type :docker
                                   :docker {:image "alpine:latest"}}
                       :task-request {:resources {:mem 512
                                                  :cpus 1.0}}
                       :hostname "kubehost"}
        pod (kcc/task-metadata->pod task-metadata)]
    (is (= "my-task" (-> pod .getMetadata .getName)))
    (is (= "Never" (-> pod .getSpec .getRestartPolicy)))
    (is (= "kubehost" (-> pod .getSpec .getNodeName)))
    (is (= 1 (count (-> pod .getSpec .getContainers))))

    (let [^V1Container container (-> pod .getSpec .getContainers first)]
      (is (= "job" (.getName container)))
      (is (= ["/bin/sh" "-c" "foo && bar"] (.getCommand container)))
      (is (= "alpine:latest" (.getImage container)))
      (is (= 1 (count (.getEnv container))))

      (let [^V1EnvVar variable (-> container .getEnv first)]
        (is (= "FOO" (.getName variable)))
        (is (= "BAR" (.getValue variable))))

      (let [resources (-> container .getResources)]
        (is (= 1.0 (-> resources .getRequests (get "cpu") .getNumber .doubleValue)))
        (is (= (* 512.0 1024 1024) (-> resources .getRequests (get "memory") .getNumber .doubleValue)))
        (is (= (* 512.0 1024 1024) (-> resources .getLimits (get "memory") .getNumber .doubleValue)))))))

(deftest test-pod->pod-state
  (testing "returns nil for empty pod"
    (is (nil? (kcc/pod->pod-state nil))))

  (testing "no container status -> waiting"
    (let [pod (V1Pod.)
          pod-status (V1PodStatus.)]
      (.setStatus pod pod-status)
      (is (= {:state :pod/waiting
              :reason "Pending"}
             (kcc/pod->pod-state pod)))))

  (testing "waiting"
    (let [pod (V1Pod.)
          pod-status (V1PodStatus.)
          container-status (V1ContainerStatus.)
          container-state (V1ContainerState.)
          waiting (V1ContainerStateWaiting.)]
      (.setReason waiting "waiting")
      (.setWaiting container-state waiting)
      (.setState container-status container-state)
      (.setName container-status "job")
      (.setContainerStatuses pod-status [container-status])
      (.setStatus pod pod-status)

      (is (= {:state :pod/waiting
              :reason "waiting"}
             (kcc/pod->pod-state pod))))))
