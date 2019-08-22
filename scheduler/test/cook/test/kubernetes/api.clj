(ns cook.test.kubernetes.api
  (:require [clojure.test :refer :all]
            [cook.kubernetes.api :as api]
            [cook.test.testutil :as tu]
            [datomic.api :as d])
  (:import (io.kubernetes.client.models V1Container V1EnvVar V1Pod V1PodStatus V1ContainerStatus V1ContainerState V1ContainerStateWaiting)))

(deftest test-get-consumption
  (testing "correctly computes consumption for a single pod"
    (let [pod-name->pod {"podA" (tu/pod-helper "podA" "hostA" {:cpus 1.0 :mem 100.0})}]
      (is (= {"hostA" {:cpus 1.0
                       :mem 100.0}}
             (api/get-consumption pod-name->pod)))))

  (testing "correctly computes consumption for a pod with multiple containers"
    (let [pod-name->pod {"podA" (tu/pod-helper "podA" "hostA"
                                            {:cpus 1.0 :mem 100.0}
                                            {:cpus 1.0 :mem 0.0}
                                            {:mem 100.0})}]
      (is (= {"hostA" {:cpus 2.0
                       :mem 200.0}}
             (api/get-consumption pod-name->pod)))))

  (testing "correctly aggregates pods by node name"
    (let [pod-name->pod {"podA" (tu/pod-helper "podA" "hostA"
                                            {:cpus 1.0 :mem 100.0})
                         "podB" (tu/pod-helper "podB" "hostA"
                                            {:cpus 1.0})
                         "podC" (tu/pod-helper "podC" "hostB"
                                            {:cpus 1.0}
                                            {:mem 100.0})
                         "podD" (tu/pod-helper "podD" "hostC"
                                            {:cpus 1.0})}]
      (is (= {"hostA" {:cpus 2.0 :mem 100.0}
              "hostB" {:cpus 1.0 :mem 100.0}
              "hostC" {:cpus 1.0 :mem 0.0}}
             (api/get-consumption pod-name->pod))))))

(deftest test-get-capacity
  (let [node-name->node {"nodeA" (tu/node-helper "nodeA" 1.0 100.0)
                         "nodeB" (tu/node-helper "nodeB" 1.0 nil)
                         "nodeC" (tu/node-helper "nodeC" nil 100.0)
                         "nodeD" (tu/node-helper "nodeD" nil nil)}]
    (is (= {"nodeA" {:cpus 1.0 :mem 100.0}
            "nodeB" {:cpus 1.0 :mem 0.0}
            "nodeC" {:cpus 0.0 :mem 100.0}
            "nodeD" {:cpus 0.0 :mem 0.0}}
           (api/get-capacity node-name->node)))))

(deftest test-task-metadata->pod
  (let [task-metadata {:task-id "my-task"
                       :command {:value "foo && bar"
                                 :environment {"FOO" "BAR"}}
                       :container {:type :docker
                                   :docker {:image "alpine:latest"}}
                       :task-request {:resources {:mem 512
                                                  :cpus 1.0}}
                       :hostname "kubehost"}
        pod (api/task-metadata->pod "cook" task-metadata)]
    (is (= "my-task" (-> pod .getMetadata .getName)))
    (is (= "cook" (-> pod .getMetadata .getNamespace)))
    (is (= "Never" (-> pod .getSpec .getRestartPolicy)))
    (is (= "kubehost" (-> pod .getSpec .getNodeName)))
    (is (= 1 (count (-> pod .getSpec .getContainers))))

    (let [^V1Container container (-> pod .getSpec .getContainers first)]
      (is (= "required-cook-job-container" (.getName container)))
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

(deftest test-pod->synthesized-pod-state
  (testing "returns nil for empty pod"
    (is (nil? (api/pod->synthesized-pod-state nil))))

  (testing "no container status -> waiting"
    (let [pod (V1Pod.)
          pod-status (V1PodStatus.)]
      (.setStatus pod pod-status)
      (is (= {:state :pod/waiting
              :reason "Pending"}
             (api/pod->synthesized-pod-state pod)))))

  (testing "waiting"
    (let [pod (V1Pod.)
          pod-status (V1PodStatus.)
          container-status (V1ContainerStatus.)
          container-state (V1ContainerState.)
          waiting (V1ContainerStateWaiting.)]
      (.setReason waiting "waiting")
      (.setWaiting container-state waiting)
      (.setState container-status container-state)
      (.setName container-status "required-cook-job-container")
      (.setContainerStatuses pod-status [container-status])
      (.setStatus pod pod-status)

      (is (= {:state :pod/waiting
              :reason "waiting"}
             (api/pod->synthesized-pod-state pod))))))
