(ns cook.test.kubernetes.api
  (:require [clojure.test :refer :all]
            [cook.config :as config]
            [cook.kubernetes.api :as api]
            [cook.test.testutil :as tu]
            [datomic.api :as d])
  (:import (io.kubernetes.client.models V1Container V1EnvVar V1Pod V1PodStatus V1ContainerStatus V1ContainerState V1ContainerStateWaiting V1VolumeMount V1Volume)))

(deftest test-get-consumption
  (testing "correctly computes consumption for a single pod"
    (let [pods [(tu/pod-helper "podA" "hostA" {:cpus 1.0 :mem 100.0})]]
      (is (= {"hostA" {:cpus 1.0
                       :mem 100.0}}
             (api/get-consumption pods)))))

  (testing "correctly computes consumption for a pod with multiple containers"
    (let [pods [(tu/pod-helper "podA" "hostA"
                               {:cpus 1.0 :mem 100.0}
                               {:cpus 1.0 :mem 0.0}
                               {:mem 100.0})]]
      (is (= {"hostA" {:cpus 2.0
                       :mem 200.0}}
             (api/get-consumption pods)))))

  (testing "correctly aggregates pods by node name"
    (let [pods [(tu/pod-helper "podA" "hostA"
                               {:cpus 1.0 :mem 100.0})
                (tu/pod-helper "podB" "hostA"
                               {:cpus 1.0})
                (tu/pod-helper "podC" "hostB"
                               {:cpus 1.0}
                               {:mem 100.0})
                (tu/pod-helper "podD" "hostC"
                               {:cpus 1.0})]]
      (is (= {"hostA" {:cpus 2.0 :mem 100.0}
              "hostB" {:cpus 1.0 :mem 100.0}
              "hostC" {:cpus 1.0 :mem 0.0}}
             (api/get-consumption pods))))))

(deftest test-get-capacity
  (let [node-name->node {"nodeA" (tu/node-helper "nodeA" 1.0 100.0 nil)
                         "nodeB" (tu/node-helper "nodeB" 1.0 nil nil)
                         "nodeC" (tu/node-helper "nodeC" nil 100.0 nil)
                         "nodeD" (tu/node-helper "nodeD" nil nil nil)}]
    (is (= {"nodeA" {:cpus 1.0 :mem 100.0}
            "nodeB" {:cpus 1.0 :mem 0.0}
            "nodeC" {:cpus 0.0 :mem 100.0}
            "nodeD" {:cpus 0.0 :mem 0.0}}
           (api/get-capacity node-name->node)))))

(defn assert-env-var-value
  [container name value]
  (let [^V1EnvVar variable (->> container .getEnv (filter (fn [var] (= name (.getName var)))) first)]
    (is (= name (.getName variable)))
    (is (= value (.getValue variable)))))

(deftest test-task-metadata->pod
  (testing "creates pod from metadata"
    (with-redefs [config/kubernetes (constantly {:default-workdir "/mnt/sandbox"})]
      (let [task-metadata {:task-id "my-task"
                           :command {:value "foo && bar"
                                     :environment {"FOO" "BAR"}
                                     :user (System/getProperty "user.name")}
                           :container {:type :docker
                                       :docker {:image "alpine:latest"}}
                           :task-request {:resources {:mem 512
                                                      :cpus 1.0}}
                           :hostname "kubehost"}
            pod (api/task-metadata->pod "cook" "testing-cluster" task-metadata)]
        (is (= "my-task" (-> pod .getMetadata .getName)))
        (is (= "cook" (-> pod .getMetadata .getNamespace)))
        (is (= "Never" (-> pod .getSpec .getRestartPolicy)))
        (is (= "kubehost" (-> pod .getSpec .getNodeName)))
        (is (= 1 (count (-> pod .getSpec .getContainers))))
        (is (= {api/cook-pod-label "testing-cluster"} (-> pod .getMetadata .getLabels)))
        (is (< 0 (-> pod .getSpec .getSecurityContext .getRunAsGroup)))
        (is (< 0 (-> pod .getSpec .getSecurityContext .getRunAsUser)))

        (let [workdir-volume (->> pod
                                  .getSpec
                                  .getVolumes
                                  (filter (fn [^V1Volume v] (= "cook-workdir-volume" (.getName v))))
                                  first)]
          (is (not (nil? (.getEmptyDir workdir-volume)))))

        (let [^V1Container container (-> pod .getSpec .getContainers first)]
          (is (= "required-cook-job-container" (.getName container)))
          (is (= ["/bin/sh" "-c" "foo && bar"] (.getCommand container)))
          (is (= "alpine:latest" (.getImage container)))
          (is (= 4 (count (.getEnv container))))
          (is (= "/mnt/sandbox" (.getWorkingDir container)))
          (let [workdir-mount (->> container
                                   .getVolumeMounts
                                   (filter (fn [^V1VolumeMount m] (= "cook-workdir-volume" (.getName m))))
                                   first)]
            (is (= "/mnt/sandbox" (.getMountPath workdir-mount))))

          (assert-env-var-value container "FOO" "BAR")
          (assert-env-var-value container "HOME" (.getWorkingDir container))
          (assert-env-var-value container "MESOS_SANDBOX" (.getWorkingDir container))

          (let [resources (-> container .getResources)]
            (is (= 1.0 (-> resources .getRequests (get "cpu") .getNumber .doubleValue)))
            (is (= (* 512.0 api/memory-multiplier) (-> resources .getRequests (get "memory") .getNumber .doubleValue)))
            (is (= (* 512.0 api/memory-multiplier) (-> resources .getLimits (get "memory") .getNumber .doubleValue))))))))

  (testing "user parameter"
    (let [task-metadata {:task-id "my-task"
                         :command {:value "foo && bar"
                                   :environment {"FOO" "BAR"}
                                   :user (System/getProperty "user.name")}
                         :container {:type :docker
                                     :docker {:image "alpine:latest"
                                              :parameters [{:key "user"
                                                            :value "100:10"}]}}
                         :task-request {:resources {:mem 512
                                                    :cpus 1.0}}
                         :hostname "kubehost"}
          pod (api/task-metadata->pod "cook" "test-cluster" task-metadata)]
      (is (= 100 (-> pod .getSpec .getSecurityContext .getRunAsUser)))
      (is (= 10 (-> pod .getSpec .getSecurityContext .getRunAsGroup))))))

(deftest test-make-volumes
  (testing "defaults for minimal volume"
    (let [host-path "/tmp/$_*/foo"
          {:keys [volumes volume-mounts]} (api/make-volumes [{:host-path host-path}])]
      (is (= 1 (count volumes)))
      (is (= 1 (count volume-mounts)))
      (let [volume (first volumes)
            volume-mount (first volume-mounts)]
        (is (= (.getName volume)
               (.getName volume-mount)))
        ; validation regex for k8s names
        (is (re-matches #"[a-z0-9]([-a-z0-9]*[a-z0-9])?" (.getName volume)))
        (is (= host-path (-> volume .getHostPath .getPath)))
        (is (.isReadOnly volume-mount))
        (is (= host-path (.getMountPath volume-mount))))))
  (testing "correct values for fully specified volume"
    (let [host-path "/tmp/foo"
          container-path "/mnt/foo"
          {:keys [volumes volume-mounts]} (api/make-volumes [{:host-path host-path
                                                              :container-path container-path
                                                              :mode "RW"}])
          [volume] volumes
          [volume-mount] volume-mounts]
      (is (= (.getName volume)
             (.getName volume-mount)))
      (is (= host-path (-> volume .getHostPath .getPath)))
      (is (not (.isReadOnly volume-mount)))
      (is (= container-path (.getMountPath volume-mount)))))
  (testing "disallows configured volumes"
    (with-redefs [config/kubernetes (constantly {:disallowed-container-paths #{"/tmp/foo"}})]
      (is (= {:volumes []
              :volume-mounts []}
             (api/make-volumes [{:host-path "/tmp/foo"}])))
      (is (= {:volumes []
              :volume-mounts []}
             (api/make-volumes [{:container-path "/tmp/foo"
                                 :host-path "/mnt/foo"}]))))))


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
             (api/pod->synthesized-pod-state pod)))))

  (testing "pod failed phase"
    (let [pod (V1Pod.)
          pod-status (V1PodStatus.)]
      (.setPhase pod-status "Failed")
      (.setReason pod-status "SomeSillyReason")
      (.setStatus pod pod-status)
      (is (= {:state :pod/failed
              :reason "SomeSillyReason"}
             (api/pod->synthesized-pod-state pod))))))
