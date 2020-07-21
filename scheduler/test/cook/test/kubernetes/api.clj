(ns cook.test.kubernetes.api
  (:require [clj-time.core :as t]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [cook.config :as config]
            [cook.kubernetes.api :as api]
            [cook.test.testutil :as tu]
            [datomic.api :as d])
  (:import (io.kubernetes.client.openapi.models V1Container V1ContainerState V1ContainerStateWaiting V1ContainerStatus
                                                V1EnvVar V1ListMeta V1Node V1NodeSpec V1ObjectMeta V1Pod V1PodCondition
                                                V1PodList V1PodSpec V1PodStatus V1ResourceRequirements V1Taint V1Volume
                                                V1VolumeMount)))

(deftest test-get-consumption
  (testing "correctly computes consumption for a single pod without gpus"

    (let [pods [(tu/pod-helper "podA" "hostA" {:cpus 1.0 :mem 100.0})]
          node-name->pods (api/pods->node-name->pods pods)]
      (is (= {"hostA" {:cpus 1.0
                       :mem 100.0
                       :gpus {}}}
             (api/get-consumption node-name->pods)))))

  (testing "correctly computes consumption for a single pod with gpus"

    (let [pods [(tu/pod-helper "podA" "hostA" {:cpus 1.0 :mem 100.0 :gpus "2" :gpu-model "nvidia-tesla-p100"})]
          node-name->pods (api/pods->node-name->pods pods)]
      (is (= {"hostA" {:cpus 1.0
                       :mem 100.0
                       :gpus {"nvidia-tesla-p100" 2}}}
             (api/get-consumption node-name->pods)))))

  (testing "correctly computes consumption for a pod with multiple containers without gpus"
    (let [pods [(tu/pod-helper "podA" "hostA"
                               {:cpus 1.0 :mem 100.0 :gpus "0"}
                               {:cpus 1.0 :mem 0.0}
                               {:mem 100.0})]
          node-name->pods (api/pods->node-name->pods pods)]
      (is (= {"hostA" {:cpus 2.0
                       :mem 200.0
                       :gpus {}}}
             (api/get-consumption node-name->pods)))))

  (testing "correctly computes consumption for a pod with multiple containers with gpus"
    (let [pods [(tu/pod-helper "podA" "hostA"
                               {:cpus 1.0 :mem 100.0 :gpus "1" :gpu-model "nvidia-tesla-p100"}
                               {:cpus 1.0 :mem 0.0 :gpus "4" :gpu-model "nvidia-tesla-p100"}
                               {:mem 100.0})]
          node-name->pods (api/pods->node-name->pods pods)]
      (is (= {"hostA" {:cpus 2.0
                       :mem 200.0
                       :gpus {"nvidia-tesla-p100" 5}}}
             (api/get-consumption node-name->pods)))))

  (testing "correctly aggregates pods by node name"
    (let [pods [(tu/pod-helper "podA" "hostA"
                               {:cpus 1.0
                                :mem 100.0
                                :gpus "2"
                                :gpu-model "nvidia-tesla-p100"})
                (tu/pod-helper "podB" "hostA"
                               {:cpus 1.0})
                (tu/pod-helper "podA" "hostA"
                               {:gpus "1"
                                :gpu-model "nvidia-tesla-p100"})
                (tu/pod-helper "podC" "hostB"
                               {:cpus 1.0}
                               {:mem 100.0})
                (tu/pod-helper "podC" "hostB"
                               {:cpus 2.0}
                               {:mem 30.0
                                :gpus "1"
                                :gpu-model "nvidia-tesla-k80"})
                (tu/pod-helper "podD" "hostC"
                               {:cpus 1.0})
                (tu/pod-helper "podD" nil ; nil host should be skipped and not included in output.
                               {:cpus 12.0})]
          node-name->pods (api/pods->node-name->pods pods)]
      (is (= {"hostA" {:cpus 2.0 :mem 100.0 :gpus {"nvidia-tesla-p100" 3}}
              "hostB" {:cpus 3.0 :mem 130.0 :gpus {"nvidia-tesla-k80" 1}}
              "hostC" {:cpus 1.0 :mem 0.0 :gpus {}}}
             (api/get-consumption node-name->pods))))))

(deftest test-get-capacity
  (let [node-name->node {"nodeA" (tu/node-helper "nodeA" 1.0 100.0 2 "nvidia-tesla-p100" nil)
                         "nodeB" (tu/node-helper "nodeB" 1.0 nil nil nil nil)
                         "nodeC" (tu/node-helper "nodeC" nil 100.0 5 "nvidia-tesla-p100" nil)
                         "nodeD" (tu/node-helper "nodeD" nil nil 7 "nvidia-tesla-p100" nil)}]
    (is (= {"nodeA" {:cpus 1.0 :mem 100.0 :gpus {"nvidia-tesla-p100" 2}}
            "nodeB" {:cpus 1.0 :mem 0.0 :gpus {}}
            "nodeC" {:cpus 0.0 :mem 100.0 :gpus {"nvidia-tesla-p100" 5}}
            "nodeD" {:cpus 0.0 :mem 0.0 :gpus {"nvidia-tesla-p100" 7}}}
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
                           ;; assume this task requested {cpu:1.0,mem:512} for the job's container
                           ;; plus an additional {cpu:0.1,mem:64} for a sidecar container
                           :task-request {:resources {:mem 576
                                                      :cpus 1.1}
                                          :scalar-requests {"mem" 512
                                                            "cpus" 1.0}}
                           :hostname "kubehost"}
            pod (api/task-metadata->pod "cook" "testing-cluster" task-metadata)]
        (is (= "my-task" (-> pod .getMetadata .getName)))
        (is (= "cook" (-> pod .getMetadata .getNamespace)))
        (is (= "Never" (-> pod .getSpec .getRestartPolicy)))
        (is (= "kubehost" (-> pod .getSpec .getNodeSelector (get api/k8s-hostname-label))))
        (is (= 1 (count (-> pod .getSpec .getContainers))))
        (is (= {api/cook-pod-label "testing-cluster"} (-> pod .getMetadata .getLabels)))
        (is (< 0 (-> pod .getSpec .getSecurityContext .getRunAsGroup)))
        (is (< 0 (-> pod .getSpec .getSecurityContext .getRunAsUser)))

        (let [cook-sandbox-volume (->> pod
                                       .getSpec
                                       .getVolumes
                                       (filter (fn [^V1Volume v] (= "cook-sandbox-volume" (.getName v))))
                                       first)]
          (is (not (nil? cook-sandbox-volume)))
          (is (not (nil? (.getEmptyDir cook-sandbox-volume)))))

        (let [^V1Container container (-> pod .getSpec .getContainers first)
              container-env (.getEnv container)]
          (is (= "required-cook-job-container" (.getName container)))
          (is (= (conj api/default-shell "foo && bar") (.getCommand container)))
          (is (= "alpine:latest" (.getImage container)))
          (is (not (nil? container)))
          (is (= ["COOK_SANDBOX"
                  "EXECUTOR_PROGRESS_OUTPUT_FILE"
                  "FOO"
                  "HOME"
                  "MESOS_DIRECTORY"
                  "MESOS_SANDBOX"
                  "SIDECAR_WORKDIR"]
                 (->> container-env (map #(.getName %)) sort)))
          (is (= "/mnt/sandbox" (.getWorkingDir container)))
          (let [cook-sandbox-mount (->> container
                                        .getVolumeMounts
                                        (filter (fn [^V1VolumeMount m] (= "cook-sandbox-volume" (.getName m))))
                                        first)]
            (is (= "/mnt/sandbox" (.getMountPath cook-sandbox-mount))))

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
                         ;; assume this task requested {cpu:1.0,mem:512} for the job's container
                         ;; plus an additional {cpu:0.1,mem:64} for a sidecar container
                         :task-request {:resources {:mem 576
                                                    :cpus 1.1}
                                        :scalar-requests {"mem" 512
                                                          "cpus" 1.0}}
                         :hostname "kubehost"}
          pod (api/task-metadata->pod "cook" "test-cluster" task-metadata)]
      (is (= 100 (-> pod .getSpec .getSecurityContext .getRunAsUser)))
      (is (= 10 (-> pod .getSpec .getSecurityContext .getRunAsGroup)))))

  (testing "node selector for pool"
    (let [pool-name "test-pool"
          task-metadata {:container {:docker {:parameters [{:key "user"
                                                            :value "100:10"}]}}
                         :task-request {:job {:job/pool {:pool/name pool-name}}
                                        :scalar-requests {"mem" 512
                                                          "cpus" 1.0}}}
          ^V1Pod pod (api/task-metadata->pod nil nil task-metadata)
          ^V1PodSpec pod-spec (.getSpec pod)
          node-selector (.getNodeSelector pod-spec)]
      (is (contains? node-selector api/cook-pool-label))
      (is (= pool-name (get node-selector api/cook-pool-label)))))

  (testing "node selector for hostname"
    (let [hostname "test-host"
          task-metadata {:container {:docker {:parameters [{:key "user"
                                                            :value "100:10"}]}}
                         :hostname hostname
                         :task-request {:scalar-requests {"mem" 512
                                                          "cpus" 1.0}}}
          ^V1Pod pod (api/task-metadata->pod nil nil task-metadata)
          ^V1PodSpec pod-spec (.getSpec pod)
          node-selector (.getNodeSelector pod-spec)]
      (is (contains? node-selector api/k8s-hostname-label))
      (is (= hostname (get node-selector api/k8s-hostname-label)))))

  (testing "cpu limit configurability"
    (let [task-metadata {:container {:docker {:parameters [{:key "user"
                                                            :value "100:10"}]}}
                         :task-request {:scalar-requests {"mem" 512
                                                          "cpus" 1.0}}}
          pod->cpu-limit-fn (fn [^V1Pod pod]
                              (let [^V1Container container (-> pod .getSpec .getContainers first)
                                    ^V1ResourceRequirements resources (-> container .getResources)]
                                (-> resources .getLimits (get "cpu"))))]

      (with-redefs [config/kubernetes (constantly {:set-container-cpu-limit? true})]
        (let [^V1Pod pod (api/task-metadata->pod nil nil task-metadata)]
          (is (= 1.0 (-> pod pod->cpu-limit-fn .getNumber .doubleValue)))))

      (with-redefs [config/kubernetes (constantly {:set-container-cpu-limit? false})]
        (let [^V1Pod pod (api/task-metadata->pod nil nil task-metadata)]
          (is (nil? (pod->cpu-limit-fn pod)))))

      (with-redefs [config/kubernetes (constantly {})]
        (let [^V1Pod pod (api/task-metadata->pod nil nil task-metadata)]
          (is (nil? (pod->cpu-limit-fn pod)))))))
  (testing "checkpointing volumes"
    (with-redefs [config/kubernetes (constantly {:default-checkpoint-config {:volume-name "cook-checkpointing-tools-volume"
                                                                             :init-container-volume-mounts [{:path "/abc/xyz"}]
                                                                             :main-container-volume-mounts [{:path "/abc/xyz"}
                                                                                                            {:path "/qed/bbq"
                                                                                                             :sub-path "efg/hij"}]}
                                                 :init-container {:command ["init container command"]
                                                                  :image "init container image"}})]
      (let [task-metadata {:container {:docker {:parameters [{:key "user"
                                                              :value "100:10"}]}}
                           :task-request {:scalar-requests {"mem" 512
                                                            "cpus" 1.0}
                                          :job {:job/checkpoint {:checkpoint/mode "auto"}}}}
            ^V1Pod pod (api/task-metadata->pod nil nil task-metadata)
            ^V1Container init-container (-> pod .getSpec .getInitContainers first)
            ^V1Container main-container (-> pod .getSpec .getContainers (->> (filter #(= (.getName %) api/cook-container-name-for-job))) first)
            init-container-paths (into #{} (-> init-container .getVolumeMounts
                                                              (->> (filter #(= (.getName %) "cook-checkpointing-tools-volume")))
                                                              (->> (map #(str (.getMountPath %) (.getSubPath %))))))
            main-container-paths (into #{} (-> main-container .getVolumeMounts
                                                              (->> (filter #(= (.getName %) "cook-checkpointing-tools-volume")))
                                                              (->> (map #(str (.getMountPath %) (.getSubPath %))))))]
        (is (= #{"/abc/xyz"} init-container-paths))
        (is (= #{"/abc/xyz" "/qed/bbqefg/hij"} main-container-paths)))))
  (testing "gpu task-metadata"
    (let [task-metadata {:task-id "my-task"
                         :command {:value "foo && bar"
                                   :environment {"FOO" "BAR"}
                                   :user (System/getProperty "user.name")}
                         :container {:type :docker
                                     :docker {:image "alpine:latest"}}
                         :task-request {:resources {:mem 576
                                                    :cpus 1.1
                                                    :gpus 2}
                                        :scalar-requests {"mem" 512
                                                          "cpus" 1.0}
                                        :job {:job/environment #{{:environment/name "COOK_GPU_MODEL"
                                                                  :environment/value "nvidia-tesla-p100"}}}}
                         :hostname "kubehost"}
          ^V1Pod pod (api/task-metadata->pod "cook" "testing-cluster" task-metadata)
          ^V1PodSpec pod-spec (.getSpec pod)
          ^V1Container container (-> pod-spec .getContainers first)]
      (is (= (-> pod-spec .getNodeSelector (get "cloud.google.com/gke-accelerator")) "nvidia-tesla-p100"))
      (is (= 2 (-> container .getResources .getRequests (get "nvidia.com/gpu") api/to-int)))
      (is (= 2 (-> container .getResources .getLimits (get "nvidia.com/gpu") api/to-int))))))

(defn- k8s-volume->clj [^V1Volume volume]
  {:name (.getName volume)
   :src (or (some-> volume .getHostPath .getPath)
            (when (.getEmptyDir volume)
              :empty-dir))})

(defn- k8s-mount->clj [^V1VolumeMount mount]
  {:name (.getName mount)
   :mount-path (.getMountPath mount)
   :read-only? (.getReadOnly mount)})

(defn- dummy-uuid-generator []
  (let [n (atom 0)]
    #(str "uuid-" (swap! n inc))))

(def sandbox-path "/mnt/sandbox")
(def legacy-sandbox-path "/mnt/mesos/sandbox")

(def expected-sandbox-volume
  {:name api/cook-sandbox-volume-name
   :src :empty-dir})

(def expected-sandbox-mount
  {:name api/cook-sandbox-volume-name
   :mount-path sandbox-path
   :read-only? false})
(def expected-legacy-sandbox-mount
  {:name api/cook-sandbox-volume-name
   :mount-path legacy-sandbox-path
   :read-only? false})

(deftest test-make-volumes
  (testing "empty cook volumes"
    (let [{:keys [volumes volume-mounts]} (api/make-volumes [] sandbox-path)
          volumes (map k8s-volume->clj volumes)
          volume-mounts (map k8s-mount->clj volume-mounts)]
      (is (= volumes [expected-sandbox-volume]))
      (is (= volume-mounts [expected-sandbox-mount expected-legacy-sandbox-mount]))))

  (testing "valid generated volume names"
    (let [host-path "/tmp/main/foo"
          {:keys [volumes volume-mounts]} (api/make-volumes [{:host-path host-path}] sandbox-path)]
      (let [volume (first volumes)
            volume-mount (first volume-mounts)]
        (is (= (.getName volume)
               (.getName volume-mount)))
        ; validation regex for k8s names
        (is (re-matches #"[a-z0-9]([-a-z0-9]*[a-z0-9])?" (.getName volume))))))

  (testing "validate minimal cook volume spec defaults"
    (with-redefs [d/squuid (dummy-uuid-generator)]
      (let [host-path "/tmp/main/foo"
            {:keys [volumes volume-mounts]} (api/make-volumes [{:host-path host-path}] sandbox-path)
          volumes (map k8s-volume->clj volumes)
          volume-mounts (map k8s-mount->clj volume-mounts)]
        (is (= volumes
               [{:name "syn-uuid-1" :src host-path}
                expected-sandbox-volume]))
        (is (= volume-mounts
               [{:name "syn-uuid-1" :mount-path host-path :read-only? true}
                expected-sandbox-mount expected-legacy-sandbox-mount])))))

  (testing "correct values for fully specified volume"
    (let [host-path "/tmp/foo"
          container-path "/mnt/foo"
          {:keys [volumes volume-mounts]} (api/make-volumes [{:host-path host-path
                                                              :container-path container-path
                                                              :mode "RW"}] container-path)
          [volume] volumes
          [volume-mount] volume-mounts]
      (is (= (.getName volume)
             (.getName volume-mount)))
      (is (= host-path (-> volume .getHostPath .getPath)))
      (is (not (.getReadOnly volume-mount)))
      (is (= container-path (.getMountPath volume-mount)))))

  (testing "disallows configured volumes"
    (with-redefs [config/kubernetes (constantly {:disallowed-container-paths #{"/tmp/foo"}})]
      (let [{:keys [volumes volume-mounts]} (api/make-volumes [{:host-path "/tmp/foo"}] "/tmp/unused")]
        (is (= 1 (count volumes)))
        (is (= 2 (count volume-mounts))))
      (let [{:keys [volumes volume-mounts]} (api/make-volumes [{:container-path "/tmp/foo"}] "/tmp/unused")]
        (is (= 1 (count volumes)))
        (is (= 2 (count volume-mounts)))))))


(deftest test-pod->synthesized-pod-state
  (testing "returns nil for empty pod"
    (is (nil? (api/pod->synthesized-pod-state nil))))

  (testing "no container status -> waiting"
    (let [pod (V1Pod.)
          pod-status (V1PodStatus.)
          pod-metadata (V1ObjectMeta.)]
      (.setStatus pod pod-status)
      (.setName pod-metadata "test-pod")
      (.setMetadata pod pod-metadata)
      (is (= {:state :pod/waiting
              :reason "Pending"}
             (api/pod->synthesized-pod-state pod)))))

  (testing "waiting"
    (let [pod (V1Pod.)
          pod-status (V1PodStatus.)
          pod-metadata (V1ObjectMeta.)
          container-status (V1ContainerStatus.)
          container-state (V1ContainerState.)
          waiting (V1ContainerStateWaiting.)]
      (.setReason waiting "waiting")
      (.setWaiting container-state waiting)
      (.setState container-status container-state)
      (.setName container-status "required-cook-job-container")
      (.setContainerStatuses pod-status [container-status])
      (.setStatus pod pod-status)
      (.setMetadata pod pod-metadata)
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
             (api/pod->synthesized-pod-state pod)))))

  (let [make-pod-with-condition-fn
        (fn [pod-condition-type pod-condition-status
             pod-condition-reason pod-condition-last-transition-time]
          (let [pod (V1Pod.)
                pod-status (V1PodStatus.)
                pod-metadata (V1ObjectMeta.)
                pod-condition (V1PodCondition.)]
            (.setType pod-condition pod-condition-type)
            (.setStatus pod-condition pod-condition-status)
            (.setReason pod-condition pod-condition-reason)
            (.setLastTransitionTime pod-condition pod-condition-last-transition-time)
            (.addConditionsItem pod-status pod-condition)
            (.setStatus pod pod-status)
            (.setName pod-metadata "test-pod")
            (.setMetadata pod pod-metadata)
            pod))

        make-unschedulable-pod-fn
        (fn [pod-condition-last-transition-time]
          (make-pod-with-condition-fn "PodScheduled" "False" "Unschedulable"
                                      pod-condition-last-transition-time))]

    (testing "unschedulable pod"
      (let [pod (make-unschedulable-pod-fn (t/epoch))]
        (with-redefs [config/kubernetes
                      (constantly {:pod-condition-unschedulable-seconds 60})]
          (is (= {:state :pod/failed
                  :reason "Unschedulable"}
                 (api/pod->synthesized-pod-state pod))))))

    (testing "briefly unschedulable pod"
      (let [pod (make-unschedulable-pod-fn (t/now))]
        (with-redefs [config/kubernetes
                      (constantly {:pod-condition-unschedulable-seconds 60})]
          (is (= {:state :pod/waiting
                  :reason "Pending"}
                 (api/pod->synthesized-pod-state pod))))))

    (testing "containers not initialized"
      (let [pod (make-pod-with-condition-fn "Initialized" "False" "ContainersNotInitialized" (t/epoch))
            container-status (V1ContainerStatus.)
            container-state (V1ContainerState.)
            waiting (V1ContainerStateWaiting.)
            pod-status (.getStatus pod)]
        (.setReason waiting "waiting")
        (.setWaiting container-state waiting)
        (.setState container-status container-state)
        (.setName container-status "required-cook-job-container")
        (.setContainerStatuses pod-status [container-status])
        (with-redefs [config/kubernetes
                      (constantly {:pod-condition-containers-not-initialized-seconds 60})]
          (is (= {:state :pod/failed
                  :reason "ContainersNotInitialized"}
                 (api/pod->synthesized-pod-state pod))))))

    (testing "node preempted default label"
      (let [pod (V1Pod.)
            pod-metadata (V1ObjectMeta.)
            pod-status (V1PodStatus.)]
        (.setStatus pod pod-status)
        (.setMetadata pod pod-metadata)
        (.setLabels pod-metadata {"node-preempted" 1589084484537})
        (is (= {:reason "Pending"
                :state :pod/waiting
                :pod-preempted-timestamp 1589084484537}
               (api/pod->synthesized-pod-state pod)))))
    (testing "node preempted custom label"
      (with-redefs [config/kubernetes (fn [] {:node-preempted-label "custom-node-preempted"})]
        (let [pod (V1Pod.)
              pod-metadata (V1ObjectMeta.)
              pod-status (V1PodStatus.)]
          (.setStatus pod pod-status)
          (.setMetadata pod pod-metadata)
          (.setLabels pod-metadata {"custom-node-preempted" 1589084484537})
          (is (= {:reason "Pending"
                  :state :pod/waiting
                  :pod-preempted-timestamp 1589084484537}
                 (api/pod->synthesized-pod-state pod))))))))

(deftest test-node-schedulable
  ;; TODO: Need the 'stuck pod scanner' to detect stuck states and move them into killed.
  (with-redefs [api/num-pods-on-node (constantly 1)]
    (testing "Blocklist-labels"
      (let [^V1Node node (V1Node.)
            metadata (V1ObjectMeta.)
            ^V1NodeSpec spec (V1NodeSpec.)]
        (.setLabels metadata {"blocklist-1" "val-1"})
        (.setName metadata "NodeName")
        (.setNamespace metadata "cook")
        (.setMetadata node metadata)
        (.setSpec node spec)
        (is (not (api/node-schedulable? node 30 nil ["blocklist-1"])))
        (is (api/node-schedulable? node 30 nil ["blocklist-2"]))))
    (testing "Pool Taint"
      (let [^V1Node node (V1Node.)
            metadata (V1ObjectMeta.)
            ^V1NodeSpec spec (V1NodeSpec.)
            ^V1Taint taint (V1Taint.)]
        (.setKey taint api/cook-pool-taint)
        (.setValue taint "a-pool")
        (.setEffect taint "NoSchedule")
        (.addTaintsItem spec taint)

        (.setName metadata "NodeName")
        (.setNamespace metadata "cook")
        (.setMetadata node metadata)
        (.setSpec node spec)
        (is (api/node-schedulable? node 30 nil ["blocklist-1"]))))
    (testing "Other Taint"
      (let [^V1Node node (V1Node.)
            metadata (V1ObjectMeta.)
            ^V1NodeSpec spec (V1NodeSpec.)
            ^V1Taint taint (V1Taint.)]
        (.setKey taint "othertaint")
        (.setValue taint "othertaintvalue")
        (.setEffect taint "NoSchedule")
        (.addTaintsItem spec taint)

        (.setName metadata "NodeName")
        (.setNamespace metadata "cook")
        (.setMetadata node metadata)
        (.setSpec node spec)
        (is (not (api/node-schedulable? node 30 nil ["blocklist-1"])))))
    (testing "GPU Taint"
      (let [^V1Node node (V1Node.)
            metadata (V1ObjectMeta.)
            ^V1NodeSpec spec (V1NodeSpec.)
            ^V1Taint taint (V1Taint.)]
        (.setKey taint "nvidia.com/gpu")
        (.setValue taint "present")
        (.setEffect taint "NoSchedule")
        (.addTaintsItem spec taint)

        (.setName metadata "NodeName")
        (.setNamespace metadata "cook")
        (.setMetadata node metadata)
        (.setSpec node spec)
        (is (api/node-schedulable? node 30 nil ["blocklist-1"]))))))

(deftest test-initialize-pod-watch-helper
  (testing "only processes each pod once"
    (let [pod-list-metadata (V1ListMeta.)
          pod-list (doto (V1PodList.) (.setMetadata pod-list-metadata))
          namespaced-pod-names-visited (atom [])
          callback-fn (fn [namespaced-pod-name _ _]
                        (swap! namespaced-pod-names-visited conj namespaced-pod-name))
          compute-cluster-name "test-compute-cluster"
          pod-metadata (doto (V1ObjectMeta.) (.setLabels {api/cook-pod-label compute-cluster-name}))
          pod (doto (V1Pod.) (.setMetadata pod-metadata))
          namespaced-pod-name {:namespace "foo" :name "bar"}
          all-pods-atom (atom {namespaced-pod-name pod})]
      (with-redefs [api/get-all-pods-in-kubernetes (constantly [pod-list {namespaced-pod-name pod}])
                    api/create-pod-watch (constantly nil)]
        (api/initialize-pod-watch-helper {:name compute-cluster-name :all-pods-atom all-pods-atom :node-name->pod-name->pod (atom {})} callback-fn))
      (is (= 1 (count @namespaced-pod-names-visited)))
      (is (= [namespaced-pod-name] @namespaced-pod-names-visited)))))

(deftest test-adjust-job-resources
  (testing "No change"
    (let [initial-resources {:mem 123
                             :cpu 456}
          job {}
          adjusted-resources (api/adjust-job-resources job initial-resources)]
      (is (= initial-resources adjusted-resources))))
  (with-redefs [config/kubernetes (fn [] {:sidecar {:resource-requirements {:cpu-request 0.6
                                                                            :memory-request 128}}})]
    (testing "Add resources for sidecar"
      (let [initial-resources {:mem 123
                               :cpus 456.1}
            expected-resources {:mem 251.0
                                :cpus 456.7}
            job {}
            adjusted-resources (api/adjust-job-resources job initial-resources)]
        (is (= expected-resources adjusted-resources)))))
  (with-redefs [config/kubernetes (fn [] {:default-checkpoint-config {:memory-overhead 129}})]
    (testing "Add resources for checkpointing"
      (let [initial-resources {:mem 123
                               :cpu 456.1}
            expected-resources {:mem 252.0
                                :cpu 456.1}
            job {:job/checkpoint {}}
            adjusted-resources (api/adjust-job-resources job initial-resources)]
        (is (= expected-resources adjusted-resources)))))
  (with-redefs [config/kubernetes (fn [] {:sidecar {:resource-requirements {:cpu-request 0.6
                                                                            :memory-request 128}}
                                          :default-checkpoint-config {:memory-overhead 129}})]
    (testing "Add resources for sidecar and checkpointing"
      (let [initial-resources {:mem 123
                               :cpus 456.1}
            expected-resources {:mem 380.0
                                :cpus 456.7}
            job {:job/checkpoint {}}
            adjusted-resources (api/adjust-job-resources job initial-resources)]
        (is (= expected-resources adjusted-resources))))))

(deftest test-calculate-effective-checkpointing-config
  (testing "No checkpoint"
    (with-redefs [config/kubernetes (fn [] {})]
      (let [job {}]
        (is (= nil (api/calculate-effective-checkpointing-config job 1))))))
  (testing "No change"
    (with-redefs [config/kubernetes (fn [] {})]
      (let [checkpoint {:mode "auto"}
            job {:job/checkpoint {:checkpoint/mode "auto"}}]
        (is (= checkpoint (api/calculate-effective-checkpointing-config job 1))))))
  (testing "Checkpoint and add defaults"
    (with-redefs [config/kubernetes (fn [] {:default-checkpoint-config {:memory-overhead 129}})]
      (let [checkpoint {:mode "auto"
                        :memory-overhead 129}
            job {:job/checkpoint {:checkpoint/mode "auto"}}]
        (is (= checkpoint (api/calculate-effective-checkpointing-config job 1))))))
  (testing "checkpoint when max attempts not set"
    (with-redefs [config/kubernetes (fn [] {:default-checkpoint-config {}})]
      (let [job {:job/checkpoint {:checkpoint/mode "auto"}
                 :job/instance [{:instance/reason {:reason/name :mesos-unknown}}
                                {:instance/reason {:reason/name :mesos-unknown}}
                                {:instance/reason {:reason/name :mesos-unknown}}]}]
        (is (= {:mode "auto"} (api/calculate-effective-checkpointing-config job 1))))))
  (testing "Don't checkpoint when max attempts exceeded"
    (with-redefs [config/kubernetes (fn [] {:default-checkpoint-config {:max-checkpoint-attempts 3}})]
      (let [job {:job/checkpoint {:checkpoint/mode "auto"}
                 :job/instance [{:instance/reason {:reason/name :mesos-unknown}}
                                {:instance/reason {:reason/name :mesos-unknown}}
                                {:instance/reason {:reason/name :mesos-unknown}}]}]
        (is (= nil (api/calculate-effective-checkpointing-config job 1)))))))

(deftest test-pod->sandbox-file-server-container-state
  (testing "file server not running"
    (let [pod (V1Pod.)
          pod-status (V1PodStatus.)
          container-status (V1ContainerStatus.)]
      (.setName container-status api/cook-container-name-for-file-server)
      (.addContainerStatusesItem pod-status container-status)
      (.setStatus pod pod-status)
      (= :not-running (api/pod->sandbox-file-server-container-state pod)))))
