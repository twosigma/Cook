(ns cook.test.kubernetes.compute-cluster
  (:require [clojure.test :refer :all]
            [cook.cached-queries :as cached-queries]
            [cook.compute-cluster :as cc]
            [cook.config :as config]
            [cook.kubernetes.api :as api]
            [cook.kubernetes.compute-cluster :as kcc]
            [cook.kubernetes.controller :as controller]
            [cook.mesos.task :as task]
            [cook.test.postgres]
            [cook.scheduler.scheduler :as sched]
            [cook.test.testutil :as tu]
            [datomic.api :as d])
  (:import (clojure.lang ExceptionInfo)
           (com.twosigma.cook.kubernetes ParallelWatchQueue)
           (io.kubernetes.client.openapi.models V1NodeSelectorRequirement V1Pod V1PodSecurityContext)
           (java.util.concurrent Executors)
           (java.util.concurrent.locks ReentrantLock ReentrantReadWriteLock)
           (java.util UUID)))

(use-fixtures :once cook.test.postgres/with-pg-db)

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

(deftest test-get-pods-in-pool
  (let [in
        {:pool->node-name->node (atom {:pool-a {:node-1 1 :node-2 2}
                                       :pool-b {:node-3 3}
                                       :pool-c {:node-4 4}
                                       :pool-d {:node-5 5}
                                       :pool-e {}})
         :node-name->pod-name->pod (atom {:node-1 {:pod-1a 1 :pod-1b 2}
                                          :node-2 {:pod-2a 2}
                                          :node-3 {}
                                          :node-4 {:pod-4a 1 :pod-4b 2}})}]
    (is (= {:pod-1a 1 :pod-1b 2 :pod-2a 2} (kcc/get-pods-in-pool in :pool-a)))
    (is (= {} (kcc/get-pods-in-pool in :pool-b)))
    (is (= {:pod-4a 1 :pod-4b 2} (kcc/get-pods-in-pool in :pool-c)))
    (is (= {} (kcc/get-pods-in-pool in :pool-d)))
    (is (= {} (kcc/get-pods-in-pool in :pool-e)))
    (is (= {} (kcc/get-pods-in-pool in :pool-f)))))

(deftest test-get-name->node-for-pool
  (let [in (atom {:pool-a {:node-1 1 :node-2 2}
                  :pool-b {}})]
    (is (= {:node-1 1 :node-2 2} (kcc/get-name->node-for-pool @in :pool-a)))
    (is (= {} (kcc/get-name->node-for-pool @in :pool-b)))
    (is (= {} (kcc/get-name->node-for-pool @in nil)))))

(deftest test-namespace-config
  (tu/setup)
  (let [conn (tu/restore-fresh-database! "datomic:mem://test-namespace-config")
        task-assignment-result-helper (fn [user]
                                        (let [job-id (tu/create-dummy-job conn :user user)
                                              job-ent (d/entity (d/db conn) job-id)]
                                          (-> job-ent
                                              tu/make-task-request
                                              tu/make-task-assignment-result)))
        launched-pod-atom (atom nil)]
    (with-redefs [api/launch-pod (fn [_ _ {:keys [launch-pod]} _]
                                   (reset! launched-pod-atom launch-pod))
                  api/make-security-context (constantly (V1PodSecurityContext.))
                  cook.config-incremental/get-conn (fn [] conn)]
      (testing "static namespace"
        (let [compute-cluster (kcc/->KubernetesComputeCluster nil "kubecompute" nil nil
                                                              (atom {}) (atom {}) (atom {}) (atom {}) (atom {}) (atom {}) (atom {}) (atom nil)
                                                              {:kind :static :namespace "cook"} nil nil nil nil
                                                              (Executors/newSingleThreadExecutor)
                                                              {} (atom :running) (atom false) false
                                                              cook.rate-limit/AllowAllRateLimiter "t-a" "p-a" "t2-a" "p2-a" "l-p" "l-v1"
                                                              (repeatedly 16 #(ReentrantLock.))
                                                              (ReentrantReadWriteLock. true)
                                                              (ParallelWatchQueue. (Executors/newSingleThreadExecutor) 1000 10))
              task-metadata (task/TaskAssignmentResult->task-metadata (d/db conn)
                                                                      nil
                                                                      compute-cluster
                                                                      (task-assignment-result-helper "testuser"))]

          (cc/launch-tasks compute-cluster "test-pool" [{:task-metadata-seq [task-metadata]}] (fn [_]))
          (with-redefs [d/db (constantly nil)
                        cached-queries/instance-uuid->job-uuid-datomic-query (constantly nil)]
            (is (= (str (get-in task-metadata [:task-request :job :job/uuid])) (cached-queries/instance-uuid->job-uuid-cache-lookup (:task-id task-metadata)))))
          (is (= "cook" (-> @launched-pod-atom
                            :pod
                            .getMetadata
                            .getNamespace)))))

      (testing "per-user namespace"
        (let [compute-cluster (kcc/->KubernetesComputeCluster nil "kubecompute" nil nil
                                                              (atom {}) (atom {}) (atom {}) (atom {}) (atom {}) (atom {}) (atom {}) (atom nil)
                                                              {:kind :per-user} nil nil nil nil
                                                              (Executors/newSingleThreadExecutor)
                                                              {} (atom :running) (atom false) false
                                                              cook.rate-limit/AllowAllRateLimiter "t-b" "p-b" "t2-a" "p2-a" "l-p" "l-v2"
                                                              (repeatedly 16 #(ReentrantLock.))
                                                              (ReentrantReadWriteLock. true)
                                                              (ParallelWatchQueue. (Executors/newSingleThreadExecutor) 1000 10))
              task-metadata (task/TaskAssignmentResult->task-metadata (d/db conn)
                                                                      nil
                                                                      compute-cluster
                                                                      (task-assignment-result-helper "testuser"))]
          (cc/launch-tasks compute-cluster "test-pool" [{:task-metadata-seq [task-metadata]}] (fn [_]))
          (is (= "testuser" (-> @launched-pod-atom
                                :pod
                                .getMetadata
                                .getNamespace))))))))

(deftest test-generate-offers
  (tu/setup)
  (let [conn (tu/restore-fresh-database! "datomic:mem://test-generate-offers")]
    (with-redefs [api/launch-pod (constantly true)
                  config/disk (constantly [{:pool-regex "test-pool"
                                            :max-size 256000.0
                                            :valid-types #{"standard", "pd-ssd"}
                                            :default-type "standard"
                                            :default-request 10000.0
                                            :type-map {"standard", "pd-standard"}
                                            :enable-constraint? true
                                            :disk-node-label "cloud.google.com/gke-boot-disk"}])
                  cook.config-incremental/get-conn (fn [] conn)]
      (let [compute-cluster (kcc/->KubernetesComputeCluster nil "kubecompute" nil nil
                                                            (atom {}) (atom {}) (atom {}) (atom {}) (atom {}) (atom {}) (atom {}) (atom nil)
                                                            {:kind :static :namespace "cook"} nil 3 nil nil
                                                            (Executors/newSingleThreadExecutor)
                                                            {} (atom :running) (atom false) false
                                                            cook.rate-limit/AllowAllRateLimiter "t-c" "p-c" "t2-a" "p2-a" "l-p" "l-c2"
                                                            (repeatedly 16 #(ReentrantLock.))
                                                            (ReentrantReadWriteLock. true)
                                                            (ParallelWatchQueue. (Executors/newSingleThreadExecutor) 1000 10))
            node-name->node {"nodeA" (tu/node-helper "nodeA" 1.0 1000.0 10 "nvidia-tesla-p100" nil nil)
                             "nodeB" (tu/node-helper "nodeB" 1.0 1000.0 25 "nvidia-tesla-p100" nil nil)
                             "nodeC" (tu/node-helper "nodeC" 1.0 1000.0 nil nil nil nil)
                             "nodeE" (tu/node-helper "nodeE" 2.0 1100.0 nil nil {:disk-amount 256000 :disk-type "pd-standard"} nil)
                             "my.fake.host" (tu/node-helper "my.fake.host" 1.0 1000.0 nil nil nil nil)}
            _ (tu/create-pool conn "no-pool")
            j1 (tu/create-dummy-job conn :ncpus 0.1 :pool "no-pool")
            j2 (tu/create-dummy-job conn :ncpus 0.2 :pool "no-pool")
            db (d/db conn)
            job-ent-1 (d/entity db j1)
            job-ent-2 (d/entity db j2)
            task-1 (tu/make-task-metadata job-ent-1 db compute-cluster)
            task-2 (tu/make-task-metadata job-ent-2 db compute-cluster)
            _ (cc/launch-tasks compute-cluster "no-pool" [{:task-metadata-seq [task-1 task-2]}] (fn [_]))
            task-1-id (-> task-1 :task-request :task-id)
            pods {{:namespace "cook" :name "podA"} (tu/pod-helper "podA" "nodeA"
                                                                  {:cpus 0.25 :mem 250.0 :gpus "9" :gpu-model "nvidia-tesla-p100"}
                                                                  {:cpus 0.1 :mem 100.0})
                  {:namespace "cook" :name "podB"} (tu/pod-helper "podB" "nodeA"
                                                                  {:cpus 0.25 :mem 250.0 :gpus "1" :gpu-model "nvidia-tesla-p100"})
                  {:namespace "cook" :name "podC"} (tu/pod-helper "podC" "nodeB"
                                                                  {:cpus 1.0 :mem 1100.0 :gpus "10" :gpu-model "nvidia-tesla-p100"})
                  {:namespace "cook" :name "podD"} (tu/pod-helper "podD" "nodeD"
                                                                  {:cpus 1.0 :mem 1100.0 :gpus "10" :gpu-model "nvidia-tesla-p100"})
                  {:namespace "cook" :name "podE"} (tu/pod-helper "podE" "nodeE"
                                                                  {:cpus 1.0 :mem 1100.0 :disk {:disk-request 50 :disk-limit 70 :disk-type "pd-standard"}})
                  {:namespace "cook" :name task-1-id} (tu/pod-helper task-1-id "my.fake.host"
                                                                     {:cpus 0.1 :mem 10.0})}
            node-name->pods (api/pods->node-name->pods (kcc/add-starting-pods compute-cluster pods))
            offers (kcc/generate-offers compute-cluster node-name->node node-name->pods "test-pool")]
        (is (= 5 (count offers)))
        (let [offer (first (filter #(= "nodeA" (:hostname %))
                                   offers))]
          (is (not (nil? offer)))
          (is (= "kubecompute" (:framework-id offer)))
          (is (= {:value "nodeA"} (:slave-id offer)))
          (is (= [{:name "mem" :type :value-scalar :scalar 400.0}
                  {:name "cpus" :type :value-scalar :scalar 0.4}
                  {:name "disk" :type :value-text->scalar :text->scalar {}}
                  {:name "gpus" :type :value-text->scalar :text->scalar {"nvidia-tesla-p100" 0}}]
                 (:resources offer)))
          (is (:reject-after-match-attempt offer)))

        (let [offer (first (filter #(= "nodeB" (:hostname %))
                                   offers))]
          (is (= {:value "nodeB"} (:slave-id offer)))
          (is (= [{:name "mem" :type :value-scalar :scalar 0.0}
                  {:name "cpus" :type :value-scalar :scalar 0.0}
                  {:name "disk" :type :value-text->scalar :text->scalar {}}
                  {:name "gpus" :type :value-text->scalar :text->scalar {"nvidia-tesla-p100" 15}}]
                 (:resources offer))))

        (let [offer (first (filter #(= "my.fake.host" (:hostname %)) offers))]
          (is (= [{:name "mem" :type :value-scalar :scalar 980.0}
                  {:name "cpus" :type :value-scalar :scalar 0.7}
                  {:name "disk" :type :value-text->scalar :text->scalar {}}
                  {:name "gpus" :type :value-text->scalar :text->scalar {}}]
                 (:resources offer))))

        (let [offer (first (filter #(= "nodeE" (:hostname %))
                                   offers))]
          (is (= {:value "nodeE"} (:slave-id offer)))
          (is (= [{:name "mem" :type :value-scalar :scalar 0.0}
                  {:name "cpus" :type :value-scalar :scalar 1.0}
                  {:name "disk" :type :value-text->scalar :text->scalar {"pd-standard" 255950.0}}
                  {:name "gpus" :type :value-text->scalar :text->scalar {}}]
                 (:resources offer))))

        (let [entire-node-a-capacity [{:name "mem"
                                       :type :value-scalar
                                       :scalar 1000.0}
                                      {:name "cpus"
                                       :type :value-scalar
                                       :scalar 1.0}
                                      {:name "disk"
                                       :type :value-text->scalar
                                       :text->scalar {}}
                                      {:name "gpus"
                                       :type :value-text->scalar
                                       :text->scalar {"nvidia-tesla-p100" 10}}]]
          (testing "graceful handling of node with empty pod list"
            (let [offers
                  (kcc/generate-offers
                    compute-cluster
                    node-name->node
                    (assoc node-name->pods "nodeA" [])
                    "test-pool")
                  offer (first (filter #(= "nodeA" (:hostname %)) offers))]
              (is offer)
              (is (= entire-node-a-capacity (:resources offer)))))

          (testing "graceful handling of node with nil pod list"
            (let [offers
                  (kcc/generate-offers
                    compute-cluster
                    node-name->node
                    (assoc node-name->pods "nodeA" nil)
                    "test-pool")
                  offer (first (filter #(= "nodeA" (:hostname %)) offers))]
              (is offer)
              (is (= entire-node-a-capacity (:resources offer)))))

          (testing "graceful handling of node with pods with no resource requests"
            (let [offers
                  (kcc/generate-offers
                    compute-cluster
                    node-name->node
                    (assoc node-name->pods "nodeA" [(V1Pod.) (V1Pod.)])
                    "test-pool")
                  offer (first (filter #(= "nodeA" (:hostname %)) offers))]
              (is offer)
              (is (= entire-node-a-capacity (:resources offer))))))))))

(deftest determine-cook-expected-state
  ; TODO
  )

(deftest test-autoscale!
  (tu/setup)
  (let [pool-name "test-pool"
        make-job-fn (fn [job-uuid user]
                      {:job/resource [{:resource/type :cpus, :resource/amount 0.1}
                                      {:resource/type :mem, :resource/amount 32}]
                       :job/user user
                       :job/uuid job-uuid
                       :job/pool pool-name})]
    (testing "synthetic pods basics"
      (let [job-uuid-1 (str (UUID/randomUUID))
            job-uuid-2 (str (UUID/randomUUID))
            job-uuid-3 (str (UUID/randomUUID))
            ^V1Pod outstanding-synthetic-pod-1 (tu/synthetic-pod-helper job-uuid-1 pool-name nil)
            compute-cluster (tu/make-kubernetes-compute-cluster {nil outstanding-synthetic-pod-1}
                                                                #{pool-name} nil {:user "user"})
            pending-jobs [(make-job-fn job-uuid-1 nil)
                          (make-job-fn job-uuid-2 nil)
                          (make-job-fn job-uuid-3 nil)]
            launched-pods-atom (atom [])]
        (with-redefs [api/launch-pod (fn [_ _ cook-expected-state-dict _]
                                       (swap! launched-pods-atom conj cook-expected-state-dict))
                      kcc/get-pods-in-pool (constantly {{:namespace nil :name job-uuid-1} outstanding-synthetic-pod-1})]
          (cc/autoscale! compute-cluster pool-name pending-jobs sched/adjust-job-resources-for-pool-fn))
        (is (= 2 (count @launched-pods-atom)))
        (is (= job-uuid-2 (-> @launched-pods-atom (nth 0) :launch-pod :pod kcc/synthetic-pod->job-uuid)))
        (is (= job-uuid-3 (-> @launched-pods-atom (nth 1) :launch-pod :pod kcc/synthetic-pod->job-uuid)))))

    (testing "synthetic pod max pod limit"
      (let [job-uuid-1 (str (UUID/randomUUID))
            job-uuid-2 (str (UUID/randomUUID))
            job-uuid-3 (str (UUID/randomUUID))
            ^V1Pod outstanding-synthetic-pod-1 (tu/synthetic-pod-helper job-uuid-1 pool-name nil)
            compute-cluster (tu/make-kubernetes-compute-cluster {nil outstanding-synthetic-pod-1}
                                                                #{pool-name} nil {:max-total-pods 2 :user "user"})
            pending-jobs [(make-job-fn job-uuid-1 nil)
                          (make-job-fn job-uuid-2 nil)
                          (make-job-fn job-uuid-3 nil)]
            launched-pods-atom (atom [])]
        (with-redefs [api/launch-pod (fn [_ _ cook-expected-state-dict _]
                                       (swap! launched-pods-atom conj cook-expected-state-dict))
                      kcc/get-pods-in-pool (constantly {{:namespace nil :name job-uuid-1} outstanding-synthetic-pod-1})]
          (cc/autoscale! compute-cluster pool-name pending-jobs sched/adjust-job-resources-for-pool-fn))
        ; We have one running and a limit of 2, so should have a max of 1 launch.
        (is (= 1 (count @launched-pods-atom)))
        (is (= job-uuid-2 (-> @launched-pods-atom (nth 0) :launch-pod :pod kcc/synthetic-pod->job-uuid)))))

    (testing "synthetic pods use the user's namespace"
      (let [job-uuid-1 (str (UUID/randomUUID))
            job-uuid-2 (str (UUID/randomUUID))
            pool-name "test-pool"
            compute-cluster (tu/make-kubernetes-compute-cluster {} #{pool-name} nil {})
            pending-jobs [(make-job-fn job-uuid-1 "user-1")
                          (make-job-fn job-uuid-2 "user-2")]
            launched-pods-atom (atom [])]
        (with-redefs [api/launch-pod (fn [_ _ cook-expected-state-dict _]
                                       (swap! launched-pods-atom conj cook-expected-state-dict))]
          (cc/autoscale! compute-cluster pool-name pending-jobs sched/adjust-job-resources-for-pool-fn))
        (is (= 2 (count @launched-pods-atom)))
        (is (= "user-1" (-> @launched-pods-atom (nth 0) :launch-pod :pod .getMetadata .getNamespace)))
        (is (= "user-2" (-> @launched-pods-atom (nth 1) :launch-pod :pod .getMetadata .getNamespace)))))

    (testing "synthetic pods avoid job's previous hosts"
      (let [job-uuid-1 (str (UUID/randomUUID))
            pool-name "test-pool"
            compute-cluster (tu/make-kubernetes-compute-cluster {} #{pool-name} nil {})
            pending-jobs [(-> (make-job-fn job-uuid-1 "user-1")
                              (assoc :job/instance
                                     [{:instance/hostname "test-host-1"}
                                      {:instance/hostname "test-host-2"}]))]
            launched-pods-atom (atom [])]
        (with-redefs [api/launch-pod (fn [_ _ cook-expected-state-dict _]
                                       (swap! launched-pods-atom conj cook-expected-state-dict))]
          (cc/autoscale! compute-cluster pool-name pending-jobs sched/adjust-job-resources-for-pool-fn))
        (is (= 1 (count @launched-pods-atom)))
        (let [^V1NodeSelectorRequirement node-selector-requirement
              (-> @launched-pods-atom
                  (nth 0)
                  :launch-pod
                  :pod
                  .getSpec
                  .getAffinity
                  .getNodeAffinity
                  .getRequiredDuringSchedulingIgnoredDuringExecution
                  .getNodeSelectorTerms
                  first
                  .getMatchExpressions
                  first)]
          (is (= api/k8s-hostname-label (.getKey node-selector-requirement)))
          (is (= "NotIn" (.getOperator node-selector-requirement)))
          (is (= #{"test-host-1" "test-host-2"} (set (.getValues node-selector-requirement)))))))

    (testing "synthetic pods have safe-to-evict annotation"
      (let [job-uuid-1 (str (UUID/randomUUID))
            pool-name "test-pool"
            compute-cluster (tu/make-kubernetes-compute-cluster {} #{pool-name} nil {})
            pending-jobs [(make-job-fn job-uuid-1 "user-1")]
            launched-pods-atom (atom [])]
        (with-redefs [api/launch-pod (fn [_ _ cook-expected-state-dict _]
                                       (swap! launched-pods-atom conj cook-expected-state-dict))]
          (cc/autoscale! compute-cluster pool-name pending-jobs sched/adjust-job-resources-for-pool-fn))
        (is (= 1 (count @launched-pods-atom)))
        (is (= "true"
               (-> @launched-pods-atom
                   (nth 0)
                   :launch-pod
                   :pod
                   .getMetadata
                   .getAnnotations
                   (get api/k8s-safe-to-evict-annotation))))))

    (testing "synthetic pods with gpus"
      (let [job-uuid-1 (str (UUID/randomUUID))
            pool-name "test-pool"
            compute-cluster (tu/make-kubernetes-compute-cluster {} #{pool-name} nil nil)
            pending-jobs [(-> (make-job-fn job-uuid-1 "user-1")
                              (assoc :job/environment
                                     #{{:environment/name "COOK_GPU_MODEL"
                                        :environment/value "nvidia-tesla-p100"}}
                                     :job/resource [{:resource/type :cpus, :resource/amount 0.1}
                                                    {:resource/type :mem, :resource/amount 32}
                                                    {:resource/type :gpus, :resource/amount 1}]))]
            launched-pods-atom (atom [])]
        (with-redefs [api/launch-pod (fn [_ _ cook-expected-state-dict _]
                                       (swap! launched-pods-atom conj cook-expected-state-dict))]
          (cc/autoscale! compute-cluster pool-name pending-jobs sched/adjust-job-resources-for-pool-fn))
        (is (= 1 (count @launched-pods-atom)))
        (is (= job-uuid-1 (-> @launched-pods-atom (nth 0) :launch-pod :pod kcc/synthetic-pod->job-uuid)))
        (let [container-resources (-> @launched-pods-atom
                                      (nth 0)
                                      :launch-pod
                                      :pod
                                      .getSpec
                                      .getContainers
                                      first
                                      .getResources)]
        (is (= 1 (-> container-resources .getRequests (get "nvidia.com/gpu") (api/to-int))))
        (is (= 1 (-> container-resources .getLimits (get "nvidia.com/gpu") (api/to-int)))))))

    (testing "synthetic pod attribution labels"
      (let [job-uuid (str (UUID/randomUUID))
            pool-name "test-pool"
            compute-cluster (tu/make-kubernetes-compute-cluster {} #{pool-name} nil {})
            pending-jobs [(make-job-fn job-uuid "user-1")]
            launched-pods-atom (atom [])]
        (with-redefs [api/launch-pod
                      (fn [_ _ cook-expected-state-dict _]
                        (swap! launched-pods-atom conj cook-expected-state-dict))
                      config/kubernetes
                      (constantly {:add-job-label-to-pod-prefix "platform/"})]
          (cc/autoscale! compute-cluster pool-name pending-jobs sched/adjust-job-resources-for-pool-fn))
        (is (= 1 (count @launched-pods-atom)))
        (let [pod-labels (-> @launched-pods-atom (nth 0) :launch-pod :pod .getMetadata .getLabels)]
          (is (= "infrastructure" (get pod-labels "platform/application.workload-class")))
          (is (= "synthetic-pod" (get pod-labels "platform/application.workload-id"))))))))

(deftest test-factory-fn
  (testing "guards against inappropriate number of threads"
    (with-redefs [kcc/get-or-create-cluster-entity-id (constantly 1)
                  cc/register-compute-cluster! (constantly nil)
                  config/kubernetes (constantly {:controller-lock-num-shards 5})]
      (is (kcc/factory-fn {:use-google-service-account? false} nil))
      (is (kcc/factory-fn {:controller-num-threads 1
                           :use-google-service-account? false}
                          nil))
      (is (kcc/factory-fn {:controller-num-threads 511
                           :use-google-service-account? false}
                          nil))
      (is (thrown? ExceptionInfo
                   (kcc/factory-fn {:controller-num-threads 0
                                    :use-google-service-account? false}
                                   nil)))
      (is (thrown? ExceptionInfo
                   (kcc/factory-fn {:controller-num-threads 512
                                    :use-google-service-account? false}
                                   nil))))))

(deftest test-total-resource
  (testing "gracefully handles missing resource"
    (= 3 (kcc/total-resource {"node-a" {:cpus 1} "node-b" {} "node-c" {:cpus 2}} :cpus))))

(deftest test-add-starting-pods
  (with-redefs [controller/starting-namespaced-pod-name->pod
                (constantly
                  {{:namespace "ns" :name "name1"} "pod1"
                   {:namespace "ns" :name "name2"} "pod2"})]
    (let [pods {{:namespace "ns" :name "name2"} "pod2b"
                {:namespace "ns" :name "name3"} "pod3"}]
      (is (= #{"pod1" "pod2" "pod3"} (into #{} (kcc/add-starting-pods nil pods))))
      (is (= #{"pod1" "pod2"} (into #{} (kcc/add-starting-pods nil {}))))))
  (with-redefs [controller/starting-namespaced-pod-name->pod (constantly {})]
    (let [pods {{:namespace "ns" :name "name2"} "pod2b"
                {:namespace "ns" :name "name3"} "pod3"}]
      (is (= #{"pod2b" "pod3"} (into #{} (kcc/add-starting-pods nil pods))))
      (is (= #{} (into #{} (kcc/add-starting-pods nil {})))))))


(deftest test-add-starting-pods-reverse
  (with-redefs [controller/starting-namespaced-pod-name->pod
                (constantly
                  {{:namespace "ns" :name "name1"} "pod1"
                   {:namespace "ns" :name "name2"} "pod2"})]
    (let [pods {{:namespace "ns" :name "name2"} "pod2b"
                {:namespace "ns" :name "name3"} "pod3"}]
      (is (= #{"pod1" "pod2b" "pod3"} (into #{} (kcc/add-starting-pods-reverse nil pods))))
      (is (= #{"pod1" "pod2"} (into #{} (kcc/add-starting-pods-reverse nil {}))))))
  (with-redefs [controller/starting-namespaced-pod-name->pod (constantly {})]
    (let [pods {{:namespace "ns" :name "name2"} "pod2b"
                {:namespace "ns" :name "name3"} "pod3"}]
      (is (= #{"pod2b" "pod3"} (into #{} (kcc/add-starting-pods-reverse nil pods))))
      (is (= #{} (into #{} (kcc/add-starting-pods-reverse nil {})))))))