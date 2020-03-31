(ns cook.test.kubernetes.compute-cluster
  (:require [clj-time.core :as t]
            [clojure.core.cache :as cache]
            [clojure.test :refer :all]
            [cook.compute-cluster :as cc]
            [cook.kubernetes.api :as api]
            [cook.kubernetes.compute-cluster :as kcc]
            [cook.kubernetes.controller :as controller]
            [cook.mesos.task :as task]
            [cook.scheduler.scheduler :as sched]
            [cook.test.testutil :as tu]
            [cook.tools :as util]
            [datomic.api :as d])
  (:import (com.netflix.fenzo SimpleAssignmentResult)
           (io.kubernetes.client.models V1NodeSelectorRequirement V1Pod V1PodSecurityContext)
           (java.util UUID)))

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
        task-assignment-result-helper (fn [user]
                                        (let [job-id (tu/create-dummy-job conn :user user)
                                              job-ent (d/entity (d/db conn) job-id)]
                                          (-> job-ent
                                              tu/make-task-request
                                              tu/make-task-assignment-result)))
        launched-pod-atom (atom nil)]
    (with-redefs [api/launch-pod (fn [_ _ {:keys [launch-pod]} _]
                                   (reset! launched-pod-atom launch-pod))
                  api/make-security-context (constantly (V1PodSecurityContext.))]
      (testing "static namespace"
        (let [compute-cluster (kcc/->KubernetesComputeCluster nil "kubecompute" nil nil nil
                                                              (atom {}) (atom {}) (atom {}) (atom {}) (atom nil)
                                                              {:kind :static :namespace "cook"} nil nil nil nil)
              task-metadata (task/TaskAssignmentResult->task-metadata (d/db conn)
                                                                      nil
                                                                      compute-cluster
                                                                      (task-assignment-result-helper "testuser"))]

          (cc/launch-tasks compute-cluster [] [task-metadata])
          (is (= "cook" (-> @launched-pod-atom
                            :pod
                            .getMetadata
                            .getNamespace)))))

      (testing "per-user namespace"
        (let [compute-cluster (kcc/->KubernetesComputeCluster nil "kubecompute" nil nil nil
                                                              (atom {}) (atom {}) (atom {}) (atom {}) (atom nil)
                                                              {:kind :per-user} nil nil nil nil)
              task-metadata (task/TaskAssignmentResult->task-metadata (d/db conn)
                                                                      nil
                                                                      compute-cluster
                                                                      (task-assignment-result-helper "testuser"))]
          (cc/launch-tasks compute-cluster [] [task-metadata])
          (is (= "testuser" (-> @launched-pod-atom
                                :pod
                                .getMetadata
                                .getNamespace))))))))

(deftest test-generate-offers
  (tu/setup)
  (with-redefs [api/launch-pod (constantly true)]
    (let [conn (tu/restore-fresh-database! "datomic:mem://test-generate-offers")
          compute-cluster (kcc/->KubernetesComputeCluster nil "kubecompute" nil nil nil
                                                          (atom {}) (atom {}) (atom {}) (atom {}) (atom nil)
                                                          {:kind :static :namespace "cook"} nil 3 nil nil)
          node-name->node {"nodeA" (tu/node-helper "nodeA" 1.0 1000.0 nil)
                           "nodeB" (tu/node-helper "nodeB" 1.0 1000.0 nil)
                           "nodeC" (tu/node-helper "nodeC" 1.0 1000.0 nil)
                           "my.fake.host" (tu/node-helper "my.fake.host" 1.0 1000.0 nil)}
          j1 (tu/create-dummy-job conn :ncpus 0.1)
          j2 (tu/create-dummy-job conn :ncpus 0.2)
          db (d/db conn)
          job-ent-1 (d/entity db j1)
          job-ent-2 (d/entity db j2)
          task-1 (tu/make-task-metadata job-ent-1 db compute-cluster)
          task-2 (tu/make-task-metadata job-ent-2 db compute-cluster)
          _ (cc/launch-tasks compute-cluster nil [task-1 task-2])
          task-1-id (-> task-1 :task-request :task-id)
          pod-name->pod {{:namespace "cook" :name "podA"} (tu/pod-helper "podA" "nodeA"
                                                                         {:cpus 0.25 :mem 250.0}
                                                                         {:cpus 0.1 :mem 100.0})
                         {:namespace "cook" :name "podB"} (tu/pod-helper "podB" "nodeA"
                                                                         {:cpus 0.25 :mem 250.0})
                         {:namespace "cook" :name "podC"} (tu/pod-helper "podC" "nodeB"
                                                                         {:cpus 1.0 :mem 1100.0})
                         {:namespace "cook" :name task-1-id} (tu/pod-helper task-1-id "my.fake.host"
                                                                            {:cpus 0.1 :mem 10.0})}
          all-offers (kcc/generate-offers compute-cluster node-name->node
                                          (kcc/add-starting-pods compute-cluster pod-name->pod))
          offers (get all-offers "no-pool")]
      (is (= 4 (count offers)))
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
               (:resources offer))))

      (let [offer (first (filter #(= "my.fake.host" (:hostname %)) offers))]
        (is (= [{:name "mem" :type :value-scalar :scalar 980.0}
                {:name "cpus" :type :value-scalar :scalar 0.7}
                {:name "disk" :type :value-scalar :scalar 0.0}]
               (:resources offer)))))))

(deftest determine-cook-expected-state
  ; TODO
  )

(deftest test-autoscale!
  (let [make-job-fn (fn [job-uuid user]
                      {:job/resource [{:resource/type :cpus, :resource/amount 0.1}
                                      {:resource/type :mem, :resource/amount 32}]
                       :job/user user
                       :job/uuid job-uuid})]

    (testing "synthetic pods basics"
      (let [job-uuid-1 (str (UUID/randomUUID))
            job-uuid-2 (str (UUID/randomUUID))
            job-uuid-3 (str (UUID/randomUUID))
            pool-name "test-pool"
            ^V1Pod outstanding-synthetic-pod-1 (tu/synthetic-pod-helper job-uuid-1 pool-name nil)
            compute-cluster (tu/make-kubernetes-compute-cluster {nil outstanding-synthetic-pod-1}
                                                                #{pool-name} "user" nil)
            pending-jobs [(make-job-fn job-uuid-1 nil)
                          (make-job-fn job-uuid-2 nil)
                          (make-job-fn job-uuid-3 nil)]
            launched-pods-atom (atom [])]
        (with-redefs [api/launch-pod (fn [_ _ cook-expected-state-dict _]
                                       (swap! launched-pods-atom conj cook-expected-state-dict))]
          (cc/autoscale! compute-cluster pool-name pending-jobs))
        (is (= 2 (count @launched-pods-atom)))
        (is (= job-uuid-2 (-> @launched-pods-atom (nth 0) :launch-pod :pod kcc/synthetic-pod->job-uuid)))
        (is (= job-uuid-3 (-> @launched-pods-atom (nth 1) :launch-pod :pod kcc/synthetic-pod->job-uuid)))))

    (testing "synthetic pods use the user's namespace"
      (let [job-uuid-1 (str (UUID/randomUUID))
            job-uuid-2 (str (UUID/randomUUID))
            pool-name "test-pool"
            compute-cluster (tu/make-kubernetes-compute-cluster {} #{pool-name} nil nil)
            pending-jobs [(make-job-fn job-uuid-1 "user-1")
                          (make-job-fn job-uuid-2 "user-2")]
            launched-pods-atom (atom [])]
        (with-redefs [api/launch-pod (fn [_ _ cook-expected-state-dict _]
                                       (swap! launched-pods-atom conj cook-expected-state-dict))]
          (cc/autoscale! compute-cluster pool-name pending-jobs))
        (is (= 2 (count @launched-pods-atom)))
        (is (= "user-1" (-> @launched-pods-atom (nth 0) :launch-pod :pod .getMetadata .getNamespace)))
        (is (= "user-2" (-> @launched-pods-atom (nth 1) :launch-pod :pod .getMetadata .getNamespace)))))

    (testing "synthetic pods avoid job's previous hosts"
      (let [job-uuid-1 (str (UUID/randomUUID))
            pool-name "test-pool"
            compute-cluster (tu/make-kubernetes-compute-cluster {} #{pool-name} nil nil)
            pending-jobs [(-> (make-job-fn job-uuid-1 "user-1")
                              (assoc :job/instance
                                     [{:instance/hostname "test-host-1"}
                                      {:instance/hostname "test-host-2"}]))]
            launched-pods-atom (atom [])]
        (with-redefs [api/launch-pod (fn [_ _ cook-expected-state-dict _]
                                       (swap! launched-pods-atom conj cook-expected-state-dict))]
          (cc/autoscale! compute-cluster pool-name pending-jobs))
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
          (is (= ["test-host-1" "test-host-2"] (.getValues node-selector-requirement))))))

    (testing "synthetic pods avoid node blocklist labels"
      (let [job-uuid-1 (str (UUID/randomUUID))
            pool-name "test-pool"
            compute-cluster (tu/make-kubernetes-compute-cluster {} #{pool-name} nil ["unhealthy-node" "unready-node"])
            pending-jobs [(make-job-fn job-uuid-1 "user-1")]
            launched-pods-atom (atom [])]
        (with-redefs [api/launch-pod (fn [_ _ cook-expected-state-dict _]
                                       (swap! launched-pods-atom conj cook-expected-state-dict))]
          (cc/autoscale! compute-cluster pool-name pending-jobs))
        (is (= 1 (count @launched-pods-atom)))
        (let [node-selector-terms
              (-> @launched-pods-atom
                  (nth 0)
                  :launch-pod
                  :pod
                  .getSpec
                  .getAffinity
                  .getNodeAffinity
                  .getRequiredDuringSchedulingIgnoredDuringExecution
                  .getNodeSelectorTerms)]
          (is (= 1 (count node-selector-terms)))
          (let [node-selector-requirement
                (->> node-selector-terms
                     (filter #(-> % .getMatchExpressions first .getKey (= "unhealthy-node")))
                     first
                     .getMatchExpressions
                     first)]
            (is (= "unhealthy-node" (.getKey node-selector-requirement)))
            (is (= "DoesNotExist" (.getOperator node-selector-requirement))))
          (let [node-selector-requirement
                (->> node-selector-terms
                     (filter #(-> % .getMatchExpressions first .getKey (= "unready-node")))
                     first
                     .getMatchExpressions
                     first)]
            (is (= "unready-node" (.getKey node-selector-requirement)))
            (is (= "DoesNotExist" (.getOperator node-selector-requirement)))))))))
