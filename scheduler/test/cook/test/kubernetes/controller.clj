(ns cook.test.kubernetes.controller
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [cook.cached-queries :as cached-queries]
            [cook.compute-cluster :as cc]
            [cook.config :as config]
            [cook.kubernetes.api :as api]
            [cook.kubernetes.controller :as controller]
            [cook.test.testutil :as tu]
            [datomic.api :as d :refer [q]]
            [metrics.timers :as timers])
  (:import (io.kubernetes.client.openapi ApiException)
           (io.kubernetes.client.openapi.models V1ObjectMeta V1Pod V1PodCondition V1PodStatus)
           (java.util.concurrent.locks ReentrantLock)))

(defn make-test-kubernetes-config
  []
  {:controller-lock-num-shards 1})

(deftest test-k8s-actual-state-equivalent?
  (testing "different states"
    (is (not (controller/k8s-actual-state-equivalent? {:state :pod/failed} {:state :pod/succeeded})))))

(deftest test-container-status->failure-reason
  (testing "no container status + out-of-cpu pod status"
    (let [pod-status (V1PodStatus.)
          pod (tu/pod-helper "12345" "hostA" {:cpus 1.0 :mem 100.0})]
      (.setReason pod-status "OutOfcpu")
      (.setStatus pod pod-status)
      (is (= :reason-invalid-offers
             (controller/container-status->failure-reason {:name "test-cluster"} pod))))))
(deftest test-process
  (tu/setup)
  (with-redefs [cached-queries/instance-uuid->job-uuid-datomic-query (constantly (java.util.UUID/randomUUID))
                d/db (constantly nil)
                cached-queries/job-uuid->job-map-cache-lookup (constantly {:job/name "sample-name"
                                                                           :job/user "sample-user"
                                                                           :job/uuid "sample-uuid"
                                                                           :job/pool {:pool/name "sample-pool"}})]
    (let [name "TestPodName"
          reason (atom nil)
          do-process-full-state (fn [cook-expected-state k8s-actual-state & {:keys [create-namespaced-pod-fn
                                                                                    ^V1PodCondition pod-condition
                                                                                    custom-test-state
                                                                                    force-nil-pod?
                                                                                    ^V1ObjectMeta pod-metadata]
                                                                             :or {create-namespaced-pod-fn (constantly true)
                                                                                  custom-test-state nil
                                                                                  force-nil-pod? false
                                                                                  pod-metadata nil}}]
                                  (reset! reason nil)
                                  (with-redefs [controller/delete-pod (fn [_ _ cook-expected-state-dict _]
                                                                        cook-expected-state-dict)
                                                controller/kill-pod (fn [_ _ cook-expected-state-dict _]
                                                                      cook-expected-state-dict)
                                                controller/handle-pod-killed (fn [_ _]
                                                                               {:cook-expected-state :cook-expected-state/completed})
                                                controller/write-status-to-datomic (fn [_ status]
                                                                                     (reset! reason (:reason status)))
                                                api/create-namespaced-pod create-namespaced-pod-fn
                                                cc/compute-cluster-name (constantly "compute-cluster-name")]
                                    (let [^V1Pod pod (tu/pod-helper name "hostA" {:cpus 1.0 :mem 100.0})
                                          pod-status (V1PodStatus.)
                                          _ (when pod-condition (.addConditionsItem pod-status pod-condition))
                                          _ (.setStatus pod pod-status)
                                          _ (when pod-metadata (.setMetadata pod pod-metadata))
                                          cook-expected-state-map
                                          (atom {name {:cook-expected-state cook-expected-state
                                                       :launch-pod {:pod pod}
                                                       :waiting-metric-timer (timers/start (timers/timer "fake-timer-name"))
                                                       ; We just want a function that does nothing here to test that the value is set/removed appropriately
                                                       :waiting-metric-timer-prom-stop-fn (fn [] (do))}})]
                                      (controller/process
                                        {:api-client nil
                                         :cook-expected-state-map cook-expected-state-map
                                         :k8s-actual-state-map (atom {name {:synthesized-state (or custom-test-state {:state k8s-actual-state})
                                                                            :pod (if force-nil-pod? nil pod)}})
                                         :cook-starting-pods (atom {})}
                                        name false)
                                      (get @cook-expected-state-map name {}))))
          do-process (fn [cook-expected-state k8s-actual-state & {:keys [create-namespaced-pod-fn
                                                                         ^V1PodCondition pod-condition
                                                                         custom-test-state
                                                                         force-nil-pod?
                                                                         ^V1ObjectMeta pod-metadata]
                                                                  :or {create-namespaced-pod-fn (constantly true)
                                                                       custom-test-state nil
                                                                       force-nil-pod? false
                                                                       pod-metadata nil}}]
                       (:cook-expected-state (do-process-full-state cook-expected-state k8s-actual-state
                                                                    :create-namespaced-pod-fn create-namespaced-pod-fn
                                                                    :pod-condition pod-condition
                                                                    :custom-test-state custom-test-state
                                                                    :force-nil-pod? force-nil-pod?
                                                                    :pod-metadata pod-metadata)))]

      (is (nil? (do-process :cook-expected-state/completed :missing)))
      (is (nil? (do-process :cook-expected-state/completed :pod/deleting)))
      (is (= :cook-expected-state/completed (do-process :cook-expected-state/completed :pod/succeeded)))
      (is (= :cook-expected-state/completed (do-process :cook-expected-state/completed :pod/failed)))
      (is (= :cook-expected-state/completed (do-process :cook-expected-state/completed :pod/running)))
      (is (= :cook-expected-state/completed (do-process :cook-expected-state/completed :pod/unknown)))
      (is (= :cook-expected-state/completed (do-process :cook-expected-state/completed :pod/waiting)))

      (is (nil? (do-process :cook-expected-state/killed :missing)))
      (is (nil? (do-process :cook-expected-state/killed :pod/deleting)))
      (is (= :cook-expected-state/completed (do-process :cook-expected-state/killed :pod/succeeded)))
      (is (= :cook-expected-state/completed (do-process :cook-expected-state/killed :pod/failed)))
      (is (= :cook-expected-state/killed (do-process :cook-expected-state/killed :pod/running)))
      (is (= :cook-expected-state/completed (do-process :cook-expected-state/killed :pod/unknown)))
      (is (= :cook-expected-state/killed (do-process :cook-expected-state/killed :pod/waiting)))

      (is (nil? (do-process :cook-expected-state/running :missing)))
      (is (= :reason-killed-externally @reason))
      (is (nil? (do-process :cook-expected-state/running :missing :custom-test-state {:state :missing
                                                                                      :reason "Pod was explicitly deleted"
                                                                                      :pod-deleted? true
                                                                                      :pod-preempted-timestamp 1589084484537})))
      (is (= :reason-slave-removed @reason))
      (is (nil? (do-process :cook-expected-state/running :missing :custom-test-state {:state :missing
                                                                                      :reason "Pod was explicitly deleted"
                                                                                      :pod-deleted? true}
                            :force-nil-pod? true)))
      (is (= :reason-killed-externally @reason))
      (is (nil? (do-process :cook-expected-state/running :missing :custom-test-state {:state :missing
                                                                                      :reason "Pod was explicitly deleted"
                                                                                      :pod-deleted? true})))
      (is (= :reason-killed-externally @reason))
      (is (nil? (do-process :cook-expected-state/running :pod/deleting)))
      (is (= :reason-killed-externally @reason))
      (is (= :cook-expected-state/completed (do-process :cook-expected-state/running :pod/succeeded)))
      (is (= :reason-normal-exit @reason))
      (is (= :cook-expected-state/completed (do-process :cook-expected-state/running :pod/failed)))
      (is (= :unknown @reason))
      (is (= :cook-expected-state/completed
             (do-process :cook-expected-state/running
                         nil
                         :custom-test-state {:pod-preempted-timestamp 1 :state :pod/failed})))
      (is (= :reason-slave-removed @reason))
      (is (= :cook-expected-state/running (do-process :cook-expected-state/running :pod/running)))
      (is (= :cook-expected-state/completed (do-process :cook-expected-state/running :pod/unknown)))
      (is (= :cook-expected-state/completed (do-process :cook-expected-state/running :pod/waiting)))
      (is (= :reason-slave-removed @reason))

      (is (= :cook-expected-state/starting (do-process :cook-expected-state/starting :missing)))
      (is (nil? (do-process
                  :cook-expected-state/starting
                  nil
                  :custom-test-state
                  {:state :pod/deleting
                   :reason "Pod was explicitly deleted"
                   :pod-preempted-timestamp 1589084484537})))
      (is (= :reason-slave-removed @reason))
      (is (nil? (do-process
                  :cook-expected-state/starting
                  nil
                  :custom-test-state
                  {:state :pod/deleting
                   :reason "Pod was explicitly deleted"}
                  :force-nil-pod? true)))
      (is (= :reason-killed-externally @reason))
      (is (nil? (do-process :cook-expected-state/starting :missing :create-namespaced-pod-fn
                            (fn [_ _ _] (throw (ApiException. nil nil 422 nil nil))))))
      (is (= :cook-expected-state/completed (do-process :cook-expected-state/starting :pod/succeeded)))
      (with-redefs [config/kubernetes (constantly {:pod-condition-unschedulable-seconds 60})]
        (is (= :cook-expected-state/completed (do-process :cook-expected-state/starting :pod/failed
                                                          :pod-condition (doto (V1PodCondition.)
                                                                           (.setType "PodScheduled")
                                                                           (.setStatus "False")
                                                                           (.setReason "Unschedulable")
                                                                           (.setLastTransitionTime (t/epoch)))))))
      (is (= :reason-scheduling-failed-on-host @reason))
      (is (= :cook-expected-state/running (do-process :cook-expected-state/starting :pod/running)))
      (is (= :reason-running @reason))
      (is (= :cook-expected-state/completed (do-process :cook-expected-state/starting :pod/unknown)))
      (is (= :cook-expected-state/starting (do-process :cook-expected-state/starting :pod/waiting)))

      (testing "Make sure we remove the waiting metric at the right time"
        (is (nil? (:waiting-metric-timer (do-process-full-state :cook-expected-state/starting :pod/waiting))))
        (is (nil? (:waiting-metric-timer-prom-stop-fn (do-process-full-state :cook-expected-state/starting :pod/waiting))))
        (is (not (nil? (:waiting-metric-timer (do-process-full-state :cook-expected-state/starting :missing)))))
        (is (not (nil? (:waiting-metric-timer-prom-stop-fn (do-process-full-state :cook-expected-state/starting :missing))))))

      (is (nil? (do-process :missing :missing)))
      (testing "(:missing, :pod/deleting)"
        (let [hard-kill-atom (atom false)
              pod-metadata (V1ObjectMeta.)]
          (with-redefs [controller/kill-pod-hard
                        (fn [_ _ _]
                          (reset! hard-kill-atom true)
                          nil)
                        config/kubernetes
                        (constantly {:pod-deletion-timeout-seconds (* 60 15)})]
            (testing "No hard kill on pods with a recent deletion timestamp"
              (.setDeletionTimestamp pod-metadata (t/now))
              (is (nil? (do-process :missing :pod/deleting :pod-metadata pod-metadata)))
              (is (false? @hard-kill-atom)))
            (testing "Hard kill on pods with an old deletion timestamp"
              (.setDeletionTimestamp pod-metadata (t/epoch))
              (is (nil? (do-process :missing :pod/deleting :pod-metadata pod-metadata)))
              (is (true? @hard-kill-atom))))))
      (is (nil? (do-process :missing :pod/succeeded)))
      (is (nil? (do-process :missing :pod/failed)))
      (is (= :missing (do-process :missing :pod/running)))
      (is (nil? (do-process :missing :pod/unknown)))
      (is (= :missing (do-process :missing :pod/waiting))))))

(deftest test-launch-kill-process
  (let [pod-name "TestPodName-LKP"
        cook-expected-state-map (atom {})
        k8s-actual-state-map (atom {})
        mock-cc {:api-client nil
                 :cook-expected-state-map cook-expected-state-map
                 :k8s-actual-state-map k8s-actual-state-map
                 :cook-starting-pods (atom {})
                 :controller-lock-objects [(ReentrantLock.)]}
        extract-cook-expected-state (fn []
                                      (:cook-expected-state (get @cook-expected-state-map pod-name {})))
        count-kill-pod (atom 0)]
    (with-redefs [controller/kill-pod  (fn [_ _ cook-expected-state-dict _] (swap! count-kill-pod inc) cook-expected-state-dict)
                  controller/launch-pod (fn [_ cook-expected-state-dict _] cook-expected-state-dict)
                  controller/handle-pod-completed (fn [_ _ _] {:cook-expected-state :cook-expected-state/completed})
                  controller/handle-pod-killed (fn [_ _]
                                                 {:cook-expected-state :cook-expected-state/completed})
                  controller/write-status-to-datomic (fn [_] :illegal_return_value_should_be_unused)
                  controller/prepare-k8s-actual-state-dict-for-logging identity
                  config/kubernetes make-test-kubernetes-config]

      (controller/update-cook-expected-state mock-cc pod-name {:cook-expected-state :cook-expected-state/starting
                                                               :launch-pod {:pod :the-launch-pod}})
      ; controller/process is implicitly run by update-cook-expected-state.
      (is (= :cook-expected-state/starting (extract-cook-expected-state)))

      ;; Now the kill arrives.
      (controller/update-cook-expected-state mock-cc pod-name {:cook-expected-state :cook-expected-state/killed})
      ; controller/process is implicitly run by update-cook-expected-state.

      ; The resulting expected state should be nil; we have opportunistically killed it.
      (is (nil? (extract-cook-expected-state)))
      (is (= 1 @count-kill-pod))

      ; Now pretend the watch returns
      (swap! k8s-actual-state-map assoc pod-name {:pod :a-watch-pod
                                                  :synthesized-state {:state :pod/waiting}})
      ; We're in :missing, :pod/starting. Should kill the pod and move to missing,missing.
      (controller/process mock-cc pod-name false)
      (is (nil? (extract-cook-expected-state)))

      (is (= 2 @count-kill-pod)))))

(deftest test-completion-protocol
  (let [name "TestPodName"
        do-process (fn [cook-expected-state k8s-actual-state]
                     (let [cook-expected-state-map
                           (atom {name {:cook-expected-state cook-expected-state}})]
                       (controller/process
                         {:api-client nil
                          :cook-expected-state-map cook-expected-state-map
                          :k8s-actual-state-map (atom {name {:synthesized-state {:state k8s-actual-state} :pod nil}})
                          :cook-starting-pods (atom {})}
                         name false)
                       (:cook-expected-state (get @cook-expected-state-map name {}))))
        count-delete-pod (atom 0)]
    (with-redefs [controller/delete-pod  (fn [_ _ cook-expected-state-dict _] (swap! count-delete-pod inc) cook-expected-state-dict)
                  controller/handle-pod-completed (fn [_ _ _] {:cook-expected-state :cook-expected-state/completed})
                  controller/write-status-to-datomic (fn [_] :illegal_return_value_should_be_unused)]

      (is (= :cook-expected-state/completed (do-process :cook-expected-state/completed :pod/succeeded)))
      (is (= 1 @count-delete-pod))
      ; Implicitly assume the watch triggers, moving us to next state in kubernetes:
      (is (nil? (do-process :cook-expected-state/completed :missing)))
      (is (= 1 @count-delete-pod)))))

(deftest test-handle-pod-completed
  (tu/setup)
  (with-redefs [cached-queries/instance-uuid->job-uuid-datomic-query (constantly (java.util.UUID/randomUUID))
                d/db (constantly nil)
                cached-queries/job-uuid->job-map-cache-lookup (constantly {:job/name "sample-name"
                                                                           :job/user "sample-user"
                                                                           :job/uuid "sample-uuid"
                                                                           :job/pool {:pool/name "sample-pool"}})]
    (testing "graceful handling of lack of exit code"
      (let [pod (tu/pod-helper "podA" "hostA" {})
            pod-status (V1PodStatus.)]
        (.setReason pod-status "OutOfcpu")
        (.setStatus pod pod-status)
        (with-redefs [controller/write-status-to-datomic (constantly nil)]
          (is (= {:cook-expected-state :cook-expected-state/completed}
                 (controller/handle-pod-completed nil "podA" {:pod pod :synthesized-state {:state :pod/failed}}))))))

    (testing "optional reason argument"
      (let [pod (tu/pod-helper "podA" "hostA" {})
            reason (atom nil)]
        (.setStatus pod (V1PodStatus.))
        (with-redefs [controller/write-status-to-datomic (fn [_ status] (reset! reason (:reason status)))
                      controller/container-status->failure-reason (fn [_ _]
                                                                    (throw (ex-info "Shouldn't get called" {})))]
          (controller/handle-pod-completed nil "podA" {:pod pod :synthesized-state {:state :pod/failed}}
                                           :reason :reason-task-invalid))
        (is (= :reason-task-invalid @reason))))

    (testing "throws exception"
      (let [pod (tu/pod-helper "podA" "hostA" {})
            reason (atom nil)]
        (.setStatus pod (V1PodStatus.))
        (with-redefs [cc/compute-cluster-name (constantly "compute-cluster-name")
                      controller/write-status-to-datomic (fn [_ status] (reset! reason (:reason status)))
                      controller/container-status->failure-reason (fn [_ _]
                                                                    (throw (ex-info "Got Exception" {})))]
          (controller/handle-pod-completed nil "podA" {:pod pod :synthesized-state {:state :pod/failed}})
          (is (= :reason-task-unknown @reason)))))

    (testing "pod mismatch"
      (let [pod (tu/pod-helper "podA" "hostA" {})
            reason (atom nil)]
        (.setStatus pod (V1PodStatus.))
        (with-redefs [cc/compute-cluster-name (constantly "compute-cluster-name")
                      controller/write-status-to-datomic (fn [_ status] (reset! reason (:reason status)))]
          ; We expect an AssertionError because of the name mismatch.
          (is (thrown? AssertionError
                       (controller/handle-pod-completed nil "podB" {:pod pod :synthesized-state {:state :pod/failed}}))))))

    (testing "pod is nil"
      (let [pod nil
            reason (atom nil)]
        (with-redefs [cc/compute-cluster-name (constantly "compute-cluster-name")
                      controller/write-status-to-datomic (fn [_ status] (reset! reason (:reason status)))]
          (controller/handle-pod-completed nil "podB" {:pod pod :synthesized-state {:state :pod/failed}})
          (is (= :reason-task-unknown @reason)))))))

(deftest test-synthesize-state-and-process-pod-if-changed
  (testing "gracefully handles nil pod"
    (let [compute-cluster {:k8s-actual-state-map (atom {})
                           :cook-expected-state-map (atom {})
                           :cook-starting-pods (atom {})} ; So that we don't get a NPE running the test
          pod-name "test-pod"
          pod nil]
      (controller/synthesize-state-and-process-pod-if-changed compute-cluster pod-name pod)
      ; Make sure no memory leak --- we shouldn't create a pod in this circumstance.
      (is (= {} @(get compute-cluster :k8s-actual-state-map)))
      (is (= {} @(get compute-cluster :cook-expected-state-map))))))

(deftest test-scan-process
  (testing "gracefully handles nil pod"
    (let [compute-cluster {:k8s-actual-state-map (atom {})
                           :cook-expected-state-map (atom {})
                           :controller-lock-objects [(ReentrantLock.)]}
          pod-name "test-pod"]
      (with-redefs [config/kubernetes make-test-kubernetes-config]
        (controller/scan-process compute-cluster pod-name)))))

(deftest test-handle-pod-preemption
  (testing "avoids datomic write for synthetic pods"
    (with-redefs [controller/write-status-to-datomic
                  (fn [_ _]
                    (throw (ex-info "BAD" {})))]
      (is (= {:cook-expected-state :cook-expected-state/completed}
             (controller/handle-pod-preemption {} (str api/cook-synthetic-pod-name-prefix "-test-pod")))))))
