(ns cook.test.kubernetes.controller
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [cook.compute-cluster :as cc]
            [cook.config :as config]
            [cook.kubernetes.api :as api]
            [cook.kubernetes.controller :as controller]
            [cook.test.testutil :as tu])
  (:import (io.kubernetes.client ApiException)
           (io.kubernetes.client.models V1PodCondition V1PodStatus)
           (java.util.concurrent.locks ReentrantLock)))

(defn make-test-kubernetes-config
  []
  {:controller-lock-num-shards 1
   :controller-lock-objects [(ReentrantLock.)]})

(deftest test-k8s-actual-state-equivalent?
  (testing "different states"
    (is (not (controller/k8s-actual-state-equivalent? {:state :pod/failed} {:state :pod/succeeded})))))

(deftest test-container-status->failure-reason
  (testing "no container status + out-of-cpu pod status"
    (let [pod-status (V1PodStatus.)
          container-status nil]
      (.setReason pod-status "OutOfcpu")
      (is (= :reason-invalid-offers
             (controller/container-status->failure-reason {:name "test-cluster"} "12345"
                                                          pod-status container-status))))))
(deftest test-process
  (let [name "TestPodName"
        reason (atom nil)
        do-process (fn [cook-expected-state k8s-actual-state & {:keys [create-namespaced-pod-fn
                                                                       ^V1PodCondition pod-condition
                                                                       custom-test-state
                                                                       force-nil-pod?]
                                                                :or {create-namespaced-pod-fn (constantly true)
                                                                     custom-test-state nil
                                                                     force-nil-pod? false}}]
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
                       (let [pod (tu/pod-helper name "hostA" {:cpus 1.0 :mem 100.0})
                             pod-status (V1PodStatus.)
                             _ (when pod-condition (.addConditionsItem pod-status pod-condition))
                             _ (.setStatus pod pod-status)
                             cook-expected-state-map
                             (atom {name {:cook-expected-state cook-expected-state
                                          :launch-pod {:pod pod}}})]
                         (controller/process
                           {:api-client nil
                            :cook-expected-state-map cook-expected-state-map
                            :k8s-actual-state-map (atom {name {:synthesized-state (or custom-test-state {:state k8s-actual-state})
                                                               :pod (if force-nil-pod? nil pod)}})}
                           name)
                         (:cook-expected-state (get @cook-expected-state-map name {})))))]

    (is (nil? (do-process :cook-expected-state/completed :missing)))
    (is (= :cook-expected-state/completed  (do-process :cook-expected-state/completed :pod/succeeded)))
    (is (= :cook-expected-state/completed  (do-process :cook-expected-state/completed :pod/failed)))
    (is (= :cook-expected-state/completed (do-process :cook-expected-state/completed :pod/running)))
    (is (= :cook-expected-state/completed (do-process :cook-expected-state/completed :pod/unknown)))
    (is (= :cook-expected-state/completed (do-process :cook-expected-state/completed :pod/waiting)))

    (is (nil? (do-process :cook-expected-state/killed :missing)))
    (is (= :cook-expected-state/completed (do-process :cook-expected-state/killed :pod/succeeded)))
    (is (= :cook-expected-state/completed (do-process :cook-expected-state/killed :pod/failed)))
    (is (= :cook-expected-state/killed (do-process :cook-expected-state/killed :pod/running)))
    (is (= :cook-expected-state/completed (do-process :cook-expected-state/killed :pod/unknown)))
    (is (= :cook-expected-state/killed (do-process :cook-expected-state/killed :pod/waiting)))

    (is (nil? (do-process :cook-expected-state/running :missing)))
    (is (= :cook-expected-state/completed (do-process :cook-expected-state/running :pod/succeeded)))
    (is (= :reason-normal-exit @reason))
    (is (= :cook-expected-state/completed (do-process :cook-expected-state/running :pod/failed)))
    (is (= :cook-expected-state/running (do-process :cook-expected-state/running :pod/running)))
    (is (= :cook-expected-state/completed (do-process :cook-expected-state/running :pod/unknown)))
    (is (= :cook-expected-state/completed (do-process :cook-expected-state/running :pod/waiting)))
    (is (= :reason-slave-removed @reason))

    (is (= :cook-expected-state/starting (do-process :cook-expected-state/starting :missing)))
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

    (is (nil? (do-process :missing :missing)))
    (is (nil? (do-process :missing :pod/succeeded)))
    (is (nil? (do-process :missing :pod/failed)))
    (is (= :missing (do-process :missing :pod/running)))
    (is (nil? (do-process :missing :pod/unknown)))
    (is (= :missing (do-process :missing :pod/waiting)))
    (is (nil? (do-process :cook-expected-state/running :missing :custom-test-state {:state :missing
                                                                                    :reason "Node preempted"
                                                                                    :pod-deleted? true
                                                                                    :pod-preempted? true})))
    (is (= :reason-slave-removed @reason))
    (is (nil? (do-process :cook-expected-state/running :missing :custom-test-state {:state :missing
                                                                                    :reason "Pod was explicitly deleted"
                                                                                    :pod-deleted? true}
                          :force-nil-pod? true)))
    (is (= :reason-executor-terminated @reason))
    (is (nil? (do-process :cook-expected-state/running :missing :custom-test-state {:state :missing
                                                                                    :reason "Pod was explicitly deleted"
                                                                                    :pod-deleted? true})))
    (is (= :reason-executor-terminated @reason))))

(deftest test-launch-kill-process
  (let [pod-name "TestPodName-LKP"
        cook-expected-state-map (atom {})
        k8s-actual-state-map (atom {})
        mock-cc {:api-client nil
                 :cook-expected-state-map cook-expected-state-map
                 :k8s-actual-state-map k8s-actual-state-map}
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
      (controller/process mock-cc pod-name)
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
                          :k8s-actual-state-map (atom {name {:synthesized-state {:state k8s-actual-state} :pod nil}})}
                         name)
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
                    controller/container-status->failure-reason (fn [_ _ _ _]
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
                    controller/container-status->failure-reason (fn [_ _ _ _]
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
        (is (= :reason-task-unknown @reason))))))

(deftest test-synthesize-state-and-process-pod-if-changed
  (testing "gracefully handles nil pod"
    (let [compute-cluster {:k8s-actual-state-map (atom {})
                           :cook-expected-state-map (atom {})}
          pod-name "test-pod"
          pod nil]
      (controller/synthesize-state-and-process-pod-if-changed compute-cluster pod-name pod))))

(deftest test-scan-process
  (testing "gracefully handles nil pod"
    (let [compute-cluster {:k8s-actual-state-map (atom {})
                           :cook-expected-state-map (atom {})}
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
