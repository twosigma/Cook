(ns cook.test.kubernetes.controller
  (:require [clojure.test :refer :all]
            [cook.kubernetes.controller :as controller]
            [datomic.api :as d])
  (:import (io.kubernetes.client.models V1ObjectMeta V1Pod V1PodStatus)))


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
        do-process (fn [cook-expected-state k8s-actual-state]
                     (let [cook-expected-state-map
                           (atom {name {:cook-expected-state cook-expected-state}})]
                       (controller/process
                         {:api-client nil
                          :cook-expected-state-map cook-expected-state-map
                          :k8s-actual-state-map (atom {name {:synthesized-state {:state k8s-actual-state} :pod nil}})}
                         name)
                       (:cook-expected-state (get @cook-expected-state-map name {}))))]
    (with-redefs [controller/delete-pod  (fn [_ cook-expected-state-dict _] cook-expected-state-dict)
                  controller/kill-pod  (fn [_ cook-expected-state-dict _] cook-expected-state-dict)
                  controller/launch-pod (fn [_ _ cook-expected-state-dict _] cook-expected-state-dict)
                  controller/log-weird-state (fn [_ _ _ _] :illegal_return_value_should_be_unused)
                  controller/handle-pod-completed (fn [_ _] {:cook-expected-state :cook-expected-state/completed})
                  controller/handle-pod-started (fn [_ _] {:cook-expected-state :cook-expected-state/running})
                  controller/handle-pod-killed (fn [_ _] {:cook-expected-state :cook-expected-state/completed})
                  controller/write-status-to-datomic (fn [_] :illegal_return_value_should_be_unused)]

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
      (is (= :cook-expected-state/completed (do-process :cook-expected-state/running :pod/failed)))
      (is (= :cook-expected-state/running (do-process :cook-expected-state/running :pod/running)))
      (is (= :cook-expected-state/completed (do-process :cook-expected-state/running :pod/unknown)))
      (is (= :cook-expected-state/completed (do-process :cook-expected-state/running :pod/waiting)))

      (is (= :cook-expected-state/starting (do-process :cook-expected-state/starting :missing)))
      (is (= :cook-expected-state/completed (do-process :cook-expected-state/starting :pod/succeeded)))
      (is (= :cook-expected-state/completed (do-process :cook-expected-state/starting :pod/failed)))
      (is (= :cook-expected-state/running (do-process :cook-expected-state/starting :pod/running)))
      (is (= :cook-expected-state/completed (do-process :cook-expected-state/starting :pod/unknown)))
      (is (= :cook-expected-state/starting (do-process :cook-expected-state/starting :pod/waiting)))

      (is (nil? (do-process :missing :missing)))
      (is (nil? (do-process :missing :pod/succeeded)))
      (is (nil? (do-process :missing :pod/failed)))
      (is (= :missing (do-process :missing :pod/running)))
      (is (nil? (do-process :missing :pod/unknown)))
      (is (= :missing (do-process :missing :pod/waiting))))))

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
    (with-redefs [controller/delete-pod  (fn [_ cook-expected-state-dict _] (swap! count-delete-pod inc) cook-expected-state-dict)
                  controller/handle-pod-completed (fn [_ _] {:cook-expected-state :cook-expected-state/completed})
                  controller/write-status-to-datomic (fn [_] :illegal_return_value_should_be_unused)]

      (is (= :cook-expected-state/completed (do-process :cook-expected-state/completed :pod/succeeded)))
      (is (= 1 @count-delete-pod))
      ; Implicitly assume the watch triggers, moving us to next state in kubernetes:
      (is (nil? (do-process :cook-expected-state/completed :missing)))
      (is (= 1 @count-delete-pod)))))

(deftest test-handle-pod-completed
  (testing "graceful handling of lack of exit code"
    (let [pod (V1Pod.)
          pod-metadata (V1ObjectMeta.)
          pod-status (V1PodStatus.)]
      (.setMetadata pod pod-metadata)
      (.setReason pod-status "OutOfcpu")
      (.setStatus pod pod-status)
      (with-redefs [controller/write-status-to-datomic (constantly nil)]
        (is (= {:cook-expected-state :cook-expected-state/completed}
               (controller/handle-pod-completed nil {:pod pod :synthesized-state {:state :pod/failed}})))))))