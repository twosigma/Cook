(ns cook.test.kubernetes.controller
  (:require [clojure.test :refer :all]
            [cook.kubernetes.controller :as controller]
            [cook.test.testutil :as tu]
            [datomic.api :as d])
  (:import (io.kubernetes.client.models V1ObjectMeta V1Pod V1PodStatus V1Pod V1PodStatus)))


(deftest test-k8s-actual-state-equivalent?
  (testing "different states"
    (is (not (controller/k8s-actual-state-equivalent? {:state :pod/failed} {:state :pod/succeeded})))))

(deftest cook-expected-state-equivalent?
  ;; TODO
  )

(deftest update-or-delete!
  ;; TODO
  )

(deftest test-container-status->failure-reason
  (testing "no container status + out-of-cpu pod status"
    (let [pod-status (V1PodStatus.)
          container-status nil]
      (.setReason pod-status "OutOfcpu")
      (is (= :reason-invalid-offers
             (controller/container-status->failure-reason {:name "test-cluster"} "12345"
                                                          pod-status container-status))))))
(deftest test-process
  (let [do-process (fn [cook-expected-state existing]
                     (let [name "name"
                           result
                           (controller/process
                             {:api-client nil
                              :k8s-actual-state-map (atom {name {:cook-expected-state cook-expected-state}})
                              :cook-expected-state-map (atom {name {:synthesized-state existing :pod nil}})}
                             name)]
                       (get result name)))]
    (with-redefs [controller/log-weird-state (fn [_ _] :illegal)
                  controller/kill-pod  (fn [_ cook-expected-state-dict _] cook-expected-state-dict)
                  controller/delete-pod  (fn [_ _] nil)
                  controller/launch-pod (fn [_ cook-expected-state-dict] cook-expected-state-dict)
                  controller/pod-has-just-completed (fn [_] {:cook-expected-state :cook-expected-state/completed})
                  controller/write-status-to-datomic (fn [_] :illegal)]

      (is (= :cook-expected-state/starting) (do-process :cook-expected-state/starting :missing))
      (is (= :cook-expected-state/completed) (do-process :cook-expected-state/starting :pod/completed))
      (is (= :cook-expected-state/completed) (do-process :cook-expected-state/starting :pod/failed))
      (is (= :cook-expected-state/running) (do-process :cook-expected-state/starting :pod/running))
      (is (= :cook-expected-state/completed) (do-process :cook-expected-state/starting :pod/unknown))
      (is (= :cook-expected-state/starting) (do-process :cook-expected-state/starting :pod/waiting))

      (is (= :cook-expected-state/completed) (do-process :cook-expected-state/running :missing))
      (is (= :cook-expected-state/completed) (do-process :cook-expected-state/running :pod/completed))
      (is (= :cook-expected-state/completed) (do-process :cook-expected-state/running :pod/failed))
      (is (= :cook-expected-state/running) (do-process :cook-expected-state/running :pod/running))
      (is (= :cook-expected-state/completed) (do-process :cook-expected-state/running :pod/unknown))
      (is (= :cook-expected-state/completed) (do-process :cook-expected-state/running :pod/waiting))

      (is (= nil) (do-process :cook-expected-state/completed :missing))
      (is (= :cook-expected-state/completed) (do-process :cook-expected-state/completed :pod/completed))
      (is (= :cook-expected-state/completed) (do-process :cook-expected-state/completed :pod/failed))
      (is (= :cook-expected-state/completed) (do-process :cook-expected-state/completed :pod/running))
      (is (= :cook-expected-state/completed) (do-process :cook-expected-state/completed :pod/unknown))
      (is (= :cook-expected-state/completed) (do-process :cook-expected-state/completed :pod/waiting))

      (is (= :cook-expected-state/killed) (do-process :cook-expected-state/killed :missing))
      (is (= :cook-expected-state/completed) (do-process :cook-expected-state/killed :pod/completed))
      (is (= :cook-expected-state/completed) (do-process :cook-expected-state/killed :pod/failed))
      (is (= :cook-expected-state/killed) (do-process :cook-expected-state/killed :pod/running))
      (is (= :cook-expected-state/killed) (do-process :cook-expected-state/killed :pod/unknown))
      (is (= :cook-expected-state/killed) (do-process :cook-expected-state/killed :pod/waiting))

      (is (= nil) (do-process :missing :missing))
      (is (= nil) (do-process :missing :pod/completed))
      (is (= nil) (do-process :missing :pod/failed))
      (is (= nil) (do-process :missing :pod/running))
      (is (= nil) (do-process :missing :pod/unknown))
      (is (= nil) (do-process :missing :pod/waiting)))))

(deftest process-successful
  ;; TODO
  ;; Drive a pod through the successful job completion, failed job, and killed job paths.
  ;; Make sure the correct state gets written back to datomic.
  )

(deftest process-failed
  ;; TODO
  ;; Drive a pod through the failed job completion, failed job, and killed job paths.
  ;; Make sure the correct state gets written back to datomic.
  )

(deftest process-killed
  ;; TODO
  ;; Drive a pod through the killed job completion, failed job, and killed job paths.
  ;; Make sure the correct state gets written back to datomic.
  )

(deftest process-killed-complete-race
  ;; TODO
  ;; Drive a pod through, but have killing race with success.
  ;; Make sure the correct state gets written back to datomic.
  )

(deftest process-visit-all-states
  ;; TODO
  ;; Run all state pairs. Make sure no exceptions.
  )

(deftest pod-update
  ;; TODO
  )

(deftest pod-deleted
  ;; TODO
  )

(deftest update-cook-expected-state
  ;; TODO
  )

(deftest test-pod-has-just-completed
  (testing "graceful handling of lack of exit code"
    (let [pod (V1Pod.)
          pod-metadata (V1ObjectMeta.)
          pod-status (V1PodStatus.)]
      (.setMetadata pod pod-metadata)
      (.setReason pod-status "OutOfcpu")
      (.setStatus pod pod-status)
      (with-redefs [controller/write-status-to-datomic (constantly nil)]
        (is (= {:cook-expected-state :cook-expected-state/completed}
               (controller/pod-has-just-completed nil {:pod pod :synthesized-state {:state :pod/failed}})))))))