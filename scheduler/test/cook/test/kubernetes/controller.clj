(ns cook.test.kubernetes.controller
  (:require [clojure.test :refer :all]
            [cook.kubernetes.controller :as controller])
  (:import (io.kubernetes.client.models V1ObjectMeta V1Pod V1PodStatus)))


(deftest test-existing-state-equivalent?
  (testing "different states"
    (is (not (controller/existing-state-equivalent? {:state :pod/failed} {:state :pod/succeeded}))))

  (testing ":ancillary is ignored"
    (is (controller/existing-state-equivalent? {:ancillary {:foo 1}} {:ancillary {:bar 2}}))))

(deftest expected-state-equivalent?
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
             (controller/container-status->failure-reason pod-status container-status))))))

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

(deftest update-expected-state
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
        (is (= {:expected-state :expected/completed}
               (controller/pod-has-just-completed nil {:pod pod :synthesized-state {:state :pod/failed}})))))))