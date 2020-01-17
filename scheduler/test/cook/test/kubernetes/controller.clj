(ns cook.test.kubernetes.controller
  (:require [clojure.test :refer :all]
            [cook.kubernetes.controller :as controller]
            [cook.test.testutil :as tu]
            [datomic.api :as d])
  (:import (io.kubernetes.client.models V1ObjectMeta V1Pod V1PodStatus V1Pod V1PodStatus)))


(deftest test-existing-state-equivalent?
  (testing "different states"
    (is (not (controller/existing-state-equivalent? {:state :pod/failed} {:state :pod/succeeded})))))

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
             (controller/container-status->failure-reason {:name "test-cluster"} "12345"
                                                          pod-status container-status))))))
(deftest test-process
  (let [do-process (fn [expected existing]
                     (let [name "name"
                           result
                           (controller/process
                             {:api-client nil
                              :existing-state-map (atom {name {:expected-state expected}})
                              :expected-state-map (atom {name {:synthesized-state existing :pod nil}})}
                             name)]
                       (get result name)))]
    (with-redefs [controller/log-weird-state (fn [_ _] :illegal)
                  controller/kill-task  (fn [_ expected-state-dict _] expected-state-dict)
                  controller/delete-task  (fn [_ _] nil)
                  controller/launch-task (fn [_ expected-state-dict] expected-state-dict)
                  controller/pod-has-just-completed (fn [_] {:expected-state :expected/completed})
                  controller/write-status-to-datomic (fn [_] :illegal)

                  ]
      (is (= :expected/starting) (do-process :expected/starting :missing))
      (is (= :expected/completed) (do-process :expected/starting :pod/completed))
      (is (= :expected/completed) (do-process :expected/starting :pod/failed))
      (is (= :expected/running) (do-process :expected/starting :pod/running))
      (is (= :expected/completed) (do-process :expected/starting :pod/unknown))
      (is (= :expected/starting) (do-process :expected/starting :pod/waiting))

      (is (= :expected/completed) (do-process :expected/running :missing))
      (is (= :expected/completed) (do-process :expected/running :pod/completed))
      (is (= :expected/completed) (do-process :expected/running :pod/failed))
      (is (= :expected/running) (do-process :expected/running :pod/running))
      (is (= :expected/completed) (do-process :expected/running :pod/unknown))
      (is (= :expected/completed) (do-process :expected/running :pod/waiting))

      (is (= nil) (do-process :expected/completed :missing))
      (is (= :expected/completed) (do-process :expected/completed :pod/completed))
      (is (= :expected/completed) (do-process :expected/completed :pod/failed))
      (is (= :expected/completed) (do-process :expected/completed :pod/running))
      (is (= :expected/completed) (do-process :expected/completed :pod/unknown))
      (is (= :expected/completed) (do-process :expected/completed :pod/waiting))

      (is (= :expected/killed) (do-process :expected/killed :missing))
      (is (= :expected/completed) (do-process :expected/killed :pod/completed))
      (is (= :expected/completed) (do-process :expected/killed :pod/failed))
      (is (= :expected/killed) (do-process :expected/killed :pod/running))
      (is (= :expected/killed) (do-process :expected/killed :pod/unknown))
      (is (= :expected/killed) (do-process :expected/killed :pod/waiting))

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