(ns cook.test.kubernetes.controller
  (:require [clojure.test :refer :all]
            [cook.kubernetes.api :as controller]
            [cook.test.testutil :as tu]
            [datomic.api :as d])
  (:import (io.kubernetes.client.models V1Container V1EnvVar V1Pod V1PodStatus V1ContainerStatus V1ContainerState V1ContainerStateWaiting)))


(deftest existing-state-equivalent?
  ;; TODO
  )

(deftest expected-state-equivalent?
  ;; TODO
  )

(deftest update-or-delete!
  ;; TODO
  )

(deftest container-status->failure-reason
  ;; TODO
  )

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
