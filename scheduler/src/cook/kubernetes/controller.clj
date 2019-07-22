(ns cook.kubernetes.controller
  (:require [clojure.tools.logging :as log]
            [cook.kubernetes.api :as api])
  (:import (io.kubernetes.client.models V1Pod)))

;
;   Wire up a store with the results.
;

(def lock-object (Object.))
(defn calculate-lock
  "Given a pod-name, return an object suitable for locking for accessing it."
  [_]
  ; TODO: Should do lock sharding based on hash of pod-name.
  lock-object)

(defn expected-state-equivalent?
  "Is the old and new state equivalent?"
  [old-state new-state]
  false ; TODO
  )

(defn existing-state-equivalent?
  "Is the old and new state equivalent?"
  [old-state new-state]
  false ; TODO
  )

(defn remove-finalization-if-set-and-delete
  [api-client expected-state-dict pod]
  (api/remove-finalization-if-set-and-delete api-client pod)
  expected-state-dict)

(defn kill-task
  [api-client expected-state-dict pod]
  (api/kill-task api-client pod)
  expected-state-dict)

(defn launch-task
  [api-client expected-state-dict]
  (api/launch-task api-client expected-state-dict)
  expected-state-dict)

(defn update-or-delete!
  [map-atom key value]
  (if (nil? value)
    (swap! map-atom dissoc map key)
    (swap! update map-atom key value)))

;; REVIEW: Review handle-status-update
(defn pod-has-just-completed
  "A pod has completed."
  [{:keys [synthesized-state pod] :as existing-state-dictionary}]
  (case synthesized-state
    :existing/failed
    (do
      ; TODO: Extract failure reason, etc and store in datomic, via refactored handle-status-update.
      {:expected-state :expected/completed})
    :existing/succeeded
    (do
      ; TODO: Mark job as success in datomic, via refactored handle-status-update.
      {:expected-state :expected/completed})))

(defn prepare-expected-state-dict-for-logging
  [expected-state-dict]
  ".toString on a pod is incredibly large. Make a version thats been elided."
  (if (:launch-pod expected-state-dict)
    (assoc expected-state-dict :launch-pod [:elided-for-brevity])
    expected-state-dict))

;; REVIEW: Review handle-status-update
(defn pod-has-started
  "A pod has completed."
  [{:keys [synthesized-state pod] :as existing-state-dictionary}]
  ;(api/TODO); TODO Update datomic state to instance.state/running.
  :existing/running)


(defn process
  "Visit this pod-name, processing the new level-state. Returns the new expected state. Returns
  empty dictionary to indicate that the result should be deleted. NOTE: Must be invoked with the lock."
  [{:keys [api-client existing-state-map expected-state-map] :as kcc} pod-name]
  (let [{:keys [expected-state] :as expected-state-dict} (get @expected-state-map pod-name)
        {:keys [synthesized-existing-state pod] :as existing-state-dict} (get @existing-state-map pod-name)]
    (log/info "Processing " pod-name ": ((" (prepare-expected-state-dict-for-logging expected-state-dict) " ===== " existing-state-dict "))")
    ; TODO: We added an :expected/starting state to the machine, to represent when a pod is starting. We map instance.status/unknown to that state
    ; The todo is to add in cases for [:expected/starting *] for those other states.
    (let
        [new-expected-state (case (vector (or expected-state :missing) (or synthesized-existing-state :missing))
                              [:expected/starting :missing] (launch-task api-client expected-state-dict)
                              [:expected/starting :existing/running] (pod-has-started existing-state-dict)
                              ; TODO We need to add cases for the other [:expected/starting *] states in.
                              [:expected/running :existing/running] expected-state-dict
                              [:expected/running :existing/succeeded] (pod-has-just-completed existing-state-dict)
                              [:expected/completed :existing/succeeded] (remove-finalization-if-set-and-delete api-client expected-state-dict pod)
                              [:expected/running :existing/failed] (pod-has-just-completed existing-state-dict)
                              [:expected/completed :existing/failed] (remove-finalization-if-set-and-delete api-client expected-state-dict pod)
                              [:expected/completed :missing] nil ; Cause it to be deleted.
                              [:expected/killed :existing/running] (kill-task api-client expected-state-dict pod) ; TODO: Where does the datomic update occur? Do we do it when we do [expected/killed :existing/failed], to be similar to mesos, we update it only when the backend says its dead?

                              [:expected/killed :existing/succeeded] (remove-finalization-if-set-and-delete api-client expected-state-dict pod)
                              [:expected/killed :existing/failed]
                              (do ; TODO: Invoke handle-status-update.
                                (remove-finalization-if-set-and-delete api-client expected-state-dict pod))
                              [:expected/killed :missing] nil
                              ; TODO: Implement :existing/unknown cases.
                              ; TODO: Implement :existing/pending cases.
                              ; TODO: Implement :existing/need-to-fail cases.
                              [:missing :existing/running] (kill-task api-client expected-state-dict pod)
                              [:missing :existing/pending] (kill-task api-client expected-state-dict pod)
                              [:missing :existing/need-to-fail] (kill-task api-client expected-state-dict pod)
                              [:missing :existing/succeeded] (remove-finalization-if-set-and-delete api-client expected-state-dict pod)
                              [:missing :existing/failed] (remove-finalization-if-set-and-delete api-client expected-state-dict pod))]
    (when-not (expected-state-equivalent? expected-state new-expected-state)
      (update-or-delete! expected-state-map pod-name new-expected-state)
      ; TODO: Recur. We hay have changed the expected state, so we should reprocess it.
      ))))

(defn pod-update
  "Update the existing state for a pod. Include some business logic to e.g., not change a state to the same value more than once.
  Invoked by callbacks from kubernetes."
  [{:keys [existing-state-map] :as kcc} ^V1Pod new-pod]
  (let [pod-name (api/V1Pod->name new-pod)]
    (locking (calculate-lock pod-name)
      (let [new-state {:pod new-pod :synthesized-state (api/pod->synthesized-pod-state new-pod)}
            old-state (get @existing-state-map pod-name)]
        (swap! existing-state-map assoc pod-name new-state)
        (when-not (existing-state-equivalent? old-state new-state)
          (process kcc pod-name))))))

(defn pod-deleted
  "Indicate that kubernetes does not have the pod. Invoked by callbacks from kubernetes."
  [{:keys [existing-state-map] :as kcc} ^V1Pod pod-deleted]
  (let [pod-name (api/V1Pod->name pod-deleted)]
    (locking (calculate-lock pod-name)
      (swap! existing-state-map dissoc pod-name)
      (process kcc pod-name))))

(defn update-expected-state
  "Update the expected state. Include some business logic to e.g., not change a state to the same value more than once. Marks any state changes Also has a lattice of state. Called externally and from state machine."
  [{:keys [expected-state-map] :as kcc} pod-name new-expected-state-dict]
  (locking (calculate-lock [pod-name])
    (let [old-state (get @expected-state-map pod-name)]
      (when-not (expected-state-equivalent? new-expected-state-dict old-state)
        (swap! expected-state-map assoc pod-name new-expected-state-dict)
        (process kcc pod-name)))))

(defn scan-process
  "Special verison of process run during scanning."
  [{:keys [api-client existing-state-map expected-state-map] :as kcc} pod-name]
  (locking (calculate-lock pod-name)
    (process kcc pod-name)))
