(ns cook.kubernetes.controller
  (:require [clojure.tools.logging :as log]
            [cook.kubernetes.api :as api])
  (:import (io.kubernetes.client.models V1Pod)
           (clojure.lang IAtom)))

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
  (= old-state new-state); TODO
  )

(defn existing-state-equivalent?
  "Is the old and new state equivalent?"
  [old-state new-state]
  (= old-state new-state) ; TODO. This is not right. We need to tune/tweak this to suppress otherwise identical states so we dno't spam.
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
  [^IAtom map-atom key value]
  (if (nil? value)
    (swap! map-atom dissoc key)
    (swap! map-atom assoc key value)))

;; REVIEW: Review handle-status-update
(defn pod-has-just-completed
  "A pod has completed."
  [{:keys [synthesized-state pod] :as existing-state-dictionary}]
  (case (:state synthesized-state)
    :pod/failed
    (do
      ; TODO: Extract failure reason, etc and store in datomic, via refactored handle-status-update.
      {:expected-state :expected/completed})
    :pod/succeeded
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
  "A pod has started."
  [{:keys [synthesized-state pod] :as existing-state-dictionary}]
  ;(api/TODO); TODO Update datomic state to instance.state/running.
  {:expected-state :expected/running})

(defn process
  "Visit this pod-name, processing the new level-state. Returns the new expected state. Returns
  empty dictionary to indicate that the result should be deleted. NOTE: Must be invoked with the lock."
  [{:keys [api-client existing-state-map expected-state-map] :as kcc} pod-name]
  (loop [{:keys [expected-state] :as expected-state-dict} (get @expected-state-map pod-name)
         {:keys [synthesized-state pod] :as existing-state-dict} (get @existing-state-map pod-name)]
    (let
      [cooked-expected-state (or expected-state :missing)
       cooked-existing-state (or (:state synthesized-state) :missing)]
      (log/info "Processing: " pod-name ": ((" (prepare-expected-state-dict-for-logging expected-state-dict) " ===== " cooked-existing-state  "///" existing-state-dict  "))")
      ; TODO: We added an :expected/starting state to the machine, to represent when a pod is starting. We map instance.status/unknown to that state
      ; The todo is to add in cases for [:expected/starting *] for those other states.
      (let
        [new-expected-state-dict (case (vector cooked-expected-state cooked-existing-state)
                                   [:expected/starting :missing] (launch-task api-client expected-state-dict)
                                   [:expected/starting :pod/running] (pod-has-started existing-state-dict)
                                   ; TODO We need to add cases for the other [:expected/starting *] states in.
                                   [:expected/starting :pod/waiting] expected-state-dict
                                   [:expected/starting :pod/succeeded] (pod-has-just-completed existing-state-dict)
                                   [:expected/starting :pod/failed] (pod-has-just-completed existing-state-dict)
                                   [:expected/running :pod/running] expected-state-dict
                                   [:expected/running :pod/succeeded] (pod-has-just-completed existing-state-dict)
                                   [:expected/completed :pod/succeeded] (remove-finalization-if-set-and-delete api-client expected-state-dict pod)
                                   [:expected/running :pod/failed] (pod-has-just-completed existing-state-dict)
                                   [:expected/completed :pod/failed] (remove-finalization-if-set-and-delete api-client expected-state-dict pod)
                                   [:expected/completed :missing] nil ; Cause it to be deleted.
                                   [:expected/killed :pod/running] (kill-task api-client expected-state-dict pod) ; TODO: Where does the datomic update occur? Do we do it when we do [expected/killed :pod/failed], to be similar to mesos, we update it only when the backend says its dead?

                                   [:expected/killed :pod/succeeded] (remove-finalization-if-set-and-delete api-client expected-state-dict pod)
                                   [:expected/killed :pod/failed]
                                   (do ; TODO: Invoke handle-status-update.
                                     (remove-finalization-if-set-and-delete api-client expected-state-dict pod))
                                   [:expected/killed :missing] nil
                                   ; TODO: Implement :pod/unknown cases.
                                   ; TODO: Implement :pod/pending cases.
                                   ; TODO: Implement :pod/need-to-fail cases.
                                   [:missing :pod/running] (kill-task api-client expected-state-dict pod)
                                   [:missing :pod/pending] (kill-task api-client expected-state-dict pod)
                                   [:missing :pod/need-to-fail] (kill-task api-client expected-state-dict pod)
                                   [:missing :pod/succeeded] (remove-finalization-if-set-and-delete api-client expected-state-dict pod)
                                   [:missing :pod/failed] (remove-finalization-if-set-and-delete api-client expected-state-dict pod))]
        (when-not (expected-state-equivalent? expected-state-dict new-expected-state-dict)
          (update-or-delete! expected-state-map pod-name new-expected-state-dict)
          (log/info "Processing: WANT TO RECUR" new-expected-state-dict " ---- " existing-state-dict)
          (recur new-expected-state-dict existing-state-dict)
          ; TODO: Recur. We hay have changed the expected state, so we should reprocess it.
          )))))

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
