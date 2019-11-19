(ns cook.kubernetes.controller
  (:require [clojure.tools.logging :as log]
            [cook.datomic :as datomic]
            [cook.kubernetes.api :as api]
            [cook.mesos.sandbox :as sandbox]
            [cook.scheduler.scheduler :as scheduler])
  (:import (clojure.lang IAtom)
           (io.kubernetes.client.models V1Pod V1ContainerStatus V1PodStatus)))

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
  (= old-state new-state) ; TODO. This is not right. We need to tune/tweak this to suppress otherwise identical states so we don't spam.
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
  "Given a map atom, key, and value, if the value is not nil, set the key to the value in the map. If the value is nil, delete the key entirely."
  [^IAtom map-atom key value]
  (if (nil? value)
    (swap! map-atom dissoc key)
    (swap! map-atom assoc key value)))

(defn container-status->failure-reason
  "Maps kubernetes failure reasons to cook failure reasons"
  [^V1PodStatus pod-status ^V1ContainerStatus status]
  ; TODO map additional kubernetes failure reasons
  (let [terminated (-> status .getState .getTerminated)]
    (cond
      (= "OutOfMemory" (.getReason pod-status)) :reason-container-limitation-memory
      (= "Error" (.getReason terminated)) :reason-command-executor-failed
      (= "OOMKilled" (.getReason terminated)) :reason-container-limitation-memory
      :default (do
                 (log/warn "Unable to determine kubernetes failure reason for pod" {:pod-status pod-status
                                                                                    :container-status status})
                 :unknown))))

(defn write-status-to-datomic
  "Helper function for calling scheduler/write-status-to-datomic"
  [kcc mesos-status]
  (scheduler/write-status-to-datomic datomic/conn
                                     @(:pool->fenzo-atom kcc)
                                     mesos-status))

(defn- get-job-container-status
  "Extract the container status for the main cook job container (defined in api/cook-container-name-for-job).
  We use this because we need the actual state object to determine the failure reason for failed jobs."
  [^V1PodStatus pod-status]
  (->> pod-status
       .getContainerStatuses
       (filter (fn [container-status] (= api/cook-container-name-for-job
                                         (.getName container-status))))
       first))

(defn pod-has-just-completed
  "A pod has completed. So now we need to update the status in datomic and store the exit code."
  [kcc {:keys [synthesized-state pod] :as existing-state-dictionary}]
  (let [task-id (-> pod .getMetadata .getName)
        pod-status (.getStatus pod)
        ^V1ContainerStatus job-container-status (get-job-container-status pod-status)
        mesos-state (case (:state synthesized-state)
                      :pod/failed :task-failed
                      :pod/succeeded :task-finished)
        status {:task-id {:value task-id}
                :state mesos-state
                :reason (container-status->failure-reason pod-status job-container-status)}
        exit-code (-> job-container-status .getState .getTerminated .getExitCode)]
    (write-status-to-datomic kcc status)
    (sandbox/aggregate-exit-code (:exit-code-syncer-state kcc) task-id exit-code)
    {:expected-state :expected/completed}))

(defn prepare-expected-state-dict-for-logging
  ".toString on a pod is incredibly large. Make a version thats been elided."
  [expected-state-dict]
  (if (:launch-pod expected-state-dict)
    (assoc expected-state-dict :launch-pod [:elided-for-brevity])
    expected-state-dict))

(defn prepare-existing-state-dict-for-logging
  [existing-state-dict]
  (-> existing-state-dict
      (update-in [:synthesized-state :state] #(or % :missing))
      (dissoc :pod)))

(defn pod-has-started
  "A pod has started. So now we need to update the status in datomic."
  [kcc {:keys [pod] :as existing-state-dictionary}]
  (let [task-id (-> pod .getMetadata .getName)
        status {:task-id {:value task-id}
                :state :task-running}]
    (write-status-to-datomic kcc status)
    {:expected-state :expected/running}))

(defn pod-was-killed
  "A pod was killed. So now we need to update the status in datomic and store the exit code."
  [kcc pod-name]
  (let [task-id pod-name
        status {:task-id {:value task-id}
                :state :task-failed
                :reason :reason-command-executor-failed}]
    (write-status-to-datomic kcc status)
    (sandbox/aggregate-exit-code (:exit-code-syncer-state kcc) task-id 143)
    {:expected-state :expected/completed}))

(defn process
  "Visit this pod-name, processing the new level-state. Returns the new expected state. Returns
  empty dictionary to indicate that the result should be deleted. NOTE: Must be invoked with the lock."
  [{:keys [api-client existing-state-map expected-state-map name] :as kcc} ^String pod-name]
  (loop [{:keys [expected-state] :as expected-state-dict} (get @expected-state-map pod-name)
         {:keys [synthesized-state pod] :as existing-state-dict} (get @existing-state-map pod-name)]
    (log/info "In compute cluster" name ", processing pod" pod-name ";"
              "expected:" (prepare-expected-state-dict-for-logging expected-state-dict) ","
              "existing:" (prepare-existing-state-dict-for-logging existing-state-dict))
    ; TODO: We added an :expected/starting state to the machine, to represent when a pod is starting. We map instance.status/unknown to that state
    ; The todo is to add in cases for [:expected/starting *] for those other states.
    (let
      [new-expected-state-dict (case (vector (or expected-state :missing) (or (:state synthesized-state) :missing))
                                 [:expected/starting :missing] (launch-task api-client expected-state-dict)
                                 [:expected/starting :pod/running] (pod-has-started kcc existing-state-dict)
                                 ; TODO We need to add cases for the other [:expected/starting *] states in.
                                 [:expected/starting :pod/waiting] expected-state-dict
                                 [:expected/starting :pod/succeeded] (pod-has-just-completed kcc existing-state-dict)
                                 [:expected/starting :pod/failed] (pod-has-just-completed kcc existing-state-dict)
                                 [:expected/running :pod/running] expected-state-dict
                                 [:expected/running :pod/succeeded] (pod-has-just-completed kcc existing-state-dict)
                                 [:expected/completed :pod/succeeded] (remove-finalization-if-set-and-delete api-client expected-state-dict pod)
                                 [:expected/running :pod/failed] (pod-has-just-completed kcc existing-state-dict)
                                 [:expected/completed :pod/failed] (remove-finalization-if-set-and-delete api-client expected-state-dict pod)
                                 [:expected/completed :missing] nil ; Cause it to be deleted.
                                 [:expected/killed :pod/waiting] (kill-task api-client expected-state-dict pod)
                                 [:expected/killed :pod/running] (kill-task api-client expected-state-dict pod)

                                 [:expected/killed :pod/succeeded] (do
                                                                     (pod-has-just-completed kcc existing-state-dict)
                                                                     (remove-finalization-if-set-and-delete api-client expected-state-dict pod))
                                 [:expected/killed :pod/failed]
                                 (do
                                   (pod-has-just-completed kcc existing-state-dict)
                                   (remove-finalization-if-set-and-delete api-client expected-state-dict pod))
                                 [:expected/killed :missing] (pod-was-killed kcc pod-name)
                                 ; TODO: Implement :pod/unknown cases.
                                 ; TODO: Implement :pod/pending cases.
                                 ; TODO: Implement :pod/need-to-fail cases.
                                 [:missing :pod/running] (kill-task api-client expected-state-dict pod)
                                 [:missing :pod/pending] (kill-task api-client expected-state-dict pod)
                                 [:missing :pod/need-to-fail] (kill-task api-client expected-state-dict pod)
                                 [:missing :pod/succeeded] (remove-finalization-if-set-and-delete api-client expected-state-dict pod)
                                 [:missing :pod/failed] (remove-finalization-if-set-and-delete api-client expected-state-dict pod)
                                 [:missing :missing] nil ; this can come up due to the recur at the end
                                 (do
                                   (log/error "Unexpected state: "
                                              (vector (or expected-state :missing) (or (:state synthesized-state) :missing))
                                              "for pod" pod-name)
                                   expected-state-dict))]
      (when-not (expected-state-equivalent? expected-state-dict new-expected-state-dict)
        (update-or-delete! expected-state-map pod-name new-expected-state-dict)
        (log/info "Processing: WANT TO RECUR")
        (recur new-expected-state-dict existing-state-dict)
        ; TODO: Recur. We hay have changed the expected state, so we should reprocess it.
        ))))

(defn pod-update
  "Update the existing state for a pod. Include some business logic to e.g., not change a state to the same value more than once.
  Invoked by callbacks from kubernetes."
  [{:keys [existing-state-map name] :as kcc} ^V1Pod new-pod]
  (let [pod-name (api/V1Pod->name new-pod)]
    (locking (calculate-lock pod-name)
      (let [new-state {:pod new-pod :synthesized-state (api/pod->synthesized-pod-state new-pod)}
            old-state (get @existing-state-map pod-name)]
        ; We always store the updated state, but only reprocess it if it is genuinely different.
        (swap! existing-state-map assoc pod-name new-state)
        (when-not (existing-state-equivalent? old-state new-state)
          (try
            (process kcc pod-name)
            (catch Exception e
              (log/error e (str "In compute-cluster " name ", error while processing pod-update for " pod-name)))))))))

(defn pod-deleted
  "Indicate that kubernetes does not have the pod. Invoked by callbacks from kubernetes."
  [{:keys [existing-state-map name] :as kcc} ^V1Pod pod-deleted]
  (let [pod-name (api/V1Pod->name pod-deleted)]
    (log/info "In compute cluster" name ", detected pod" pod-name "deleted")
    (locking (calculate-lock pod-name)
      (swap! existing-state-map dissoc pod-name)
      (try
        (process kcc pod-name)
        (catch Exception e
          (log/error e (str "In compute-cluster " name ", error while processing pod-delete for " pod-name)))))))


(defn update-expected-state
  "Update the expected state. Include some business logic to e.g., not change a state to the same value more than once. Marks any state changes Also has a lattice of state. Called externally and from state machine."
  [{:keys [expected-state-map] :as kcc} pod-name new-expected-state-dict]
  (locking (calculate-lock [pod-name])
    (let [old-state (get @expected-state-map pod-name)]
      (when-not (expected-state-equivalent? new-expected-state-dict old-state)
        (swap! expected-state-map assoc pod-name new-expected-state-dict)
        (process kcc pod-name)))))

(defn starting-namespaced-pod-name->pod
  "Returns a map from {:namespace pod-namespace :name pod-name}->pod for all tasks that we're attempting to send to
   kubernetes to start."
  [{:keys [expected-state-map] :as kcc}]
  (->> @expected-state-map
       (filter (fn [[_ {:keys [expected-state launch-pod]}]]
                 (and (= :expected/starting expected-state)
                      (some? (:pod launch-pod)))))
       (map (fn [[_ {:keys [launch-pod]}]]
              (let [{:keys [pod]} launch-pod]
                [(api/get-pod-namespaced-key pod) pod])))
       (into {})))

(defn scan-process
  "Special verison of process run during scanning. It grabs the lock before processing the pod."
  [{:keys [api-client existing-state-map expected-state-map] :as kcc} pod-name]
  (locking (calculate-lock pod-name)
    (process kcc pod-name)))
