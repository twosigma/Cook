(ns cook.kubernetes.controller
  (:require [cook.config :as config]
            [cook.datomic :as datomic]
            [cook.kubernetes.api :as api]
            [cook.mesos.sandbox :as sandbox]
            [cook.scheduler.scheduler :as scheduler]
            [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import (clojure.lang IAtom)
           (io.kubernetes.client.models V1Pod V1ContainerStatus V1PodStatus)
           (java.net URLEncoder)))

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
  ; TODO:
  ; This is not right. We need to tune/tweak this to
  ; suppress otherwise identical states so we don't spam.
  (= old-state new-state))

(defn delete-task
  "Kill task is the same as deleting a task. I semantically distinguish them. Delete is used for completed tasks that
  we're done with. Kill is used for possibly running tasks we want to kill so that they fail. Returns a new expected state
  dict of nil."
  [api-client pod]
  (api/delete-pod api-client pod)
  nil) ; Return new expected state dict of nil.

(defn kill-task
  "Kill task is the same as deleting a task. I semantically distinguish them. Delete is used for completed tasks that
  we're done with. Kill is used for possibly running tasks we want to kill so that they fail. Returns the
  expected-state-dict passed in."
  [api-client expected-state-dict pod]
  (api/delete-pod api-client pod)
  expected-state-dict)

(defn log-weird-state
  "Weird. This pod is in a weird state. Log its weird state."
  [expected-state-dict existing-state-dict]
  (log/error "Pod in a weird state:" expected-state-dict "and existing state" existing-state-dict))

(defn kill-task-weird
  "We're in a weird state that shouldn't occur with any of the normal expected races. This 'shouldn't occur. However,
  we're going to pessimistically assume that anything that could happen will, whether it shouldn't or not. Returns
  the expected-state-dict passed in."
  [api-client expected-state-dict {:keys [pod] :as existing-state-dict}]
  (log-weird-state expected-state-dict existing-state-dict)
  (kill-task api-client expected-state-dict pod))

(defn launch-task
  [api-client expected-state-dict]
  (api/launch-task api-client expected-state-dict)
  ; TODO: Should detect if we don't have a :launch-pod key and force a mea culpa retry and :expected/killed so that we retry.
  expected-state-dict)

(defn update-or-delete!
  "Given a map atom, key, and value, if the value is not nil, set the key to the value in the map.
  If the value is nil, delete the key entirely."
  [^IAtom map-atom key value]
  (if (nil? value)
    (swap! map-atom dissoc key)
    (swap! map-atom assoc key value)))

(defn container-status->failure-reason
  "Maps kubernetes failure reasons to cook failure reasons"
  [{:keys [name]} task-id ^V1PodStatus pod-status ^V1ContainerStatus container-status]
  ; TODO map additional kubernetes failure reasons
  (let [container-terminated-reason (some-> container-status .getState .getTerminated .getReason)
        pod-status-reason (.getReason pod-status)]
    (cond
      (= "OutOfMemory" pod-status-reason) :reason-container-limitation-memory
      (= "Error" container-terminated-reason) :reason-command-executor-failed
      (= "OOMKilled" container-terminated-reason) :reason-container-limitation-memory

      ; If there is no container status and the pod status reason is Outofcpu,
      ; then the node didn't have enough CPUs to start the container
      (and (nil? container-status) (= "OutOfcpu" pod-status-reason))
      (do
        (log/info "In compute cluster" name ", encountered OutOfcpu pod status reason for" task-id)
        :reason-invalid-offers)

      :default
      (do
        (log/warn "In compute cluster" name ", unable to determine failure reason for" task-id
                  {:pod-status pod-status :container-status container-status})
        :unknown))))

(defn write-status-to-datomic
  "Helper function for calling scheduler/write-status-to-datomic"
  [compute-cluster mesos-status]
  (scheduler/write-status-to-datomic datomic/conn
                                     @(:pool->fenzo-atom compute-cluster)
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
  "A pod has completed, or we're treating it as completed. E.g., it may really be running, but something is weird.

  This is supposed to look at the pod status, update datomic (with success, failure, and possibly mea culpa),
   and return a new expected-state of :expected/completed."
  [compute-cluster {:keys [synthesized-state pod] :as existing-state-dictionary}]
  (let [task-id (-> pod .getMetadata .getName)
        pod-status (.getStatus pod)
        ^V1ContainerStatus job-container-status (get-job-container-status pod-status)
        task-state (case (:state synthesized-state)
                     :pod/failed :task-failed
                     :pod/succeeded :task-finished
                     :pod/unknown :task-failed
                     :pod/waiting :task-failed ; Handle the (:expected/running,:pod/waiting) case.
                     nil :task-failed)
        status {:task-id {:value task-id}
                :state task-state
                :reason (container-status->failure-reason compute-cluster task-id pod-status job-container-status)}
        exit-code (some-> job-container-status .getState .getTerminated .getExitCode)]
    (write-status-to-datomic compute-cluster status)
    (when exit-code
      (sandbox/aggregate-exit-code (:exit-code-syncer-state compute-cluster) task-id exit-code))
    ; Must never return nil, we want it to return non-nil so that we will retry with writing the state to datomic in case we lose a race.
    ; The (completed,*) will cause use to delete the task, transitioning to (completed,missing), and thence to deleting from the map, to (missing,missing)
    {:expected-state :expected/completed}))

(defn prepare-expected-state-dict-for-logging
  ".toString on a pod is incredibly large. Make a version thats been elided."
  [expected-state-dict]
  (if (:launch-pod expected-state-dict)
    (assoc expected-state-dict :launch-pod [:elided-for-brevity])
    expected-state-dict))

(defn prepare-existing-state-dict-for-logging
  [{:keys [pod] :as existing-state-dict}]
  (try
    (-> existing-state-dict
        (update-in [:synthesized-state :state] #(or % :missing))
        (dissoc :pod)
        (assoc :pod-status (some-> pod .getStatus)))
    (catch Throwable t
      (log/error t "Error preparing existing state for logging:" existing-state-dict)
      existing-state-dict)))

(defn pod-has-started
  "A pod has started. So now we need to update the status in datomic."
  [compute-cluster {:keys [pod] :as existing-state-dictionary}]
  (let [task-id (-> pod .getMetadata .getName)
        status {:task-id {:value task-id}
                :state :task-running}]
    (write-status-to-datomic compute-cluster status)
    {:expected-state :expected/running}))

(defn record-sandbox-url
  "Record the sandbox file server URL in datomic."
  [{:keys [pod]}]
  (let [task-id (-> pod .getMetadata .getName)
        pod-ip (-> pod .getStatus .getPodIP)
        {:keys [default-workdir sandbox-fileserver pod-ip->hostname-fn]} (config/kubernetes)
        sandbox-fileserver-port (:port sandbox-fileserver)
        sandbox-url (try
                      (when (and sandbox-fileserver-port (not (str/blank? pod-ip)))
                        (str "http://"
                             (pod-ip->hostname-fn pod-ip)
                             ":" sandbox-fileserver-port
                             "/files/read.json?path="
                             (URLEncoder/encode default-workdir "UTF-8")))
                      (catch Exception e
                        (log/debug e "Unable to retrieve directory path for" task-id)
                        nil))]
    (when sandbox-url (scheduler/write-sandbox-url-to-datomic datomic/conn task-id sandbox-url))))

(defn pod-was-killed
  "A pod was killed. So now we need to update the status in datomic and store the exit code."
  [compute-cluster pod-name]
  (let [task-id pod-name
        status {:task-id {:value task-id}
                :state :task-failed
                :reason :reason-command-executor-failed}]
    (write-status-to-datomic compute-cluster status)
    (sandbox/aggregate-exit-code (:exit-code-syncer-state compute-cluster) task-id 143)
    {:expected-state :expected/completed}))

(defn process
  "Visit this pod-name, processing the new level-state. Returns the new expected state. Returns
  empty dictionary to indicate that the result should be deleted. NOTE: Must be invoked with the lock."
  [{:keys [api-client existing-state-map expected-state-map name] :as compute-cluster} ^String pod-name]
  (loop [{:keys [expected-state] :as expected-state-dict} (get @expected-state-map pod-name)
         {:keys [synthesized-state pod] :as existing-state-dict} (get @existing-state-map pod-name)]
    (log/info "In compute cluster" name ", processing pod" pod-name ";"
              "expected:" (prepare-expected-state-dict-for-logging expected-state-dict) ","
              "existing:" (prepare-existing-state-dict-for-logging existing-state-dict))
    ; We should have the cross product of
    ;      :expected/starting :expected/running :expected/completed :expected/killed :missing
    ; and
    ;      :pod/waiting :pod/running :pod/succeeded :pod/failed :pod/unknown :missing
    ;
    ; for a total of 30 states.
    ;
    ; The only terminal expected states are :expected/completed and :missing
    ; The only terminal pod states are :pod/succeeded :pod/failed and :missing. We also treat :pod/unknown as a terminal state.

    ; If you ignore the reloading on startup, the initial state is set at (:starting, missing) when we first add a pod to launch.
    ; The final state is (:missing, :missing)

    ; Approach:
    ;   All of the :pod/unknown states go at the end. We should have 6 of them. I'll ignore their existance below.
    ;   We should have 5 matches for each expected state.
    ;
    ; We use expected/killed to represent a user-chosen kill. If we're doing a state-machine-induced kill (because something
    ; went wrong) it should occur by deleting the pod, so we go, e.g.,  (:running,:waiting) (an illegal state) to (:running,:missing)
    ; to (:completed, :missing), to (:missing,missing) to deleted. We put a flag on when we delete so that we can indicate
    ; the provenance (e.g., induced because of a weird state)
    ;
    ; Invariants:
    ;   pod-has-just-completed/pod-was-killed/pod-has-started: These are callbacks invoked when kubernetes has moved
    ;       to a new state. They handle writeback to datomic only. pod-was-killed is called in all of the weird
    ;       kubernetes states that are not expected to occur, while pod-has-just-completed is invoked in all of the
    ;       normal exit states (including exit-with-failure). I.e., these MUST only be invoked if expected state is not
    ;       in a terminal state. These functions return new expected-state-dict's so should be invoked last in almost
    ;       all cases. (However, in a few cases, (delete-task ? ?) can be invoked afterwards. It deletes the key from
    ;       the expected state completely. Should only be invoked if we're nil.)
    ;
    ;   weird can be called at any time to indicate we're in a wierd state and extra logging is warranted.
    ;
    ;   We always end with one of  pod-has-just-completed/pod-was-killed/pod-has-started or delete-task.
    ;
    ;   We always update datomic first (with pod-has-* pod-was-*), etc, then we update kubernetes so we can handle restarts.
    ;
    ;   (delete-task ? ?) is only valid when the kubernetes pod is in a terminal-state.
    ;
    ;   Note that we have release semantics. We could fail to process any operation. This means that, where relevant, we
    ;     first write to datomic, then change into the next step. We thus show some intermediate states in the machine
    ;     that seem like they could be skipped.
    ;
    ;   In the (:completed,*) states, we delete from datomic, transitioning into (:completed,:missing) and thence to (:missing, :missing)
    ;     When we're in a terminal pod state.
    ;
    ;   We only delete a pod if and only if the pod is in a terminal (or unknown) state.
    ; In some cases, we may mark mea culpa retries (if e.g., the node got reclaimed, so the pod got killed). In weird cases, we always need to mark mea culpa retries.
    (let
      [new-expected-state-dict (case (vector (or expected-state :missing) (or (:state synthesized-state) :missing))
                                 [:expected/starting :missing] (launch-task api-client expected-state-dict)
                                 [:expected/starting :pod/running] (pod-has-started compute-cluster existing-state-dict)
                                 [:expected/starting :pod/waiting] expected-state-dict ; Its starting. Can be stuck here. TODO: Stuck state detector to detect being stuck.
                                 [:expected/starting :pod/succeeded] (pod-has-just-completed compute-cluster existing-state-dict) ; Finished fast.
                                 [:expected/starting :pod/failed] (pod-has-just-completed compute-cluster existing-state-dict) ; TODO: May need to mark mea culpa retry
                                 [:expected/running :pod/running] expected-state-dict
                                 [:expected/running :pod/succeeded] (pod-has-just-completed compute-cluster existing-state-dict)

                                 ; This is an exception. The writeback to datomic has occurred, so there's nothing to do except to delete the task from kubernetes
                                 ; and remove it from our tracking.
                                 [:expected/completed :pod/succeeded] (delete-task api-client pod)

                                 [:expected/running :pod/failed] (pod-has-just-completed compute-cluster existing-state-dict) ; TODO: May need to mark mea culpa retry
                                 [:expected/running :pod/waiting] (do ; This case is weird.
                                                                    ; This breaks our rule of calling pod-has-completed on a non-terminal pod state.
                                                                    (kill-task-weird api-client expected-state-dict existing-state-dict)
                                                                    (pod-has-just-completed compute-cluster existing-state-dict)) ; TODO: Should mark mea culpa retry

                                 [:expected/completed :pod/failed] (delete-task api-client pod)
                                 [:expected/completed :missing] nil ; Cause it to be deleted.
                                 [:expected/killed :pod/waiting] (kill-task api-client expected-state-dict pod)
                                 [:expected/killed :pod/running] (kill-task api-client expected-state-dict pod)

                                 [:expected/killed :pod/succeeded] (do ; There was a race and it completed normally before being it was killed.
                                                                     (pod-has-just-completed compute-cluster existing-state-dict))
                                 [:expected/killed :pod/failed] (pod-has-just-completed compute-cluster existing-state-dict)
                                 [:expected/killed :missing] (do ; Weird. We always update datomic first. Could happen if someone manually removed stuff from kubernetes.
                                                               (log-weird-state expected-state-dict existing-state-dict)
                                                               (pod-was-killed compute-cluster pod-name))

                                 ; These cases are weird.
                                 [:expected/completed :pod/waiting] (kill-task-weird api-client expected-state-dict existing-state-dict)
                                 [:expected/completed :pod/running] (kill-task-weird api-client expected-state-dict existing-state-dict)

                                 ; This is interesting. This indicates that something deleted it behind our back!
                                 ; Other 3 cases of [{completed,starting,killed}, :missing] already handled.
                                 [:expected/running :missing] (do
                                                                (log/error "Something deleted" pod-name "behind our back")
                                                                (pod-has-just-completed compute-cluster existing-state-dict)) ; TODO: Should mark mea culpa retry

                                 ; This block of :missing,* cases can only occur in testing when you're e.g., blowing away the database. 
                                 ; The next two will go through :missing,running or :missing,waiting to :missing,failed and then be deleted.
                                 ; TODO: These may be evidence of a bug where we process pod changes when we're starting up.
                                 [:missing :pod/running] (kill-task-weird api-client nil pod)
                                 [:missing :pod/waiting] (kill-task-weird api-client nil pod)

                                 ; We shouldn't hit these unless we get a database rollback.
                                 [:missing :pod/succeeded] (kill-task-weird api-client nil pod)
                                 [:missing :pod/failed] (kill-task-weird api-client nil pod)

                                 [:missing :pod/unknown] (kill-task-weird api-client expected-state-dict existing-state-dict)
                                 [:expected/starting :pod/unknown] (kill-task-weird api-client (pod-has-just-completed compute-cluster existing-state-dict) pod) ; TODO: Should mark mea culpa retry
                                 [:expected/running :pod/unknown] (kill-task-weird api-client (pod-has-just-completed compute-cluster existing-state-dict) pod) ; TODO: Should mark mea culpa retry
                                 [:expected/killed :pod/unknown] (kill-task-weird api-client (pod-has-just-completed compute-cluster existing-state-dict) pod) ; TODO: Should mark mea culpa retry
                                 [:expected/completed :pod/unknown] (kill-task-weird api-client (pod-has-just-completed compute-cluster existing-state-dict) pod) ; TODO: Should mark mea culpa retry

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
  [{:keys [existing-state-map name] :as compute-cluster} ^V1Pod new-pod]
  (let [pod-name (api/V1Pod->name new-pod)]
    (locking (calculate-lock pod-name)
      (let [new-state {:pod new-pod
                       :synthesized-state (api/pod->synthesized-pod-state new-pod)
                       :sandbox-file-server-container-state (api/pod->sandbox-file-server-container-state new-pod)}
            old-state (get @existing-state-map pod-name)]
        ; We always store the updated state, but only reprocess it if it is genuinely different.
        (swap! existing-state-map assoc pod-name new-state)
        (let [new-file-server-state (:sandbox-file-server-container-state new-state)
              old-file-server-state (:sandbox-file-server-container-state old-state)]
          (when (and (= new-file-server-state :running) (not= old-file-server-state :running))
            (record-sandbox-url new-state)))
        (when-not (existing-state-equivalent? old-state new-state)
          (try
            (process compute-cluster pod-name)
            (catch Exception e
              (log/error e (str "In compute-cluster " name ", error while processing pod-update for " pod-name)))))))))

(defn pod-deleted
  "Indicate that kubernetes does not have the pod. Invoked by callbacks from kubernetes."
  [{:keys [existing-state-map name] :as compute-cluster} ^V1Pod pod-deleted]
  (let [pod-name (api/V1Pod->name pod-deleted)]
    (log/info "In compute cluster" name ", detected pod" pod-name "deleted")
    (locking (calculate-lock pod-name)
      (swap! existing-state-map dissoc pod-name)
      (try
        (process compute-cluster pod-name)
        (catch Exception e
          (log/error e (str "In compute-cluster " name ", error while processing pod-delete for " pod-name)))))))


(defn update-expected-state
  "Update the expected state. Include some business logic to e.g., not change a state to the same value more than once. Marks any state changes Also has a lattice of state. Called externally and from state machine."
  [{:keys [expected-state-map] :as compute-cluster} pod-name new-expected-state-dict]
  (locking (calculate-lock [pod-name])
    (let [old-state (get @expected-state-map pod-name)]
      (when-not (expected-state-equivalent? new-expected-state-dict old-state)
        (swap! expected-state-map assoc pod-name new-expected-state-dict)
        (process compute-cluster pod-name)))))

(defn starting-namespaced-pod-name->pod
  "Returns a map from {:namespace pod-namespace :name pod-name}->pod for all tasks that we're attempting to send to
   kubernetes to start."
  [{:keys [expected-state-map] :as compute-cluster}]
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
  [{:keys [api-client existing-state-map expected-state-map] :as compute-cluster} pod-name]
  (locking (calculate-lock pod-name)
    (process compute-cluster pod-name)))
