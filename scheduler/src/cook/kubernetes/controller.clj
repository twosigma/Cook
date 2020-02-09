(ns cook.kubernetes.controller
  (:require [cook.config :as config]
            [cook.datomic :as datomic]
            [cook.kubernetes.api :as api]
            [cook.mesos.sandbox :as sandbox]
            [cook.scheduler.scheduler :as scheduler]
            [cook.util :as util]
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

(defn cook-expected-state-equivalent?
  "Is the old and new state equivalent?"
  [old-state new-state]
  (= old-state new-state); TODO
  )

(defn k8s-actual-state-equivalent?
  "Is the old and new state equivalent?"
  [old-state new-state]
  ; TODO:
  ; This is not right. We need to tune/tweak this to
  ; suppress otherwise identical states so we don't spam.
  (= old-state new-state))

(defn delete-pod
  "Kill pod is the same as deleting a pod, we semantically distinguish the two operations.
  Delete is used for completed pods that we're done with.
  Kill is used for possibly running pods we want to kill so that they fail.
  Returns the cook-expected-state-dict passed in."
  [api-client cook-expected-state-dict pod]
  (api/delete-pod api-client pod)
  cook-expected-state-dict)

(defn kill-pod
  "Kill pod is the same as deleting a pod, we semantically distinguish the two operations.
  Delete is used for completed pods that we're done with.
  Kill is used for possibly running pods we want to kill so that they fail.
  Returns the cook-expected-state-dict passed in."
  [api-client cook-expected-state-dict pod]
  (api/delete-pod api-client pod)
  cook-expected-state-dict)

(defn prepare-cook-expected-state-dict-for-logging
  ".toString on a pod is incredibly large. Make a version thats been elided."
  [cook-expected-state-dict]
  (if (:launch-pod cook-expected-state-dict)
    (assoc cook-expected-state-dict :launch-pod [:elided-for-brevity])
    cook-expected-state-dict))

(defn prepare-k8s-actual-state-dict-for-logging
  [{:keys [pod] :as k8s-actual-state-dict}]
  (try
    (-> k8s-actual-state-dict
        (update-in [:synthesized-state :state] #(or % :missing))
        (dissoc :pod)
        (assoc :pod-status (some-> pod .getStatus)))
    (catch Throwable t
      (log/error t "Error preparing k8s actual state for logging:" k8s-actual-state-dict)
      k8s-actual-state-dict)))

(defn log-weird-state
  "This pod is in a weird state. Log so that we can later trace if we want to."
  [{:keys [name]} pod-name cook-expected-state-dict k8s-actual-state-dict]
  (log/info "In compute cluster" name ", pod" pod-name "is in a weird state; cook expected:"
            (prepare-cook-expected-state-dict-for-logging cook-expected-state-dict)
            "and k8s actual:"
            (prepare-k8s-actual-state-dict-for-logging k8s-actual-state-dict)))

(defn kill-pod-in-weird-state
  "We're in a weird state that shouldn't occur with any of the normal expected races. This shouldn't occur. However,
  we're going to pessimistically assume that anything that could happen will, whether it should or not. Returns
  the cook-expected-state-dict passed in."
  [{:keys [api-client] :as compute-cluster} pod-name cook-expected-state-dict {:keys [pod] :as k8s-actual-state-dict}]
  (log-weird-state compute-cluster pod-name cook-expected-state-dict k8s-actual-state-dict)
  (kill-pod api-client cook-expected-state-dict pod))

(defn write-status-to-datomic
  "Helper function for calling scheduler/write-status-to-datomic"
  [compute-cluster mesos-status]
  (scheduler/write-status-to-datomic datomic/conn
                                     @(:pool->fenzo-atom compute-cluster)
                                     mesos-status))

(defn handle-pod-submission-failed
  "Marks the corresponding job instance as failed in the database and
  returns the `completed` cook expected state"
  [{:keys [name] :as compute-cluster} pod-name]
  (log/info "In compute cluster" name ", pod" pod-name "submission failed")
  (let [instance-id pod-name
        status {:reason :reason-task-invalid
                :state :task-failed
                :task-id {:value instance-id}}]
    (write-status-to-datomic compute-cluster status)
    {:cook-expected-state :cook-expected-state/completed}))

(defn launch-pod
  [{:keys [api-client] :as compute-cluster} cook-expected-state-dict pod-name]
  (if (api/launch-pod api-client cook-expected-state-dict pod-name)
    cook-expected-state-dict
    (handle-pod-submission-failed compute-cluster pod-name)))

(defn update-or-delete!
  "Given a map atom, key, and value, if the value is not nil, set the key to the value in the map.
  If the value is nil, delete the key entirely."
  [^IAtom map-atom key value]
  (if (nil? value)
    (swap! map-atom dissoc key)
    (swap! map-atom assoc key value)))

(defn container-status->failure-reason
  "Maps kubernetes failure reasons to cook failure reasons"
  [{:keys [name]} instance-id ^V1PodStatus pod-status ^V1ContainerStatus container-status]
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
        (log/info "In compute cluster" name ", encountered OutOfcpu pod status reason for" instance-id)
        :reason-invalid-offers)

      :default
      (do
        (log/warn "In compute cluster" name ", unable to determine failure reason for" instance-id
                  {:pod-status pod-status :container-status container-status})
        :unknown))))

(defn- get-job-container-status
  "Extract the container status for the main cook job container (defined in api/cook-container-name-for-job).
  We use this because we need the actual state object to determine the failure reason for failed jobs."
  [^V1PodStatus pod-status]
  (->> pod-status
       .getContainerStatuses
       (filter (fn [container-status] (= api/cook-container-name-for-job
                                         (.getName container-status))))
       first))

(defn handle-pod-completed
  "A pod has completed, or we're treating it as completed. E.g., it may really be running, but something is weird.

   Looks at the pod status, updates datomic (with success, failure, and possibly mea culpa),
   and return a new cook expected state of :cook-expected-state/completed."
  [compute-cluster {:keys [synthesized-state pod]} & {:keys [reason]}]
  (let [instance-id (-> pod .getMetadata .getName)
        pod-status (.getStatus pod)
        ^V1ContainerStatus job-container-status (get-job-container-status pod-status)
        ; We leak mesos terminology here ('task') because of backward compatibility.
        task-state (if (= (:state synthesized-state) :pod/succeeded)
                     :task-finished
                     :task-failed)
        status {:task-id {:value instance-id}
                :state task-state
                :reason (or reason
                            (container-status->failure-reason compute-cluster instance-id
                                                              pod-status job-container-status))}
        exit-code (some-> job-container-status .getState .getTerminated .getExitCode)]
    (write-status-to-datomic compute-cluster status)
    (when exit-code
      (sandbox/aggregate-exit-code (:exit-code-syncer-state compute-cluster) instance-id exit-code))
    ; Must never return nil, we want it to return non-nil so that we will retry with writing the state to datomic in case we lose a race.
    ; Being in the (completed,*) state, will cause us to delete the pod, transitioning to (completed,missing), and thence
    ; to deleting from the map, into (missing,missing) state.
    {:cook-expected-state :cook-expected-state/completed}))

(defn handle-pod-started
  "A pod has started. So now we need to update the status in datomic."
  [compute-cluster {:keys [pod]}]
  (let [instance-id (-> pod .getMetadata .getName)
        ; We leak mesos terminology here ('task') because of backward compatibility.
        status {:task-id {:value instance-id}
                :state :task-running}]
    (write-status-to-datomic compute-cluster status)
    {:cook-expected-state :cook-expected-state/running}))

(def get-pod-ip->hostname-fn
  (memoize
    (fn [pod-ip->hostname-fn]
      (if pod-ip->hostname-fn (util/lazy-load-var pod-ip->hostname-fn) identity))))

(defn record-sandbox-url
  "Record the sandbox file server URL in datomic."
  [{:keys [pod]}]
  (let [task-id (-> pod .getMetadata .getName)
        pod-ip (-> pod .getStatus .getPodIP)
        {:keys [default-workdir pod-ip->hostname-fn sidecar]} (config/kubernetes)
        sandbox-fileserver-port (:port sidecar)
        sandbox-url (try
                      (when (and sandbox-fileserver-port (not (str/blank? pod-ip)))
                        (str "http://"
                             ((get-pod-ip->hostname-fn pod-ip->hostname-fn) pod-ip)
                             ":" sandbox-fileserver-port
                             "/files/read.json?path="
                             (URLEncoder/encode default-workdir "UTF-8")))
                      (catch Exception e
                        (log/debug e "Unable to retrieve directory path for" task-id)
                        nil))]
    (when sandbox-url (scheduler/write-sandbox-url-to-datomic datomic/conn task-id sandbox-url))))

(defn handle-pod-killed
  "A pod was killed. So now we need to update the status in datomic and store the exit code."
  [compute-cluster pod-name]
  (let [instance-id pod-name
        ; We leak mesos terminology here ('task') because of backward compatibility.
        status {:task-id {:value instance-id}
                :state :task-failed
                :reason :reason-command-executor-failed}]
    (write-status-to-datomic compute-cluster status)
    (sandbox/aggregate-exit-code (:exit-code-syncer-state compute-cluster) instance-id 143)
    {:cook-expected-state :cook-expected-state/completed}))

(defn handle-pod-completed-and-kill-pod-in-weird-state
  "Writes the completed state to datomic and deletes the pod in kubernetes.
  It is unusual (and unique) because it both modifies kubernetes and modifies datomic. It is intended
  only to be invoked in pods in state :k8s-actual-state/unknown and handle their recovery."
  [compute-cluster pod-name k8s-actual-state-dict]
  ; TODO: Should mark mea culpa retry
  (kill-pod-in-weird-state compute-cluster pod-name
                           (handle-pod-completed compute-cluster k8s-actual-state-dict) k8s-actual-state-dict))

(defn process
  "Visit this pod-name, processing the new level-state. Returns the new cook expected state. Returns
  empty dictionary to indicate that the result should be deleted. NOTE: Must be invoked with the lock."
  [{:keys [api-client k8s-actual-state-map cook-expected-state-map name] :as compute-cluster} ^String pod-name]
  (loop [{:keys [cook-expected-state] :as cook-expected-state-dict} (get @cook-expected-state-map pod-name)
         {:keys [synthesized-state pod] :as k8s-actual-state-dict} (get @k8s-actual-state-map pod-name)]
    (log/info "In compute cluster" name ", processing pod" pod-name ";"
              "cook-expected:" (prepare-cook-expected-state-dict-for-logging cook-expected-state-dict) ","
              "k8s-actual:" (prepare-k8s-actual-state-dict-for-logging k8s-actual-state-dict))
    ; We should have the cross product of
    ;      :cook-expected-state/starting :cook-expected-state/running :cook-expected-state/completed :cook-expected-state/killed :missing
    ; and
    ;      :pod/waiting :pod/running :pod/succeeded :pod/failed :pod/unknown :missing
    ;
    ; for a total of 30 states.
    ;
    ; The only terminal expected states are :cook-expected-state/completed and :missing
    ; The only terminal pod states are :pod/succeeded :pod/failed and :missing. We also treat :pod/unknown as a terminal state.
    ;
    ; We don't know if :pod/unknown is a terminal state for kubernetes. We treat it as a terminal state as far
    ; as the state machine is concerned, and force a retry at the cook level.
    ;
    ; If you ignore the reloading on startup, the initial state is set at (:cook-expected-state/starting, :missing) when we first add a pod to launch.
    ; The final state is (:missing, :missing)
    ;
    ; We use :cook-expected-state/killed to represent a user-chosen kill. If we're doing a state-machine-induced kill (because something
    ; went wrong) it should occur by deleting the pod, so we go, e.g.,  (:running,:waiting) (an illegal state) to (:running,:missing)
    ; to (:completed, :missing), to (:missing,missing) to deleted. We will put a flag in the cook expected state dictionary
    ; when we delete to indicate the provenance (e.g., induced because of a weird state)
    ;
    ; Invariants:
    ;   handle-pod-completed/handle-pod-killed/handle-pod-started: These are callbacks invoked when kubernetes has moved
    ;       to a new state. They handle writeback to datomic only. handle-pod-killed is called in all of the weird
    ;       kubernetes states that are not expected to occur, while handle-pod-completed is invoked in all of the
    ;       normal exit states (including exit-with-failure). I.e., these MUST only be invoked if cook expected state is not
    ;       in a terminal state. These functions return new cook-expected-state-dict's so should be invoked last in almost
    ;       all cases. (However, in a few cases, (delete-pod ? ?) can be invoked afterwards. It deletes the key from
    ;       the cook expected state completely. Should only be invoked if we're in a :missing cook-expected-state.)
    ;
    ;   log-weird-state can be called at any time to indicate we're in a weird state and extra logging is warranted.
    ;
    ;   We always end with one of handle-pod-completed/handle-pod-killed/handle-pod-started or delete-pod.
    ;
    ;   We always update datomic first (with pod-has-* pod-was-*), etc, then we update kubernetes so we can handle restarts.
    ;
    ;   (delete-pod ? ?) is only valid when the kubernetes pod is in a terminal-state.
    ;
    ;   Note that we have release semantics. We could fail to process any operation. This means that, where relevant, we
    ;     first write to datomic, then change into the next step. We thus show some intermediate states in the machine
    ;     that seem like they could be skipped.
    ;
    ;   In the (:completed,*) states, we delete from datomic, transitioning into (:completed,:missing) and thence to (:missing, :missing)
    ;     when we're in a terminal pod state.
    ;
    ;   We delete a pod if and only if the pod is in a terminal (or unknown) state.
    ; In some cases, we may mark mea culpa retries (if e.g., the node got reclaimed, so the pod got killed). In weird cases, we always need to mark mea culpa retries.
    (let
      [pod-synthesized-state-modified (or (:state synthesized-state) :missing)
       new-cook-expected-state-dict (case (or cook-expected-state :missing)
                                      :cook-expected-state/completed
                                      (case pod-synthesized-state-modified
                                        ; Cause this entry to be deleted by update-or-delete! called later down.
                                        :missing nil
                                        ; The writeback to datomic has occurred, so there's nothing to do except to delete the pod from kubernetes
                                        ; and remove it from our tracking.
                                        :pod/failed (delete-pod api-client cook-expected-state-dict pod)
                                        ; Who resurrected this pod? Where did it come from? Do we have two instances of cook?
                                        :pod/running (kill-pod-in-weird-state compute-cluster pod-name
                                                                              cook-expected-state-dict
                                                                              k8s-actual-state-dict)
                                        ; The writeback to datomic has occurred, so there's nothing to do except to delete the pod from kubernetes
                                        ; and remove it from our tracking.
                                        :pod/succeeded (delete-pod api-client cook-expected-state-dict pod)
                                        ; TODO: Should mark mea culpa retry
                                        :pod/unknown (handle-pod-completed-and-kill-pod-in-weird-state
                                                       compute-cluster pod-name k8s-actual-state-dict)
                                        ; Who resurrected this pod? Where did it come from? Do we have two instances of cook?
                                        :pod/waiting (kill-pod-in-weird-state compute-cluster pod-name
                                                                              cook-expected-state-dict
                                                                              k8s-actual-state-dict))

                                      :cook-expected-state/killed
                                      (case pod-synthesized-state-modified
                                        :missing
                                        ; There's a race where we can launch then kill, when the kill arrives after
                                        ; the launch, but before the watch updates k8s-actual-state.
                                        ; (k8s-actual-state isn't the actual state, because of this watch lag)
                                        ; When that happens, we will have an actual state of :missing.
                                        ; If we did nothing, we'd log this as a weird state, then when the watch
                                        ; shows up, we'd see (:missing, :starting) and log that as a weird state too.
                                        ;
                                        ; So, a better approach. If we detect a (:killed, :missing), then we opportunistically
                                        ; try to kill the pod. This is why update-cook-expected-state saves :launch-pod,
                                        ; so its available here.
                                        (if-let [pod (some-> cook-expected-state-dict :launch-pod :pod)]
                                          (do
                                            (log/info "In compute cluster" name ", opportunistically killing" pod-name
                                                      "because of potential race where kill arrives before the watch responds to the launch")
                                            (kill-pod api-client :ignored pod)
                                            ; This is needed to make sure if we take the opportunistic kill, we make
                                            ; sure to write the status to datomic. Recall we're in kubernetes state missing.
                                            (handle-pod-killed compute-cluster pod-name))
                                          (do
                                            ; We treat a deleting pod in kubernetes the same as a missing pod when coming up with a synthesized state.
                                            ; That's good for (almost) all parts of the system. However,
                                            ; If it is legitimately missing, then something weird is going on. If it is
                                            ; deleting, that's an expected state. So, let's be selective with our logging.
                                            (if (= (:state synthesized-state) :missing)
                                              (log/info "In compute cluster" name ", pod" pod-name
                                                        "was killed with cook expected state"
                                                        (prepare-cook-expected-state-dict-for-logging cook-expected-state-dict)
                                                        "and k8s actual state"
                                                        (prepare-k8s-actual-state-dict-for-logging k8s-actual-state-dict))
                                              (log-weird-state compute-cluster pod-name
                                                               cook-expected-state-dict k8s-actual-state-dict))
                                            (handle-pod-killed compute-cluster pod-name)))
                                        :pod/failed (handle-pod-completed compute-cluster k8s-actual-state-dict)
                                        :pod/running (kill-pod api-client cook-expected-state-dict pod)
                                        ; There was a race and it completed normally before being it was killed.
                                        :pod/succeeded (handle-pod-completed compute-cluster k8s-actual-state-dict)
                                        ; TODO: Should mark mea culpa retry
                                        :pod/unknown (handle-pod-completed-and-kill-pod-in-weird-state
                                                       compute-cluster pod-name k8s-actual-state-dict)
                                        :pod/waiting (kill-pod api-client cook-expected-state-dict pod))

                                      :cook-expected-state/running
                                      (case pod-synthesized-state-modified
                                        ; This indicates that something deleted it behind our back
                                        :missing (do
                                                   (log/error "In compute cluster" name ", something deleted"
                                                              pod-name "behind our back")
                                                   ; TODO: Should mark mea culpa retry
                                                   (handle-pod-completed compute-cluster k8s-actual-state-dict))
                                        ; TODO: May need to mark mea culpa retry
                                        :pod/failed (handle-pod-completed compute-cluster k8s-actual-state-dict)
                                        :pod/running cook-expected-state-dict
                                        :pod/succeeded (handle-pod-completed compute-cluster k8s-actual-state-dict)
                                        ; TODO: Should mark mea culpa retry
                                        :pod/unknown (handle-pod-completed-and-kill-pod-in-weird-state
                                                       compute-cluster pod-name k8s-actual-state-dict)
                                        ; This is a sign that our pod was moved to a new node.
                                        ;
                                        ; For example, on GKE preemptible VMs:
                                        ;  "there is no guarantee that Pods running on preemptible VMs
                                        ;   can always shutdown gracefully. It may take several minutes
                                        ;   for GKE to detect that the node was preempted and that the
                                        ;   Pods are no longer running, which will delay the rescheduling
                                        ;   of the Pods to a new node."
                                        ;
                                        ; (https://cloud.google.com/kubernetes-engine/docs/how-to/preemptible-vms#best_practices)
                                        :pod/waiting (do
                                                       (log/info "In compute cluster" name ", pod" pod-name
                                                                 "went into waiting while it was expected running")
                                                       (kill-pod api-client cook-expected-state-dict pod)
                                                       (handle-pod-completed compute-cluster k8s-actual-state-dict
                                                                             :reason :reason-slave-removed)))

                                      :cook-expected-state/starting
                                      (case pod-synthesized-state-modified
                                        :missing (launch-pod compute-cluster
                                                             cook-expected-state-dict pod-name)
                                        ; TODO: May need to mark mea culpa retry
                                        :pod/failed (handle-pod-completed compute-cluster k8s-actual-state-dict) ; Finished or failed fast.
                                        :pod/running (handle-pod-started compute-cluster k8s-actual-state-dict)
                                        :pod/succeeded (handle-pod-completed compute-cluster k8s-actual-state-dict) ; Finished fast.
                                        ; TODO: Should mark mea culpa retry
                                        :pod/unknown (handle-pod-completed-and-kill-pod-in-weird-state
                                                       compute-cluster pod-name k8s-actual-state-dict)
                                        ; Its starting. Can be stuck here. TODO: Stuck state detector to detect being stuck.
                                        :pod/waiting cook-expected-state-dict)

                                      :missing
                                      (case pod-synthesized-state-modified
                                        :missing nil
                                        ; We shouldn't hit these unless we get a database rollback.
                                        :pod/failed (kill-pod-in-weird-state compute-cluster pod-name
                                                                             nil k8s-actual-state-dict)
                                        ; This can occur in testing when you're e.g., blowing away the database.
                                        ; It will go through :missing,:missing and then be deleted from the map.
                                        ; TODO: May be evidence of a bug where we process pod changes when we're starting up.
                                        ; Currently occurs because kill's can race ahead of launches, we kill something that has
                                        ; been added to datomic, but hasn't been submitted to k8s yet.
                                        :pod/running (kill-pod api-client cook-expected-state-dict pod)
                                        ; We shouldn't hit these unless we get a database rollback.
                                        :pod/succeeded (kill-pod-in-weird-state compute-cluster pod-name
                                                                                nil k8s-actual-state-dict)
                                        ; Unlike the other :pod/unknown states, no datomic state to update.
                                        :pod/unknown (kill-pod-in-weird-state compute-cluster pod-name
                                                                              nil k8s-actual-state-dict)
                                        ; This can occur in testing when you're e.g., blowing away the database.
                                        ; It will go through :missing,:missing and then be deleted from the map.
                                        ; TODO: May be evidence of a bug where we process pod changes when we're starting up.
                                        ; Currently occurs because kill's can race ahead of launches, we kill something that has
                                        ; been added to datomic, but hasn't been submitted to k8s yet.
                                        :pod/waiting (kill-pod api-client cook-expected-state-dict pod)))]
      (when-not (cook-expected-state-equivalent? cook-expected-state-dict new-cook-expected-state-dict)
        (update-or-delete! cook-expected-state-map pod-name new-cook-expected-state-dict)
        (log/info "In compute cluster" name ", processing pod" pod-name "after cook-expected-state-change")
        (recur new-cook-expected-state-dict k8s-actual-state-dict)))))

(defn pod-update
  "Update the k8s actual state for a pod. Include some business logic to e.g., not change a state to the same value more than once.
  Invoked by callbacks from kubernetes."
  [{:keys [k8s-actual-state-map name] :as compute-cluster} ^V1Pod new-pod]
  (let [pod-name (api/V1Pod->name new-pod)]
    (locking (calculate-lock pod-name)
      (let [new-state {:pod new-pod
                       :synthesized-state (api/pod->synthesized-pod-state new-pod)
                       :sandbox-file-server-container-state (api/pod->sandbox-file-server-container-state new-pod)}
            old-state (get @k8s-actual-state-map pod-name)]
        ; We always store the updated state, but only reprocess it if it is genuinely different.
        (swap! k8s-actual-state-map assoc pod-name new-state)
        (let [new-file-server-state (:sandbox-file-server-container-state new-state)
              old-file-server-state (:sandbox-file-server-container-state old-state)]
          (when (and (= new-file-server-state :running) (not= old-file-server-state :running))
            (record-sandbox-url new-state)))
        (when-not (k8s-actual-state-equivalent? old-state new-state)
          (try
            (process compute-cluster pod-name)
            (catch Exception e
              (log/error e (str "In compute-cluster " name ", error while processing pod-update for " pod-name)))))))))

(defn pod-deleted
  "Indicate that kubernetes does not have the pod. Invoked by callbacks from kubernetes."
  [{:keys [k8s-actual-state-map name] :as compute-cluster} ^V1Pod pod-deleted]
  (let [pod-name (api/V1Pod->name pod-deleted)]
    (log/info "In compute cluster" name ", detected pod" pod-name "deleted")
    (locking (calculate-lock pod-name)
      (swap! k8s-actual-state-map dissoc pod-name)
      (try
        (process compute-cluster pod-name)
        (catch Exception e
          (log/error e (str "In compute-cluster " name ", error while processing pod-delete for " pod-name)))))))


(defn update-cook-expected-state
  "Update the cook expected state. Include some business logic to e.g., not change a state to the same value more than once. Marks any state changes Also has a lattice of state. Called externally and from state machine."
  [{:keys [cook-expected-state-map] :as compute-cluster} pod-name new-cook-expected-state-dict]
  (locking (calculate-lock [pod-name])
    (let [old-state (get @cook-expected-state-map pod-name)
          ; Save the launch pod. We may need it in order to kill it. See note under (:killed, :missing) in process, above.
          old-pod (:launch-pod old-state)
          new-expected-state-dict-merged (merge {:launch-pod old-pod} new-cook-expected-state-dict)]
      (when-not (cook-expected-state-equivalent? new-expected-state-dict-merged old-state)
        (swap! cook-expected-state-map assoc pod-name new-expected-state-dict-merged)
        (process compute-cluster pod-name)))))

(defn starting-namespaced-pod-name->pod
  "Returns a map from {:namespace pod-namespace :name pod-name}->pod for all instances that we're attempting to send to
   kubernetes to start."
  [{:keys [cook-expected-state-map] :as compute-cluster}]
  (->> @cook-expected-state-map
       (filter (fn [[_ {:keys [cook-expected-state launch-pod]}]]
                 (and (= :cook-expected-state/starting cook-expected-state)
                      (some? (:pod launch-pod)))))
       (map (fn [[_ {:keys [launch-pod]}]]
              (let [{:keys [pod]} launch-pod]
                [(api/get-pod-namespaced-key pod) pod])))
       (into {})))

(defn scan-process
  "Special verison of process run during scanning. It grabs the lock before processing the pod."
  [{:keys [api-client k8s-actual-state-map cook-expected-state-map] :as compute-cluster} pod-name]
  (locking (calculate-lock pod-name)
    (process compute-cluster pod-name)))
