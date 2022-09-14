(ns cook.kubernetes.controller
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cook.compute-cluster :as cc]
            [cook.config :as config]
            [cook.datomic :as datomic]
            [cook.kubernetes.api :as api]
            [cook.kubernetes.metrics :as metrics]
            [cook.log-structured :as log-structured]
            [cook.mesos.sandbox :as sandbox]
            [cook.passport :as passport]
            [cook.prometheus-metrics :as prom]
            [cook.scheduler.scheduler :as scheduler]
            [metrics.meters :as meters]
            [metrics.timers :as timers])
  (:import (clojure.lang IAtom)
           (com.twosigma.cook.kubernetes FinalizerHelper)
           (io.kubernetes.client.openapi.models V1ContainerStatus V1ObjectMeta V1Pod V1PodStatus)
           (java.net URLEncoder)
           (java.util.concurrent.locks Lock)))

(defmacro with-process-lock
  "Evaluates body (which should contain a call to
  process) after acquiring the pod-name-based lock"
  [compute-cluster pod-name & body]
  `(prom/with-duration
     prom/process-lock-duration {:compute-cluster ~compute-cluster}
     (timers/time!
       (metrics/timer "process-lock" (:name ~compute-cluster))
       (let [lock-objects#
             (:controller-lock-objects ~compute-cluster)
             lock-count# (count lock-objects#)
             ^Lock lock-object#
             (nth lock-objects#
                  (-> ~pod-name
                    hash
                    (mod lock-count#)))
             timer-context#
             (timers/start
               (metrics/timer
                 "process-lock-acquire"
                 (:name ~compute-cluster)))
             prom-stop-fn#
             (prom/start-timer prom/process-lock-acquire-duration {:compute-cluster (:name ~compute-cluster)})]
         (.lock lock-object#)
         (try
           (.stop timer-context#)
           (prom-stop-fn#)
           ~@body
           (finally
             (.unlock lock-object#)))))))

(defn canonicalize-cook-expected-state
  "Canonicalize the expected states before comparing if they're equivalent"
  [cook-expected-state]
  (dissoc cook-expected-state :waiting-metric-timer :running-metric-timer
          :waiting-metric-timer-prom-stop-fn :running-metric-timer-prom-stop-fn))

(defn cook-expected-state-equivalent?
  "Is the old and new state equivalent?"
  [old-state new-state]
  (=
    (canonicalize-cook-expected-state old-state)
    (canonicalize-cook-expected-state new-state)))

(defn starting-pod?
  "Given the expected-state-dict, is this a starting pod?"
  [{:keys [cook-expected-state launch-pod]}]
  (and (= :cook-expected-state/starting cook-expected-state)
       (some? (:pod launch-pod))))

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
  [api-client compute-cluster-name cook-expected-state-dict pod]
  (api/delete-pod api-client compute-cluster-name pod)
  cook-expected-state-dict)

(defn kill-pod
  "Kill pod is the same as deleting a pod, we semantically distinguish the two operations.
  Delete is used for completed pods that we're done with.
  Kill is used for possibly running pods we want to kill so that they fail.
  Returns the cook-expected-state-dict passed in."
  [api-client compute-cluster-name cook-expected-state-dict pod & {:keys [grace-period-seconds]}]
  (api/delete-pod api-client compute-cluster-name pod :grace-period-seconds grace-period-seconds)
  cook-expected-state-dict)

(defn prepare-cook-expected-state-dict-for-logging
  ".toString on a pod is incredibly large. Make a version thats been elided."
  [cook-expected-state-dict]
  (let [stripped-cook-expected-state-dict
        (try
          (-> cook-expected-state-dict
              (dissoc :waiting-metric-timer)
              (dissoc :running-metric-timer)
              (dissoc :waiting-metric-timer-prom-stop-fn)
              (dissoc :running-metric-timer-prom-stop-fn))
          (catch Throwable t
            (log/error t "Error preparing cook expected state for logging:" cook-expected-state-dict)
            cook-expected-state-dict))]
    (if (:launch-pod stripped-cook-expected-state-dict)
      (assoc stripped-cook-expected-state-dict :launch-pod [:elided-for-brevity])
      stripped-cook-expected-state-dict)))

(defn prepare-pod-metadata-for-logging
  "Generates a pruned version of pod metadata that is suitable for logging"
  [pod]
  (when-let [^V1ObjectMeta metadata (some-> ^V1Pod pod .getMetadata)]
    (api/prepare-object-metadata-for-logging metadata)))

(defn prepare-k8s-actual-state-dict-for-logging
  [{:keys [pod] :as k8s-actual-state-dict}]
  (try
    (-> k8s-actual-state-dict
        (update-in [:synthesized-state :state] #(or % :missing))
        (dissoc :pod)
        (assoc
          :node-name (api/pod->node-name pod)
          :pod-metadata (prepare-pod-metadata-for-logging pod)
          :pod-status (some-> ^V1Pod pod .getStatus)))
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
  [{:keys [api-client name] :as compute-cluster} pod-name cook-expected-state-dict {:keys [pod] :as k8s-actual-state-dict}]
  (log-weird-state compute-cluster pod-name cook-expected-state-dict k8s-actual-state-dict)
  (kill-pod api-client name cook-expected-state-dict pod))

(defn kill-pod-hard
  "Kills the pod with a grace period of 0 (a 'hard kill'). This is used when pods
  are in the :pod/deleting state for an extended period of time, to force them to
  actually go away. We don't do this for every kill because under normal
  circumstances, we want to give processes a chance to properly handle SIGTERM."
  [{:keys [api-client name]} pod-name {:keys [pod]}]
  (log/info "In compute cluster" name ", pod" pod-name "needs to be hard-killed")
  (kill-pod
    api-client
    name
    ; It's ok to pass nil for cook-expected-state-dict because we only use this
    ; code path when the cook-expected-state-dict is already nil (aka :missing)
    nil
    pod
    :grace-period-seconds 0))

(defn write-status-to-datomic
  "Helper function for calling scheduler/write-status-to-datomic"
  [compute-cluster mesos-status]
  (scheduler/write-status-to-datomic datomic/conn
                                     @(:pool-name->fenzo-state-atom compute-cluster)
                                     mesos-status))

(defn write-status-to-passport
  "Helper function for logging completed job status to cook passport"
  [{:keys [name]} {:keys [task-id state reason]} exit-code]
  (passport/log-event (merge
                        (api/pod-name->job-map (task-id :value))
                        {:compute-cluster name
                         :event-type passport/pod-completed
                         :exit-code exit-code
                         :instance-exited? (some? exit-code)
                         :reason reason
                         :state state})))

(defn handle-pod-submission-failed
  "Marks the corresponding job instance as failed in the database and
  returns the `completed` cook expected state"
  [{:keys [name] :as compute-cluster} pod-name reason]
  (log/info "In compute cluster" name ", pod" pod-name "submission failed")
  (when-not (api/synthetic-pod? pod-name)
    (let [instance-id pod-name
          status {:reason reason
                  :state :task-failed
                  :task-id {:value instance-id}}]
      (write-status-to-datomic compute-cluster status)))
  {:cook-expected-state :cook-expected-state/completed})

(defn handle-pod-preemption
  "Marks the corresponding job instance as failed in the database and
  returns the `completed` cook expected state."
  [{:keys [name] :as compute-cluster} pod-name]
  (log/info "In compute cluster" name ", pod" pod-name "preemption has occurred")
  (when-not (api/synthetic-pod? pod-name)
    (let [instance-id pod-name
          status {:reason :reason-slave-removed
                  :state :task-failed
                  :task-id {:value instance-id}}]
      (write-status-to-datomic compute-cluster status)))
  {:cook-expected-state :cook-expected-state/completed})

(defn handle-pod-externally-deleted
  "Marks the corresponding job instance as failed in the database and
  returns the `completed` cook expected state."
  [{:keys [name] :as compute-cluster} pod-name]
  (log/info "In compute cluster" name ", pod" pod-name "was externally deleted")
  (when-not (api/synthetic-pod? pod-name)
    (let [instance-id pod-name
          status {:reason :reason-killed-externally
                  :state :task-failed
                  :task-id {:value instance-id}}]
      (write-status-to-datomic compute-cluster status)))
  {:cook-expected-state :cook-expected-state/completed})

(defn launch-pod
  [{:keys [api-client name] :as compute-cluster} cook-expected-state-dict pod-name]
  (let [{:keys [failure-reason terminal-failure?]}
        (api/launch-pod api-client name cook-expected-state-dict pod-name)]
    (if terminal-failure?
      (handle-pod-submission-failed compute-cluster pod-name failure-reason)
      ; These metrics only measure the happy paths to avoid wide variations from error rates changing.
      ; k8s-response-time-until-waiting helps us characterize watch latency
      (let [{{:keys [^V1Pod pod]} :launch-pod} cook-expected-state-dict
            metric-suffix (if (api/synthetic-pod? pod-name) "-synthetic" "")]
        (merge {:waiting-metric-timer
                (timers/start (metrics/timer (str "k8s-response-time-until-waiting" metric-suffix) name))
                :waiting-metric-timer-prom-stop-fn
                (prom/start-timer prom/pod-waiting-duration {:compute-cluster name
                                                             :synthetic (str (api/synthetic-pod? pod-name))
                                                             :kubernetes-scheduler-pod (str (api/kubernetes-scheduler-pod? pod))})
                :running-metric-timer
                (timers/start (metrics/timer (str "k8s-response-time-until-running" metric-suffix) name))
                :running-metric-timer-prom-stop-fn
                (prom/start-timer prom/pod-running-duration {:compute-cluster name
                                                             :synthetic (str (api/synthetic-pod? pod-name))
                                                             :kubernetes-scheduler-pod (str (api/kubernetes-scheduler-pod? pod))})}
               cook-expected-state-dict)))))

(defn update-or-delete!
  "Given a map atom, key, and value, if the value is not nil, set the key to the value in the map.
  If the value is nil, delete the key entirely."
  [^IAtom map-atom key value]
  (if (nil? value)
    (swap! map-atom dissoc key)
    (swap! map-atom assoc key value)))

(defn- get-job-container-status
  "Extract the container status for the main cook job container (defined in api/cook-container-name-for-job).
  We use this because we need the actual state object to determine the failure reason for failed jobs."
  [^V1PodStatus pod-status]
  (->> pod-status
       .getContainerStatuses
       (filter (fn [^V1ContainerStatus container-status] (= api/cook-container-name-for-job
                                                            (.getName container-status))))
       first))

(defn container-status->failure-reason
  "Maps kubernetes failure reasons to cook failure reasons"
  [{:keys [name]} ^V1Pod pod]
  ; TODO map additional kubernetes failure reasons
  (let [pod-name (api/V1Pod->name pod)
        pod-status (.getStatus pod)
        ^V1ContainerStatus container-status (get-job-container-status pod-status)
        container-terminated-reason (some-> container-status .getState .getTerminated .getReason)
        pod-status-reason (.getReason pod-status)
        pod-status-message (.getMessage pod-status)]
    (cond
      (= "OutOfMemory" pod-status-reason) :reason-container-limitation-memory
      (= "Error" container-terminated-reason) :reason-command-executor-failed
      (= "OOMKilled" container-terminated-reason) :reason-container-limitation-memory

      ; Check the node pod for NodeAffinity.
      (= "NodeAffinity" pod-status-reason)
      (do
        (log/info "In compute cluster" name ", encountered NodeAffinity pod status reason for " pod-name)
        :reason-slave-removed)

      (and pod-status-message (or
                               ; this message is given when pod exceeds pod ephemeral-storage limit
                               (str/includes? pod-status-message "ephemeral local storage usage exceeds")
                               ; this message is given when pod exceeds ephemeral-storage request and available disk space on node is low
                               (str/includes? pod-status-message "low on resource: ephemeral-storage")))
      :reason-container-limitation-disk

      ; If there is no container status and the pod status reason is Outofcpu,
      ; then the node didn't have enough CPUs to start the container
      (and (nil? container-status) (= "OutOfcpu" pod-status-reason))
      (do
        (log/info "In compute cluster" name ", encountered OutOfcpu pod status reason for" pod-name)
        :reason-invalid-offers)

      ; NOTE: the timeout used for determining whether a pod is unschedulable
      ; depends on if it was submitted via Fenzo or Kubernetes Schedulers.
      ; TODO: simplify this logic once we are no longer using both schedulers.
      (api/pod-unschedulable? pod)
      (do
        (log/info "In compute cluster" name ", encountered unschedulable pod" pod-name)
        :reason-scheduling-failed-on-host)

      (api/pod-containers-not-initialized? pod-name pod-status)
      (do
        (log/info "In compute cluster" name ", encountered containers not initialized in pod" pod-name)
        :reason-container-initialization-timed-out)

      (api/pod-containers-not-ready? pod-name pod-status)
      (do
        (log/info "In compute cluster" name ", encountered containers not ready in pod" pod-name)
        :reason-container-readiness-timed-out)

      :default
      (do
        (log/warn "In compute cluster" name ", unable to determine failure reason for" pod-name
                  {:pod-status pod-status :container-status container-status})
        :unknown))))

(defn make-failed-task-result
  "Make a pod status result corresponding to a failed pod"
  [instance-id reason]
  {:status {:task-id {:value instance-id}
            :state :task-failed
            :reason (or reason :reason-task-unknown)}})

(defn calculate-pod-status
  "Calculate the pod status of a completed pod. Factored out so that we can wrap it in an exception handler."
  [compute-cluster pod-name {:keys [synthesized-state ^V1Pod pod]} & {:keys [reason]}]
  (let [instance-id (some-> pod .getMetadata .getName)]
    ; If we have an actual pod here, make sure it has the same name as pod-name.
    (when instance-id
      (assert (= instance-id pod-name)))
    (try
      ; If we have a pod, we can look at it to calculate the status.
      (if instance-id
        (let [pod-status (.getStatus pod)
              ^V1ContainerStatus job-container-status (get-job-container-status pod-status)
              ; We leak mesos terminology here ('task') because of backward compatibility.
              succeeded? (= (:state synthesized-state) :pod/succeeded)
              task-state (if succeeded?
                           :task-finished
                           :task-failed)
              reason (or reason
                         (when succeeded? :reason-normal-exit)
                         (container-status->failure-reason compute-cluster pod))
              status {:task-id {:value pod-name}
                      :state task-state
                      :reason reason}
              exit-code (some-> job-container-status .getState .getTerminated .getExitCode)]
          {:status status :exit-code exit-code})
        ; We didn't have a pod, so we have to generate a status, without it.
        (do
          (log/error "In compute cluster" (cc/compute-cluster-name compute-cluster) ", pod" pod-name
                     "had a null pod")
          (make-failed-task-result pod-name reason)))
      (catch Throwable t
        (log/error t "In compute cluster" (cc/compute-cluster-name compute-cluster) ", pod" pod-name
                   "threw an error computing pod status for write to datomic.")
        (make-failed-task-result pod-name reason)))))

(defn handle-pod-completed
  "A pod has completed, or we're treating it as completed. E.g., it may really be running, but something is weird.

   Looks at the pod status, updates datomic (with success, failure, and possibly mea culpa),
   and return a new cook expected state of :cook-expected-state/completed."
  [compute-cluster pod-name k8s-actual-state & {:keys [reason]}]
  (when-not (api/synthetic-pod? pod-name)
    (let [{:keys [status exit-code]} (calculate-pod-status compute-cluster pod-name k8s-actual-state :reason reason)]
      (write-status-to-datomic compute-cluster status)
      (write-status-to-passport compute-cluster status exit-code)
      (when exit-code
        (sandbox/aggregate-exit-code (:exit-code-syncer-state compute-cluster) pod-name exit-code))))
  ; Must never return nil, we want it to return non-nil so that we will retry with writing the state to datomic in case we lose a race.
  ; Being in the (completed,*) state, will cause us to delete the pod, transitioning to (completed,missing), and thence
  ; to deleting from the map, into (missing,missing) state.
  {:cook-expected-state :cook-expected-state/completed})

(defn handle-pod-failed
  "Handles pod failure, checking to see if the failure was caused by pod preemption."
  [{:keys [name] :as compute-cluster} pod-name {:keys [synthesized-state] :as k8s-actual-state-dict}]
  (if (some-> synthesized-state :pod-preempted-timestamp)
    (do
      (log/info "In compute cluster" name ", pod" pod-name "failed and has"
                ":pod-preempted-timestamp, so is being treated as preempted")
      (handle-pod-preemption compute-cluster pod-name))
    (handle-pod-completed compute-cluster pod-name k8s-actual-state-dict)))

(defn handle-pod-started
  "A pod has started. So now we need to update the status in datomic."
  [compute-cluster {:keys [running-metric-timer running-metric-timer-prom-stop-fn]} pod-name]
  (when-not (api/synthetic-pod? pod-name)
    (let [instance-id pod-name
          ; We leak mesos terminology here ('task') because of backward compatibility.
          status {:task-id {:value instance-id}
                  :state :task-running
                  :reason :reason-running}]
      (write-status-to-datomic compute-cluster status)))
  ; Track the time until running, if available.
  (when running-metric-timer (timers/stop running-metric-timer))
  (when running-metric-timer-prom-stop-fn (running-metric-timer-prom-stop-fn))
  ; At this point, we don't care about the launch pod or the metric timers, so toss their dictionary away.
  {:cook-expected-state :cook-expected-state/running})

(defn record-sandbox-url
  "Record the sandbox file server URL in datomic."
  [pod-name {:keys [^V1Pod pod]}]
  (when-not (api/synthetic-pod? pod-name)
    (let [task-id (-> pod .getMetadata .getName)
          pod-ip (-> pod .getStatus .getPodIP)
          {:keys [default-workdir sidecar]} (config/kubernetes)
          sandbox-fileserver-port (:port sidecar)
          sandbox-health-check-endpoint (:health-check-endpoint sidecar)
          sandbox-url (try
                        (when (and sandbox-fileserver-port
                                   sandbox-health-check-endpoint
                                   (not (str/blank? pod-ip)))
                          (str "http://"
                               pod-ip
                               ":" sandbox-fileserver-port
                               "/files/read.json?path="
                               (URLEncoder/encode default-workdir "UTF-8")))
                        (catch Exception e
                          (log/debug e "Unable to retrieve directory path for" task-id)
                          nil))]
      (when sandbox-url (scheduler/write-sandbox-url-to-datomic datomic/conn task-id sandbox-url)))))

(defn record-hostname
  "Record the hostname in Datomic for the instance. This is necessary for pods
   submitted directly to Kubernetes (i.e. when we let Kubernetes do bin packing
   instead of Fenzo). In this case, the instance has no hostname and needs to be 
   updated once the pod is scheduled on a node by Kubernetes.
   
   If the hostname is null, we do not write to Datomic."
  [^V1Pod pod]
  (let [pod-name (-> pod .getMetadata .getName)
        hostname (api/pod->node-name pod)]
    (when hostname (scheduler/write-hostname-to-datomic datomic/conn pod-name hostname))))

(defn handle-pod-killed
  "A pod was killed. So now we need to update the status in datomic and store the exit code."
  [compute-cluster pod-name]
  (when-not (api/synthetic-pod? pod-name)
    (let [instance-id pod-name
          ; We leak mesos terminology here ('task') because of backward compatibility.
          status {:task-id {:value instance-id}
                  :state :task-failed
                  :reason :reason-command-executor-failed}]
      (write-status-to-datomic compute-cluster status)
      (sandbox/aggregate-exit-code (:exit-code-syncer-state compute-cluster) instance-id 143)))
  {:cook-expected-state :cook-expected-state/completed})

(defn handle-pod-completed-and-kill-pod-in-weird-state
  "Writes the completed state to datomic and deletes the pod in kubernetes.
  It is unusual (and unique) because it both modifies kubernetes and modifies datomic. It is intended
  only to be invoked in pods in state :k8s-actual-state/unknown and handle their recovery."
  [compute-cluster pod-name k8s-actual-state-dict]
  ; TODO: Should mark mea culpa retry
  (kill-pod-in-weird-state compute-cluster pod-name
                           (handle-pod-completed compute-cluster pod-name k8s-actual-state-dict) k8s-actual-state-dict))

(defn update-cook-starting-pods-cache
  "Update a cache of starting pods."
  [cook-starting-pods pod-name new-cook-expected-state-dict]
  (if (starting-pod? new-cook-expected-state-dict)
    (update-or-delete! cook-starting-pods pod-name new-cook-expected-state-dict)
    (update-or-delete! cook-starting-pods pod-name nil)))

(defn- handle-pod-missing-unexpectedly
  "Handle case when pod was running but then was removed unexpectedly"
  [synthesized-state compute-cluster pod-name]
  (if (some-> synthesized-state :pod-preempted-timestamp)
    (handle-pod-preemption compute-cluster pod-name)
    (handle-pod-externally-deleted compute-cluster pod-name)))

(defn process
  "Visit this pod-name, processing the new level-state. Returns the new cook expected state. Returns
  empty dictionary to indicate that the result should be deleted. NOTE: Must be invoked with the lock."
  [{:keys [api-client k8s-actual-state-map cook-expected-state-map cook-starting-pods name] :as compute-cluster} ^String pod-name doing-scan?]
  (prom/with-duration
    prom/controller-process-duration {:compute-cluster name :doing-scan (str doing-scan?)}
    (timers/time!
      (metrics/timer "controller-process" name)
      (loop [{:keys [cook-expected-state waiting-metric-timer waiting-metric-timer-prom-stop-fn] :as cook-expected-state-dict} (get @cook-expected-state-map pod-name)
             {:keys [synthesized-state pod] :as k8s-actual-state-dict} (get @k8s-actual-state-map pod-name)]
        ; Skip logging when process invocation is due to a scan and both Cook and Kubernetes think the pod is running
        ; We still want to log when process is invoked due to a state change
        (when-not (and doing-scan? (= cook-expected-state :cook-expected-state/running) (= (:state synthesized-state) :pod/running))
          (log/info "In" name "compute cluster, processing pod" pod-name ";"
                    "cook-expected:" (prepare-cook-expected-state-dict-for-logging cook-expected-state-dict) ","
                    "k8s-actual:" (prepare-k8s-actual-state-dict-for-logging k8s-actual-state-dict)))
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
        ; We use :cook-expected-state/killed to represent a cook scheduler kill (e.g. user killed, rebalancer killed) as opposed to a state machine kill.
        ; If we're doing a state-machine-induced kill (because something went wrong) it should occur by deleting the pod,
        ; so we go, e.g.,  (:running,:waiting) (an illegal state) to (:running,:missing)
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
                                            :pod/deleting nil
                                            ; The writeback to datomic has occurred, so there's nothing to do except to delete the pod from kubernetes
                                            ; and remove it from our tracking.
                                            :pod/failed (delete-pod api-client name cook-expected-state-dict pod)
                                            ; Who resurrected this pod? Where did it come from? Do we have two instances of cook?
                                            :pod/running (kill-pod-in-weird-state compute-cluster pod-name
                                                                                  cook-expected-state-dict
                                                                                  k8s-actual-state-dict)
                                            ; The writeback to datomic has occurred, so there's nothing to do except to delete the pod from kubernetes
                                            ; and remove it from our tracking.
                                            :pod/succeeded (delete-pod api-client name cook-expected-state-dict pod)
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
                                                (kill-pod api-client name :ignored pod)
                                                ; This is needed to make sure if we take the opportunistic kill, we make
                                                ; sure to write the status to datomic. Recall we're in kubernetes state missing.
                                                (handle-pod-killed compute-cluster pod-name))
                                              (do
                                                ; This shouldn't occur except in cases of extreme
                                                ; staleness (if we miss the deleting intermediate states)
                                                (log-weird-state compute-cluster pod-name
                                                                 cook-expected-state-dict k8s-actual-state-dict)
                                                (handle-pod-killed compute-cluster pod-name)))
                                            ; This is an expected transition in the deletion path
                                            :pod/deleting
                                            (do
                                              (log/info "In compute cluster" name ", pod" pod-name
                                                        "was killed with cook expected state"
                                                        (prepare-cook-expected-state-dict-for-logging
                                                          cook-expected-state-dict)
                                                        "and k8s actual state"
                                                        (prepare-k8s-actual-state-dict-for-logging k8s-actual-state-dict))
                                              (handle-pod-killed compute-cluster pod-name))
                                            :pod/failed (handle-pod-completed compute-cluster pod-name k8s-actual-state-dict)
                                            :pod/running (kill-pod api-client name cook-expected-state-dict pod)
                                            ; There was a race and it completed normally before being it was killed.
                                            :pod/succeeded (handle-pod-completed compute-cluster pod-name k8s-actual-state-dict)
                                            ; TODO: Should mark mea culpa retry
                                            :pod/unknown (handle-pod-completed-and-kill-pod-in-weird-state
                                                           compute-cluster pod-name k8s-actual-state-dict)
                                            :pod/waiting (kill-pod api-client name cook-expected-state-dict pod))

                                          :cook-expected-state/running
                                          (case pod-synthesized-state-modified
                                            ; This indicates that something deleted it behind our back
                                            :missing (handle-pod-missing-unexpectedly synthesized-state compute-cluster pod-name)
                                            :pod/deleting
                                            (handle-pod-missing-unexpectedly synthesized-state compute-cluster pod-name)
                                            ; TODO: May need to mark mea culpa retry
                                            :pod/failed (handle-pod-failed compute-cluster pod-name k8s-actual-state-dict)
                                            :pod/running cook-expected-state-dict
                                            :pod/succeeded (handle-pod-completed compute-cluster pod-name k8s-actual-state-dict)
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
                                                           (kill-pod api-client name cook-expected-state-dict pod)
                                                           (handle-pod-preemption compute-cluster pod-name)))

                                          :cook-expected-state/starting
                                          (case pod-synthesized-state-modified
                                            :missing (launch-pod compute-cluster cook-expected-state-dict pod-name)
                                            :pod/deleting
                                            (do
                                              (log/info "In compute cluster" name ", pod" pod-name
                                                        "was deleted while it was expected starting")
                                              (handle-pod-missing-unexpectedly
                                                synthesized-state
                                                compute-cluster
                                                pod-name))
                                            ; TODO: May need to mark mea culpa retry
                                            :pod/failed (handle-pod-completed compute-cluster pod-name k8s-actual-state-dict) ; Finished or failed fast.
                                            :pod/running (handle-pod-started compute-cluster cook-expected-state-dict pod-name)
                                            :pod/succeeded (handle-pod-completed compute-cluster pod-name k8s-actual-state-dict) ; Finished fast.
                                            ; TODO: Should mark mea culpa retry
                                            :pod/unknown (handle-pod-completed-and-kill-pod-in-weird-state
                                                           compute-cluster pod-name k8s-actual-state-dict)
                                            ; Its starting. Can be stuck here. TODO: Stuck state detector to detect being stuck.
                                            :pod/waiting (do
                                                           (when waiting-metric-timer (timers/stop waiting-metric-timer))
                                                           (when waiting-metric-timer-prom-stop-fn (waiting-metric-timer-prom-stop-fn))
                                                           ; Delete the timer so we only track the first time.
                                                           (dissoc cook-expected-state-dict :waiting-metric-timer :waiting-metric-timer-prom-stop-fn)))

                                          :missing
                                          (case pod-synthesized-state-modified
                                            :missing nil
                                            :pod/deleting
                                            (if (api/has-old-deletion-timestamp? pod)
                                              ; If the pod has an "old" deletion timestamp, then we should
                                              ; stop waiting and submit a hard-kill (grace period sec = 0)
                                              (kill-pod-hard compute-cluster pod-name k8s-actual-state-dict)
                                              nil)
                                            ; We shouldn't hit these unless we get a database rollback.
                                            :pod/failed (kill-pod-in-weird-state compute-cluster pod-name
                                                                                 nil k8s-actual-state-dict)
                                            ; This can occur in testing when you're e.g., blowing away the database.
                                            ; It will go through :missing,:missing and then be deleted from the map.
                                            ; TODO: May be evidence of a bug where we process pod changes when we're starting up.
                                            ; Currently occurs because kill's can race ahead of launches, we kill something that has
                                            ; been added to datomic, but hasn't been submitted to k8s yet.
                                            :pod/running (kill-pod api-client name cook-expected-state-dict pod)
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
                                            :pod/waiting (kill-pod api-client name cook-expected-state-dict pod)))]
          ; We do not want to gate whether we update on cook-expected-state-equivalent?.
          ; cook-expected-state-equivalent? is intended to identify when the states are different in a way that
          ; may need reprocessing in the state machine.
          ; However, the state may have changed (e.g., metric changes) and we want to
          ; capture that change.
          (update-or-delete! cook-expected-state-map pod-name new-cook-expected-state-dict)
          (update-cook-starting-pods-cache cook-starting-pods pod-name new-cook-expected-state-dict)
          (when-not (cook-expected-state-equivalent? cook-expected-state-dict new-cook-expected-state-dict)
            (log/info "In compute cluster" name ", processing pod" pod-name "after cook-expected-state-change")
            (recur new-cook-expected-state-dict k8s-actual-state-dict)))))))

(defn synthesize-state-and-process-pod-if-changed
  "Synthesizes the k8s-actual-state for the given
  pod and calls process if the state changed, or
  if force-process? is true."
  ([compute-cluster pod-name ^V1Pod pod]
   (synthesize-state-and-process-pod-if-changed compute-cluster pod-name pod false false))
  ([{:keys [k8s-actual-state-map api-client name] :as compute-cluster} pod-name ^V1Pod pod force-process? doing-scan?]

   (api/delete-collect-results-finalizer compute-cluster pod-name pod)

   (let [new-state {:pod pod
                    :synthesized-state (api/pod->synthesized-pod-state pod-name pod)
                    :sandbox-file-server-container-state (api/pod->sandbox-file-server-container-state pod)}
         old-state (get @k8s-actual-state-map pod-name)]
     ; We always store the new state (to capture any changes in the pod) if the pod's not nil. If we write here unconditionally,
     ; we may leak a deleted pod back into this dictionary where it will never be recoverable.
     ;
     ; We only reprocess if something important has actually changed.
     (when (some? pod)
       (swap! k8s-actual-state-map assoc pod-name new-state))
     (let [new-file-server-state (:sandbox-file-server-container-state new-state)
           old-file-server-state (:sandbox-file-server-container-state old-state)]
       (when (and (= new-file-server-state :running) (not= old-file-server-state :running))
         (record-sandbox-url pod-name new-state)))
     ; Hostname will change for pods scheduled by Kubernetes.
     (when (api/kubernetes-scheduler-pod? pod)
       (when-not (= (api/pod->node-name (:pod new-state))
                    (api/pod->node-name (:pod old-state)))
         (record-hostname pod)))
     (when (or force-process?
               (not (k8s-actual-state-equivalent? old-state new-state)))
       (when-not force-process?
           (prom/inc prom/pods-processed-unforced {:compute-cluster name})
           (meters/mark! (metrics/meter "pods-processed-unforced" name)))
       (try
         (process compute-cluster pod-name doing-scan?)
         (catch Exception e
           (log/error e "In compute-cluster" name ", error while processing pod" pod-name)))))))

(defn pod-update
  "Handles a pod update from the pod watch."
  [{:keys [name] :as compute-cluster} ^V1Pod new-pod]
  (let [pod-name (api/V1Pod->name new-pod)]
    (prom/with-duration
      prom/handle-pod-update-duration {:compute-cluster name}
      (timers/time!
        (metrics/timer "pod-update" name)
        (with-process-lock
          compute-cluster
          pod-name
          (synthesize-state-and-process-pod-if-changed compute-cluster pod-name new-pod))))))

(defn pod-deleted
  "Indicate that kubernetes does not have the pod. Invoked by callbacks from kubernetes."
  [{:keys [k8s-actual-state-map name] :as compute-cluster} ^V1Pod pod-deleted]
  (let [pod-name (api/V1Pod->name pod-deleted)]
    (log/info "In compute cluster" name ", detected pod" pod-name "deleted")
    (prom/with-duration
      prom/handle-pod-deletion-duration {:compute-cluster name}
      (timers/time!
        (metrics/timer "pod-deleted" name)
        (with-process-lock
          compute-cluster
          pod-name
          (swap! k8s-actual-state-map dissoc pod-name)
          (try
            (process compute-cluster pod-name false)
            (catch Exception e
              (log/error e (str "In compute-cluster " name ", error while processing pod-delete for " pod-name)))))))))

(defn update-cook-expected-state
  "Update the cook expected state. Include some business logic to e.g., not
   change a state to the same value more than once. Marks any state changes.
   Also has a lattice of state. Called externally and from state machine."
  [{:keys [cook-expected-state-map name cook-starting-pods] :as compute-cluster} pod-name new-cook-expected-state-dict]
  (prom/with-duration
    prom/update-cook-expected-state-duration {:compute-cluster name}
    (timers/time!
      (metrics/timer "update-cook-expected-state" name)
      (with-process-lock
        compute-cluster
        pod-name
        (let [old-state (get @cook-expected-state-map pod-name)
              ; Save the launch pod. We may need it in order to kill it.
              ; See note under (:killed, :missing) in process, above.
              old-pod (:launch-pod old-state)
              new-expected-state-dict-merged (merge {:launch-pod old-pod} new-cook-expected-state-dict)]
          (when-not (cook-expected-state-equivalent? new-expected-state-dict-merged old-state)
            (update-cook-starting-pods-cache cook-starting-pods pod-name new-cook-expected-state-dict)
            (swap! cook-expected-state-map assoc pod-name new-expected-state-dict-merged)
            (process compute-cluster pod-name false)))))))

(defn starting-namespaced-pod-name->pod
  "Returns a map from {:namespace pod-namespace :name pod-name}->pod for all instances that we're attempting to send to
   kubernetes to start."
  [{:keys [cook-expected-state-map cook-starting-pods] :as compute-cluster}]
  (->> @cook-starting-pods
       (map (fn [[_ {:keys [launch-pod]}]]
              (let [{:keys [pod]} launch-pod]
                [(api/get-pod-namespaced-key pod) pod])))
       (into {})))

(defn scan-process
  "Given a pod-name, looks up the pod and delegates
  to synthesize-state-and-process-pod-if-changed."
  [{:keys [k8s-actual-state-map name] :as compute-cluster} pod-name]
  (prom/with-duration
    prom/scan-process-duration {:compute-cluster name}
    (timers/time!
      (metrics/timer "scan-process" name)
      (with-process-lock
        compute-cluster
        pod-name
        (let [{:keys [pod]} (get @k8s-actual-state-map pod-name)]
          ; When scanning, set force-process? and doing-scan? to true
          (synthesize-state-and-process-pod-if-changed compute-cluster pod-name pod true true))))))
