;;
;; Copyright (c) Two Sigma Open Source, LLC
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;  http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(ns cook.mesos.reason)

;; Note: Commented reasons are specified but not implemented

;;(def reason-normal 1000)
;;(def reason-aborted-by-user 1001)
(def reason-preempted-by-rebalancer 1002)
(def reason-mesos-container-preempted 1003)

(def reason-mesos-container-limitation 2000)
(def reason-mesos-container-limitation-disk 2001)
(def reason-mesos-container-limitation-memory 2002)
(def reason-max-runtime 2003)

(def reason-mesos-reconciliation 3000)
(def reason-mesos-invalid-frameworkid 3001)
(def reason-mesos-invalid-offers 3002)
(def reason-mesos-resources-unknown 3003)
(def reason-mesos-task-invalid 3004)
(def reason-mesos-task-unauthorized 3005)
(def reason-mesos-task-unknown 3006)
(def reason-mesos-slave-unknown 3007)

(def reason-mesos-slave-removed 4000)
(def reason-mesos-slave-restarted 4001)
(def reason-mesos-gc-error 4002)
(def reason-mesos-container-launch-failed 4003)
(def reason-mesos-container-update-failed 4004)
(def reason-mesos-slave-disconnected 4005)
(def reason-heartbeat-lost 4006)

(def reason-mesos-framework-removed 5000)
(def reason-mesos-master-disconected 5001)

(def reason-mesos-executor-registration-timeout 6000)
(def reason-mesos-executor-reregistration-timeout 6001)
(def reason-mesos-executor-unregistered 6002)

(def reason-unknown 99000)
(def reason-mesos-unknown 99001)
(def reason-mesos-executor-terminated 99002)

(defn mesos-reason->cook-reason-code
  [mesos-reason]
  (or (get {;;This reason is no longer used
            ;;:reason-command-executor-failed
            :reason-executor-preempted reason-mesos-container-preempted
            :reason-executor-terminated reason-mesos-executor-terminated
            :reason-executor-unregistered reason-mesos-executor-unregistered
            :reason-framework-removed reason-mesos-framework-removed
            :reason-gc-error reason-mesos-gc-error
            :reason-invalid-frameworkid reason-mesos-invalid-frameworkid
            :reason-invalid-offers reason-mesos-invalid-offers
            :reason-master-disconnected reason-mesos-master-disconected
            :reason-memory-limit reason-mesos-container-limitation-memory
            :reason-reconciliation reason-mesos-reconciliation
            :reason-resources-unknown reason-mesos-resources-unknown
            :reason-slave-disconnected reason-mesos-slave-disconnected
            :reason-slave-removed reason-mesos-slave-removed
            :reason-slave-restarted reason-mesos-slave-restarted
            :reason-slave-unknown reason-mesos-slave-unknown
            :reason-task-invalid reason-mesos-task-invalid
            :reason-task-unauthorized reason-mesos-task-unauthorized
            :reason-task-unknown reason-mesos-task-unknown

            :reason-container-launch-failed reason-mesos-container-launch-failed
            :reason-container-limitation reason-mesos-container-limitation
            :reason-container-limitation-disk reason-mesos-container-limitation-disk
            :reason-container-limitation-memory reason-mesos-container-limitation-memory
            :reason-container-preempted reason-mesos-container-preempted
            :reason-container-update-failed reason-mesos-container-update-failed
            :reason-executor-registration-timeout reason-mesos-executor-registration-timeout
            :reason-executor-reregistration-timeout reason-mesos-executor-reregistration-timeout}
           mesos-reason)
      reason-mesos-unknown))
