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
(ns cook.mesos.schema
  (:require [datomic.api :as d]
            [metatransaction.core :as mt]))

(def schema-attributes
  [;; Job attributes
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/command
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/user
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/uuid
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/index true
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/max-retries
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/max-runtime
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/environment
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/state
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/instance
    :db/valueType :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/resource
    :db/valueType :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/submit-time
    :db/index true
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/doc "Determines if this job uses a custom executor (true) or the command
             executor (false). If unset, then uses a custom executor (for legacy
             compatibility)."
    :db/ident :job/custom-executor
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/preemptions
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db
    :db/noHistory true}
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/priority
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/port
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/container
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   ;; Container Attributes
   {:db/id (d/tempid :db.part/db)
    :db/doc "variant records based on container/type"
    :db/ident :container/type
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :container/volumes
    :db/valueType :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :container/docker
    :db/valueType :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   ;; Docker attributes
   {:db/id (d/tempid :db.part/db)
    :db/ident :docker/image
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :docker/parameters
    :db/valueType :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :docker/network
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :docker/port-mapping
    :db/valueType :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}
   ;; Docker parameters attributes
   {:db/id (d/tempid :db.part/db)
    :db/ident :docker.param/key
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :docker.param/value
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   ;; Docker port-mapping attributes
   {:db/id (d/tempid :db.part/db)
    :db/ident :docker.portmap/host-port
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :docker.portmap/container-port
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :docker.portmap/protocol
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   ;; Container Volume Attributes
   {:db/id (d/tempid :db.part/db)
    :db/ident :container.volume/container-path
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :container.volume/host-path
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :container.volume/mode
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   ;; Environment Variable attributes
   {:db/id (d/tempid :db.part/db)
    :db/ident :environment/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :environment/value
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   ;; Resource attributes
   {:db/id (d/tempid :db.part/db)
    :db/ident :resource/type
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :resource/amount
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :resource.uri/executable?
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :resource.uri/value
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :resource.uri/extract?
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :resource.uri/cache?
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   ;; Instance attributes
   {:db/id (d/tempid :db.part/db)
    :db/ident :instance/task-id
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :instance/progress
    :db/doc "represents the progress of the instance, from 0 to 100"
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :instance/hostname
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :instance/executor-id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :instance/slave-id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :instance/status
    :db/index true
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :instance/start-time
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :instance/end-time
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :instance/preempted?
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :instance/reason-code
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

    ;; Share attributes
   {:db/id (d/tempid :db.part/db)
    :db/ident :share/resource
    :db/valueType :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :share/user
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   ;; Resource mapping attributes
   {:db/id (d/tempid :db.part/db)
    :db/ident :resource.type/mesos-name
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(def migration-add-index-to-job-state
  "This was written on 9-26-2014"
  [{:db/id :job/state
    :db/index true
    :db.alter/_attribute :db.part/db}])
  
(def migration-add-index-to-job-user
  "This was written on 3-30-2016"
  [{:db/id :job/user
    :db/index true
    :db.alter/_attribute :db.part/db}])

(def rebalancer-configs
  [{:db/id (d/tempid :db.part/user)
    :db/ident :rebalancer/config}
   {:db/id (d/tempid :db.part/db)
    :db/ident :rebalancer.config/min-utilization-threshold
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :rebalancer.config/safe-dru-threshold
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :rebalancer.config/min-dru-diff
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :rebalancer.config/max-preemption
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(def state-enums
  [;; Job states
   {:db/id (d/tempid :db.part/user)
    :db/ident :job.state/waiting}
   {:db/id (d/tempid :db.part/user)
    :db/ident :job.state/running}
   {:db/id (d/tempid :db.part/user)
    :db/ident :job.state/completed}
   ;; Enum of instance states
   {:db/id (d/tempid :db.part/user)
    :db/ident :instance.status/unknown}
   {:db/id (d/tempid :db.part/user)
    :db/ident :instance.status/running}
   {:db/id (d/tempid :db.part/user)
    :db/ident :instance.status/success}
   {:db/id (d/tempid :db.part/user)
    :db/ident :instance.status/failed}
   ;; Enum of resource types
   {:db/id (d/tempid :db.part/user)
    :db/ident :resource.type/cpus
    :resource.type/mesos-name :cpus}
   {:db/id (d/tempid :db.part/user)
    :db/ident :resource.type/mem
    :resource.type/mesos-name :mem}
   {:db/id (d/tempid :db.part/user)
    :db/ident :resource.type/uri}
   ;; Functions for database manipulation
   {:db/id (d/tempid :db.part/user)
    :db/ident :instance/create
    :db/doc "Creates an instance for a job"}])

(def db-fns
  [{:db/id (d/tempid :db.part/user)
    :db/ident :generic/atomic-inc
    :db/doc "Given a long-valued attribute, adds x to it atomically. If it doesn't exist, sets the value to x"
    :db/fn #db/fn {:lang "clojure"
                   :params [db e a x]
                   :requires [[metatransaction.core :as mt]]
                   :code
                   (let [db (mt/filter-committed db)
                         old-val (get (d/entity db e) a 0)]
                     [[:db/add e a (+ x old-val)]])}}

   {:db/id (d/tempid :db.part/user)
    :db/ident :generic/ensure
    :db/doc "Ensures an attribute of an entity is what we expected it to be. Throws exception otherwise"
    :db/fn #db/fn {:lang "clojure"
                   :params [db e a v]
                   :requires [[metatransaction.core :as mt]]
                   :code
                   (let [db (mt/filter-committed db)]
                     (if (seq (d/datoms db :eavt e a v))
                       nil
                       (throw (ex-info "Fail to ensure attribute" {:entity e
                                                                   :attribute a
                                                                   :expected v}))))}}

   {:db/id (d/tempid :db.part/user)
    :db/ident :job/update-state
    :db/doc "job state change cases:
             - task is running, job was running => no change
             - task is running, job was waiting => job starts running
             - task succeeded => job completed
             - task failed, no other tasks, retries exceeded => job completed
             - task failed, no other tasks, retries remaining => job waiting
             - task failed, other tasks running => job running"
    :db/fn #db/fn {:lang "clojure"
                   :params [db j]
                   :requires [[metatransaction.core :as mt]]
                   :code
                   (let [db (mt/filter-committed db)
                         job (d/entity db j)
                         instance-states (mapv first (q '[:find ?state ?i
                                                          :in $ ?j
                                                          :where
                                                          [?j :job/instance ?i]
                                                          [?i :instance/status ?s]
                                                          [?s :db/ident ?state]]
                                                        db j))
                         any-success? (some #{:instance.status/success} instance-states)
                         any-running? (some #{:instance.status/running} instance-states)
                         all-failed? (every? #{:instance.status/failed} instance-states)
                         attempts-consumed? (>= (count instance-states) (+ (:job/preemptions job 0)
                                                                           (:job/max-retries job)))
                         prior-state (:job/state job)]
                     (cond
                       (= prior-state :job.state/completed)
                       []

                       (or (and all-failed?
                                attempts-consumed?)
                           any-success?)
                       [[:db/add j :job/state :job.state/completed]]

                       any-running?
                       [[:db/add j :job/state :job.state/running]]

                       :else
                       [[:db/add j :job/state :job.state/waiting]]))}}

   {:db/id (d/tempid :db.part/user)
    :db/ident :instance/update-state
    :db/doc "Update instance status. But instead of setting it directly, queries the status first and sets status iff it's an valid state transition."
    :db/fn #db/fn {:lang "clojure"
                   :params [db instance new-state]
                   :requires [[metatransaction.core :as mt]]
                   :code
                   (let [db (mt/filter-committed db)
                         state-transitions {:instance.status/unknown #{:instance.status/running :instance.status/failed}
                                            :instance.status/running #{:instance.status/failed :instance.status/success}
                                            ;; terminal states
                                            :instance.status/success #{}
                                            :instance.status/failed #{}}
                         old-state (ffirst (q '[:find ?state
                                                :in $ ?e
                                                :where
                                                [?e :instance/status ?s]
                                                [?s :db/ident ?state]]
                                              db instance))]
                     ;; Checking the validity of the target state transition
                     (when (get-in state-transitions [old-state new-state])
                       [[:db/add instance :instance/status new-state]]))}}

   {:db/id (d/tempid :db.part/user)
    :db/ident :job/allowed-to-start?
    :db/doc "Throws an exception if the given job isn't allowed to start due to it have other instances or being in an indeterminate state.
             The exception is used to block a transaction from succeeding to launch the job, and in the task matcher,
             the launching of the task must come after the transaction. This ensures that we never launch a task we shouldn't."
    :db/fn #db/fn {:lang "clojure"
                   :params [db j]
                   :code
                   (let [job (d/entity db j)
                         instance-statuses (map :instance/status (:job/instance job))]
                     (when-not (and (= (:job/state job) :job.state/waiting) ; ensure still waiting
                                    ;; ensure not in indeterminate state
                                    (empty? (filter #{:instance.status/unknown :instance.status/running} instance-statuses)))
                       (throw (ex-info "The job can't start now" {:job j
                                                                  :state (:job/state job)
                                                                  :instance/statuses instance-statuses}))))}}

   {:db/id (d/tempid :db.part/user)
    :db/ident :job/ensure-not-completed
    :db/doc "job state change cases:
             - job waiting   => no change
             - job running   => no change
             - job completed => job waiting"
    :db/fn #db/fn {:lang "clojure"
                   :params [db j]
                   :requires [[metatransaction.core :as mt]]
                   :code
                   (let [db (mt/filter-committed db)
                         job (d/entity db j)
                         prior-state (:job/state job)]
                     (if (= prior-state :job.state/completed)
                       [[:db/add j :job/state :job.state/waiting]]
                       []))}}])

(def work-item-schema
  [schema-attributes state-enums rebalancer-configs migration-add-index-to-job-state migration-add-index-to-job-user db-fns])
