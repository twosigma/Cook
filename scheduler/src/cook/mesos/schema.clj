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
    :db/ident :job/commit-latch
    :db/valueType :db.type/ref
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
    :db/doc "The (optional) expected running time of the job in milliseconds.
             If provided, expected_runtime must be less than or equal to max_runtime."
    :db/ident :job/expected-runtime
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/environment
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/label
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/constraint
    :db/valueType :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/many
    :db/doc "A map of attribute to value patterns that constrain what hosts
             a job may be placed on"
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
    :db/doc "Determines if this job uses a custom executor (true) or the cook executor (false).
             If unset, then uses a custom executor (for legacy compatibility)."
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
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/disable-mea-culpa-retries
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db
    :db/doc "Flag that disables mea culpa retries. If set to true, mea culpa retries will count against the job's retry count."}
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/application
    :db/valueType :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/last-fenzo-placement-failure
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db
    :db/doc "Serialized EDN representing a summary of Fenzo placement failures
for a job. E.g. {:resources {:cpus 4 :mem 3} :constraints {\"unique_host_constraint\" 2}}"}
   {:db/id (d/tempid :db.part/db)
    :db/ident :job/under-investigation
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db
    :db/doc "Whether a summary of Fenzo placement failures should be recorded for the job at next opportunity."}
   ;; Group attributes
   {:db/id (d/tempid :db.part/db)
    :db/ident :group/uuid
    :db/valueType :db.type/uuid
    :db/unique :db.unique/identity
    :db/index true
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db
    :db/doc "A group is used to assign constraints and rules to an aggregate of jobs."}
   {:db/id (d/tempid :db.part/db)
    :db/ident :group/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :group/host-placement
    :db/valueType :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :group/commit-latch
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :group/job
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :group/straggler-handling
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/isComponent true
    :db.install/_attribute :db.part/db}
   ;;straggler-handling attriutes
   {:db/id (d/tempid :db.part/db)
    :db/ident :straggler-handling/type
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/isComponent true
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :straggler-handling/parameters
    :db/valueType :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :straggler-handling.quantile-deviation/quantile
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :straggler-handling.quantile-deviation/multiplier
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/user)
    :db/ident :straggler-handling.type/none}
   {:db/id (d/tempid :db.part/user)
    :db/ident :straggler-handling.type/quantile-deviation}
   ;; host-placement attributes
   {:db/id (d/tempid :db.part/db)
    :db/ident :host-placement/type
    :db/valueType :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :host-placement/parameters
    :db/valueType :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   ;; parameters for type balanced
   {:db/id (d/tempid :db.part/db)
    :db/ident :host-placement.balanced/attribute
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :host-placement.balanced/minimum
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   ;; parameters for type attribute-equals
   {:db/id (d/tempid :db.part/db)
    :db/ident :host-placement.attribute-equals/attribute
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   ;; host-placement.type attributes
   {:db/id (d/tempid :db.part/user)
    :db/ident :host-placement.type/unique}
   {:db/id (d/tempid :db.part/user)
    :db/ident :host-placement.type/balanced}
   {:db/id (d/tempid :db.part/user)
    :db/ident :host-placement.type/attribute-equals}
   {:db/id (d/tempid :db.part/user)
    :db/ident :host-placement.type/all}
   ;; commit-latch attributes
   {:db/id (d/tempid :db.part/db)
    :db/ident :commit-latch/committed?
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :commit-latch/uuid
    :db/valueType :db.type/uuid
    :db/unique :db.unique/identity
    :db/index true
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db
    :db/doc "A commit latch is used to determine if a job should be considered
             for scheduling. Many jobs can share the same commit latch if they
             should be considered schedulable at the same time. However, it is
             recommended not to consider jobs that share a commit latch to hold
             any semantic meaning other than they will be considered schedulable
             at the same time. A primitive of job group will be added to add
             more semantic power in the future."}
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
    :db/ident :docker/force-pull-image
    :db/valueType :db.type/boolean
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
   ;; Label attributes
   {:db/id (d/tempid :db.part/db)
    :db/ident :label/key
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :label/value
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   ;; Host constraint attributes
   {:db/id (d/tempid :db.part/db)
    :db/ident :constraint/attribute
    :db/doc "Attribute of host to constrain on"
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :constraint/operator
    :db/doc "Operator to use to evaulate pattern"
    :db/valueType :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :constraint/pattern
    :db/doc "Pattern that must pass on value of attribute for host to be valid
             to place task on"
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/user)
    :db/ident :constraint.operator/equals}
   ;; Application attributes
   {:db/id (d/tempid :db.part/db)
    :db/doc
    "Applications scheduling jobs on Cook can optionally provide the application
    name, which could be used to analyze the source of requests after the fact"
    :db/ident :application/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/doc "Along with application name, clients can provide an application version"
    :db/ident :application/version
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
    :db/ident :instance/progress-message
    :db/doc "represents the progress message of the instance"
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :instance/backfilled?
    ;;;   In a future version, datomic adds these schema values, leaving the info here when that occurs
    ;;     :schema/deprecated true
    ;;     :schema/deprecated-because "The concept of backfill was meant to allow Cook to schedule jobs out of order
    ;;                                 temporarily but treat the jobs as opportunistic and upgrade the jobs out of
    ;;                                 backfill later once the scheduling order had been corrected. Unfortunately,
    ;;                                 this causes a lot of unexpected behavior (jobs being preempted out of priority
    ;;                                 order) and lots of bugs (it is hard to correctly update jobs). The concept of
    ;;                                 backfill is not worth the added problems and so it is being removed."
    :db/doc "DEPRECATED: If this is true, then this instance should be preempted first regardless of priority. It's okay to upgrade an instance to be non-backfilled after a while."
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :instance/hostname
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :instance/ports
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/many
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
    :db/ident :instance/mesos-start-time
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
   {:db/id (d/tempid :db.part/db) ; this is deprecated in favor of the reason entity
    :db/ident :instance/reason-code
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :instance/reason
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :instance/cancelled
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :instance/sandbox-directory
    :db/doc "represents the sandbox directory of the instance on the Mesos agent"
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :instance/exit-code
    :db/doc "represents the return code of executing the command of the instance"
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
   {:db/id (d/tempid :db.part/db)
    :db/ident :share/reason
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   ;; Quota attributes
   {:db/id (d/tempid :db.part/db)
    :db/ident :quota/user
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :quota/resource
    :db/valueType :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :quota/count
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :quota/reason
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   ;; Resource mapping attributes
   {:db/id (d/tempid :db.part/db)
    :db/ident :resource.type/mesos-name
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   ;; Reason entity
   {:db/id (d/tempid :db.part/db)
    :db/ident :reason/code
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :reason/string
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :reason/mesos-reason
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :reason/name
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :reason/mea-culpa?
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :reason/failure-limit
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   ])

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

(def migration-add-port-count
  "This was written on 04-12-2016"
  [{:db/id (d/tempid :db.part/db)
    :db/ident :job/ports
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

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

(def scheduler-configs
  [{:db/id (d/tempid :db.part/user)
    :db/ident :scheduler/config}
   {:db/id (d/tempid :db.part/db)
    :db/ident :scheduler.config/mea-culpa-failure-limit
    :db/valueType :db.type/long
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
    :db/ident :resource.type/gpus
    :resource.type/mesos-name :gpus}
   {:db/id (d/tempid :db.part/user)
    :db/ident :resource.type/uri}
   ;; Functions for database manipulation
   {:db/id (d/tempid :db.part/user)
    :db/ident :instance/create
    :db/doc "Creates an instance for a job"}])

(def straggler-handling-types
  (->> schema-attributes
       (map :db/ident)
       (filter #(= "straggler-handling.type" (namespace %)))))

(def host-placement-types
  (->> schema-attributes
       (map :db/ident)
       (filter #(= "host-placement.type" (namespace %)))))

(def constraint-operators
  (->> schema-attributes
       (map :db/ident)
       (filter #(= "constraint.operator" (namespace %)))))

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
    :db/ident :job/reasons->attempts-consumed
    :db/doc "Determines the amount of attempts consumed by a collection of failure reasons."
    :db/fn #db/fn {:lang "clojure"
                   :params [mea-culpa-limit reasons]
                   :code
                   (->> reasons
                        frequencies
                        (map (fn [[reason count]]
                               ;; Note a nil reason counts as a non-mea-culpa failure!
                               (if (:reason/mea-culpa? reason)
                                 (let [failure-limit (or (:reason/failure-limit reason)
                                                         mea-culpa-limit)]
                                   (if (= failure-limit -1)
                                     0 ; -1 means no failure limit
                                     (max 0 (- count failure-limit))))
                                 count)))
                        (apply +))}}

   {:db/id (d/tempid :db.part/user)
    :db/ident :job/attempts-consumed
    :db/doc "Determines the amount of attempts consumed by a job-ent."
    :db/fn #db/fn {:lang "clojure"
                   :params [db job-ent]
                   :code
                   (let [done-statuses #{:instance.status/success :instance.status/failed}
                         mea-culpa-limit (or (when (:job/disable-mea-culpa-retries job-ent)
                                               0)
                                             (:scheduler.config/mea-culpa-failure-limit
                                               (d/entity db :scheduler/config))
                                             5)]
                     (->> job-ent
                          :job/instance
                          (filter #(done-statuses (:instance/status %)))
                          (map :instance/reason)
                          (d/invoke db :job/reasons->attempts-consumed mea-culpa-limit)))}}

   {:db/id (d/tempid :db.part/user)
    :db/ident :job/all-attempts-consumed?
    :db/doc "True if a job-ent is out of retries."
    :db/fn #db/fn {:lang "clojure"
                   :params [db job-ent]
                   :code
                   (<= (:job/max-retries job-ent)
                       (d/invoke db :job/attempts-consumed db job-ent))}}

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
                         instance-states (map :instance/status (:job/instance job))
                         any-success? (some #{:instance.status/success} instance-states)
                         any-running? (some #{:instance.status/running} instance-states)
                         any-unknown? (some #{:instance.status/unknown} instance-states)
                         all-failed? (every? #{:instance.status/failed} instance-states)
                         prior-state (:job/state job)
                         all-attempts-consumed? (d/invoke db :job/all-attempts-consumed? db job)]
                     (cond
                       (= prior-state :job.state/completed)
                       []

                       (or (and all-failed?
                                all-attempts-consumed?)
                           any-success?)
                       [[:db/add j :job/state :job.state/completed]]

                       (or any-running?
                           any-unknown?)
                       [[:db/add j :job/state :job.state/running]]

                       :else
                       [[:db/add j :job/state :job.state/waiting]]))}}

   {:db/id (d/tempid :db.part/user)
    :db/ident :instance/update-state
    :db/doc "Update instance and job status. Queries the instance status first and checks that the transition is valid. Also transitions the job status considering the new update.

             Note that in order to provide consistency between an instance and the job that owns it, you should not wrap calls to :instance/update-state for multiple instances
             of the same job in a single transaction (or more generally, if you are calling :instance/update-state on an instance, you should not modify the status of another instance
             of the same job in the same transaction).
             To see one case where wrapping multiple calls in the same transaction leads to inconsistency, suppose jobA has instances instanceA and instanceB, both of which are marked
             as running. jobA is running and has no retries remaining. Suppose in one transaction we call (:instance/update-state instanceA :instance.status/failed) and
             (:instance/update-state instanceB :instance.status/failed). We would hope that after the transaction, jobA has status completed since both instances have failed and it is
             out of retries. However, since during the evaluation of :instance/update-state for instanceA (instanceB), the status of instanceB (instanceA) still appears to be running
             from the perspective of the current Datomic state, the state of jobA will not be set to completed. After the transaction completes, the state of jobA is still running."
    :db/fn #db/fn {:lang "clojure"
                   :params [db instance new-state reason]
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
                       (into [[:db/add instance :instance/status new-state]]
                             (let [instance-ent (d/entity db instance)
                                   job-ent (:job/_instance instance-ent)
                                   job (:db/id job-ent)
                                   other-instances (->> (:job/instance job-ent)
                                                        (remove #(= (:db/id %) (:db/id instance-ent))))
                                   instance-states (->> other-instances
                                                        (map :instance/status)
                                                        (cons new-state))
                                   any-success? (some #{:instance.status/success} instance-states)
                                   any-running? (some #{:instance.status/running} instance-states)
                                   any-unknown? (some #{:instance.status/unknown} instance-states)
                                   all-failed? (every? #{:instance.status/failed} instance-states)
                                   prior-state (:job/state job-ent)
                                   reason (d/entity db reason)
                                   all-attempts-consumed?
                                   (d/invoke db :job/all-attempts-consumed? db
                                             (update-in (into {} job-ent) [:job/instance]
                                                        conj {:instance/status new-state
                                                              :instance/reason reason}))]
                               (cond
                                 (= prior-state :job.state/completed)
                                 []

                                 (or (and all-failed?
                                          all-attempts-consumed?)
                                     any-success?)
                                 [[:db/add job :job/state :job.state/completed]]

                                 (or any-running?
                                     any-unknown?)
                                 [[:db/add job :job/state :job.state/running]]

                                 :else
                                 [[:db/add job :job/state :job.state/waiting]])))))}}

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
                       []))}}
   {:db/id (d/tempid :db.part/user)
    :db/ident :entity/ensure-not-exists
    :db/doc "Ensure that the given entity does not exist"
    :db/fn #db/fn {:lang "clojure"
                   :params [db id-or-lookup]
                   :code
                   (let [j (d/entity db id-or-lookup)]
                     (when j
                       (throw (IllegalStateException.
                               (str "Entity with id " id-or-lookup " already exists.")))))}}])

(def reason-entities
  [{:db/id (d/tempid :db.part/user)
    :reason/code 1002
    :reason/string "Preempted by rebalancer"
    :reason/mea-culpa? true
    :reason/name :preempted-by-rebalancer}
   {:db/id (d/tempid :db.part/user)
    :reason/code 1003
    :reason/string "Container preempted by Mesos"
    :reason/name :mesos-container-preempted
    :reason/mea-culpa? false
    :reason/mesos-reason :reason-executor-preempted}

   {:db/id (d/tempid :db.part/user)
    :reason/code 2000
    :reason/string "Container limitation reached"
    :reason/name :mesos-container-limitation
    :reason/mea-culpa? false
    :reason/mesos-reason :reason-container-limitation}
   {:db/id (d/tempid :db.part/user)
    :reason/code 2001
    :reason/string "Container disk limitation exceeded"
    :reason/name :mesos-container-limitation-disk
    :reason/mea-culpa? false
    :reason/mesos-reason :reason-container-limitation-disk}
   {:db/id (d/tempid :db.part/user)
    :reason/code 2002
    :reason/string "Container memory limit exceeded"
    :reason/name :mesos-container-limitation-memory
    :reason/mea-culpa? false
    :reason/mesos-reason :reason-container-limitation-memory}
   {:db/id (d/tempid :db.part/user)
    :reason/code 2003
    :reason/string "Task max runtime exceeded"
    :reason/name :max-runtime-exceeded
    :reason/mea-culpa? false}
   {:db/id (d/tempid :db.part/user)
    :reason/code 2004
    :reason/string "Task was a straggler"
    :reason/name :straggler
    :reason/mea-culpa? false}

   {:db/id (d/tempid :db.part/user)
    :reason/code 3000
    :reason/string "Mesos task reconciliation"
    :reason/name :mesos-reconciliation
    :reason/mea-culpa? false
    :reason/mesos-reason :reason-reconciliation}
   {:db/id (d/tempid :db.part/user)
    :reason/code 3001
    :reason/string "Invalid Mesos framework id"
    :reason/name :mesos-invalid-framework-id
    :reason/mea-culpa? true
    :reason/mesos-reason :reason-invalid-frameworkid}
   {:db/id (d/tempid :db.part/user)
    :reason/code 3002
    :reason/string "Invalid Mesos offer"
    :reason/name :mesos-invalid-offers
    :reason/mea-culpa? true
    :reason/mesos-reason :reason-invalid-offers}
   {:db/id (d/tempid :db.part/user)
    :reason/code 3003
    :reason/string "Resource unknown"
    :reason/name :mesos-resources-unknown
    :reason/mea-culpa? false
    :reason/mesos-reason :reason-resources-unknown}
   {:db/id (d/tempid :db.part/user)
    :reason/code 3004
    :reason/string "Invalid task"
    :reason/name :mesos-task-invalid
    :reason/mea-culpa? false
    :reason/mesos-reason :reason-task-invalid}
   {:db/id (d/tempid :db.part/user)
    :reason/code 3005
    :reason/string "Unauthorized task"
    :reason/name :mesos-task-unauthorized
    :reason/mea-culpa? false
    :reason/mesos-reason :reason-task-unauthorized}
   {:db/id (d/tempid :db.part/user)
    :reason/code 3006
    :reason/string "Unknown task"
    :reason/name :mesos-task-unknown
    :reason/mea-culpa? false
    :reason/mesos-reason :reason-task-unknown}
   {:db/id (d/tempid :db.part/user)
    :reason/code 3007
    :reason/string "Agent unknown"
    :reason/name :mesos-slave-unknown
    :reason/mea-culpa? false
    :reason/mesos-reason :reason-slave-unknown}

   {:db/id (d/tempid :db.part/user)
    :reason/code 4000
    :reason/string "Agent removed"
    :reason/name :mesos-slave-removed
    :reason/mea-culpa? true
    :reason/mesos-reason :reason-slave-removed}
   {:db/id (d/tempid :db.part/user)
    :reason/code 4001
    :reason/string "Mesos agent restarted"
    :reason/name :mesos-slave-restarted
    :reason/mea-culpa? true
    :reason/mesos-reason :reason-slave-restarted}
   {:db/id (d/tempid :db.part/user)
    :reason/code 4002
    :reason/string "Mesos agent GC error"
    :reason/name :mesos-gc-error
    :reason/mea-culpa? true
    :reason/mesos-reason :reason-gc-error}
   {:db/id (d/tempid :db.part/user)
    :reason/code 4003
    :reason/string "Container launch failed"
    :reason/name :mesos-container-launch-failed
    :reason/mea-culpa? false
    :reason/mesos-reason :reason-container-launch-failed}
   {:db/id (d/tempid :db.part/user)
    :reason/code 4004
    :reason/string "Container update failed"
    :reason/name :mesos-container-update-failed
    :reason/mea-culpa? false
    :reason/mesos-reason :reason-container-update-failed}
   {:db/id (d/tempid :db.part/user)
    :reason/code 4005
    :reason/string "Agent disconnected"
    :reason/name :mesos-slave-disconnected
    :reason/mea-culpa? true
    :reason/mesos-reason :reason-slave-disconnected}
   {:db/id (d/tempid :db.part/user)
    :reason/code 4006
    :reason/string "Unable to contact agent"
    :reason/name :heartbeat-lost
    :reason/mea-culpa? true}

   {:db/id (d/tempid :db.part/user)
    :reason/code 5000
    :reason/string "Mesos framework removed"
    :reason/name :mesos-framework-removed
    :reason/mea-culpa? true
    :reason/mesos-reason :reason-framework-removed}
   {:db/id (d/tempid :db.part/user)
    :reason/code 5001
    :reason/string "Mesos master disconnected"
    :reason/name :mesos-master-disconnected
    :reason/mea-culpa? true
    :reason/mesos-reason :reason-master-disconnected}

   {:db/id (d/tempid :db.part/user)
    :reason/code 6000
    :reason/string "Mesos executor registration timed out"
    :reason/name :mesos-executor-registration-timeout
    :reason/mea-culpa? true
    :reason/mesos-reason :reason-executor-registration-timeout}
   {:db/id (d/tempid :db.part/user)
    :reason/code 6001
    :reason/string "Mesos executor re-registration timed out"
    :reason/name :mesos-executor-reregistration-timeout
    :reason/mea-culpa? false
    :reason/mesos-reason :reason-executor-reregistration-timeout}
   {:db/id (d/tempid :db.part/user)
    :reason/code 6002
    :reason/string "Mesos executor unregistered"
    :reason/name :mesos-executor-unregistered
    :reason/mea-culpa? false
    :reason/mesos-reason :reason-executor-unregistered}

   {:db/id (d/tempid :db.part/user)
    :reason/code 99000
    :reason/string "Unknown reason"
    :reason/name :unknown
    :reason/mea-culpa? false}
   {:db/id (d/tempid :db.part/user)
    :reason/code 99001
    :reason/string "Unknown mesos reason"
    :reason/name :mesos-unknown
    :reason/mea-culpa? false}
   {:db/id (d/tempid :db.part/user)
    :reason/code 99002
    :reason/string "Mesos executor terminated"
    :reason/name :mesos-executor-terminated
    :reason/mea-culpa? false
    :reason/mesos-reason :reason-executor-terminated}
   {:db/id (d/tempid :db.part/user)
    :reason/code 99003
    :reason/string "Command exited non-zero"
    :reason/name :mesos-command-executor-failed
    :reason/mea-culpa? false
    :reason/mesos-reason :reason-command-executor-failed}])

(def work-item-schema
  [schema-attributes state-enums rebalancer-configs scheduler-configs
   migration-add-index-to-job-state migration-add-index-to-job-user
   migration-add-port-count db-fns reason-entities])
