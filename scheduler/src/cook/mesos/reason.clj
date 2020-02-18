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
(ns cook.mesos.reason
  (:require [clojure.tools.logging :as log]
            [datomic.api :as d]))

(defn reason-code->reason-entity
  [db reason-code]
  (d/entity db [:reason/code reason-code]))

(defn reason-code->reason-string
  [db reason-code]
  (:reason/string (reason-code->reason-entity db reason-code)))

(defn mesos-reason->cook-reason-entity-id
  [db task-id mesos-reason]
  (if-let [reason-entity-id (:db/id (d/entity db [:reason/mesos-reason mesos-reason]))]
    reason-entity-id
    (do
      (log/warn "Unknown mesos reason:" mesos-reason "for task" task-id)
      (:db/id (d/entity db [:reason/name :mesos-unknown])))))

(defn instance-entity->reason-entity
  [db instance]
  (or (:instance/reason instance)
      (reason-code->reason-entity db (:instance/reason-code instance))))

(defn all-known-reasons
  "Returns a list of Datomic entities corresponding to all
  of the currently defined failure reasons."
  [db]
  (map (partial d/entity db)
       (d/q '[:find [?e ...]
              :in $
              :where
              [?e :reason/code]]
            db)))

(defn default-failure-limit
  [db]
  (:scheduler.config/mea-culpa-failure-limit (d/entity db :scheduler/config)))
