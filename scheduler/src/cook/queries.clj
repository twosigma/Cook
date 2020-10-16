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
(ns cook.queries
  (:require [datomic.api :as d :refer [q]]))

;;
;; This file is intended to break circular dependencies by letting us place datomic queries into a
;; place that no other cook code depends on. DO NOT REQUITE ANY COOK NAMESPACE.

(defn get-all-resource-types
  "Return a list of all supported resources types. Example, :cpus :mem :gpus ..."
  [db]
  (->> (q '[:find ?ident
            :where
            [?e :resource.type/mesos-name ?ident]]
          db)
       (map first)))

(defn get-quota-resource-types
  "Return a list of resource types used for quota, :cpus :mem :gpus ..."
  [db]
  (keep #{:cpus :mem :gpus} (get-all-resource-types db)))
