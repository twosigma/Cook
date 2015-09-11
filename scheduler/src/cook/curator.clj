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
(ns cook.curator
  (:require [clojure.tools.logging :as log])
  (:import [org.apache.curator.framework CuratorFramework]
           [org.apache.zookeeper KeeperException$NoNodeException KeeperException$NodeExistsException]))

(defn set-or-create [^CuratorFramework curator-framework ^String k ^bytes v]
  (try
    (.. curator-framework create creatingParentsIfNeeded (forPath k v))
    (catch org.apache.zookeeper.KeeperException$NodeExistsException e
      (.. curator-framework setData (forPath k v)))))

(defn get-or-nil [^CuratorFramework curator-framework ^String k]
  (try
    (.. curator-framework getData (forPath k))
    (catch org.apache.zookeeper.KeeperException$NoNodeException e
      nil)))
