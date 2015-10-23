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
(ns clj-http-async-pool.client
  "Provides HTTP method functions for a pool or router.
  cf. clj-http/get and friends."
  (:require [clj-http-async-pool.core :refer :all])
  (:refer-clojure :exclude (get update)))

(defn- make-req
  [this url req method]
  (request this (merge req {:url url
                            :method method})))

(defn get
  [this url req]
  (make-req this url req :get))

(defn head
  [this url req]
  (make-req this url req :head))

(defn post
  [this url req]
  (make-req this url req :post))

(defn put
  [this url req]
  (make-req this url req :put))

(defn delete
  [this url req]
  (make-req this url req :delete))

(defn options
  [this url req]
  (make-req this url req :options))

(defn copy
  [this url req]
  (make-req this url req :copy))

(defn move
  [this url req]
  (make-req this url req :move))

(defn patch
  [this url req]
  (make-req this url req :patch))
