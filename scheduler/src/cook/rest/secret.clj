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
(ns cook.rest.secret
  "This namespace is for development. It uses a fake secret."
  (:require [ring.middleware.params]))

(defn wrap-terribly-insecure-auth
  [handler]
  (fn [{{user "user"} :params :as req}]
    (handler (assoc req :authorization/user user))))

(defn authorization-middleware
  [auth]
  (-> auth
      (wrap-terribly-insecure-auth)))
