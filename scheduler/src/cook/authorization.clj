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
(ns cook.authorization
  "Functions that determine whether a given user is allowed to perform a
  certain action.

  Note that this is a seperate concern than authentication, which is
  determining whether a user is who they claim to be."
  (:require [plumbing.core :refer (fnk defnk)]))


(defn is-admin?
  "Given a username and an app state ref, determines whether the user
  has administrative privileges."
  [app-state username]
  
  ;; Currently, we only support including a list of admins in the
  ;; :user-privileges key in the app settings.
  ;; 
  ;; In the future, more complex schemes can be plugged in here,
  ;; selected through the global app settings read from the config file.

  (contains? (some-> @app-state :settings :user-privileges :admin) 
             username))
