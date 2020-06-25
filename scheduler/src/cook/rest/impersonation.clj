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
(ns cook.rest.impersonation
  "Support for services impersonating a user, performing Cook actions on the user's behalf."
  (:require [clojure.tools.logging :as log]
            [cook.util]
            [ring.util.response :refer [header response status]])
  (:import (clojure.lang Keyword)))

(defn- unrestricted-access
  "Denotes that an impersonator may perform any operation on a given object type."
  [_]
  true)

(defn- getter-access
  "Denotes that an impersonator may only perform gets on a given object type."
  [verb]
  (= :get verb))

(def object-type->verb->impersonatable?
  "Specification of operations that authorized impersonators may perform on behalf of another user."
  {:job unrestricted-access
   :quota getter-access
   :share getter-access
   :usage getter-access})

(defn impersonation-authorized-wrapper
  "This function is meant to wrap the authorization/is-authorized? function.

  The resulting function determines whether an authorized impersonator
  can perform the given operation on the given object, on behalf of the given user.

  Returns true if allowed, else false."
  [is-authorized-fn settings]
  (fn is-impersonatable?
    [^String user
     ^Keyword verb
     ^String impersonator
     {:keys [item] :as object}]
    (log/debug "[is-impersonatable?] Checking whether user" impersonator
               "may impersonate user" user
               "performing" verb "on" (str object) "...")
    (log/debug "[is-impersonatable?] Settings are:" settings)
    (and (or (nil? impersonator)
             (when-let [verb->impersonatable? (object-type->verb->impersonatable? item)]
               (verb->impersonatable? verb)))
         (is-authorized-fn settings user verb impersonator object))))

(defn create-impersonation-middleware
  "Provides user-impersonation middleware.
  Uses the impersonators set-argument to verify impersonation authorization."
  [impersonators]
  (fn impersonation-middleware [h]
    (if-let [impersonators-set (some-> impersonators seq set)]
      (fn impersonation-wrapper [{user :authorization/user :as req}]
        (if-let [impersonated-principal (get-in req [:headers "x-cook-impersonate"])]
          (let [impersonated-user (cook.util/principal->username impersonated-principal)]
            (log/debug "User" user "is attempting to impersonate" impersonated-user)
            (cond
              ; Case: self-impersonation
              ; Simply ignore the impersonation, treating it as a normal request from the user.
              (= impersonated-user user)
              (h req)
              ; Case: impersonation looks OK so far
              ; Note that the is-authorized-fn (via the impersonation-authorized-wrapper function)
              ; will later look for the :authorization/impersonator value and ensure that
              ; the target operation is allowed to be impersonated (see components.clj).
              (contains? impersonators-set user)
              (h (assoc req :authorization/user impersonated-user :authorization/impersonator user))
              ; Case: this user isn't authorized to impersonate
              :else
              (-> (response (str "User " user " does not have impersonation privileges."))
                  (status 403)
                  (header "Content-Type" "text/plain"))))
          ; Case: not attempting to impersonate
          (h req)))
      ; we don't add this middleware if no impersonators are configured
      h)))
