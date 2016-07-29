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
  determining whether a user is who they claim to be.


  Authorization functions
  =======================

  An operator-selectable authorization function determines how users are
  authorized to manipulate objects. The specific function to use is
  named by the value of the `:authorization-fn` key in
  the `:authorization-config` section of the config file.

  An authorization function has the signature:
    `(fn ^boolean x [settings-map
                     ^String user
                     ^clojure.lang.Keyword verb
                     ^cook.authorization.Ownable object])`

  settings-map is the data in the `:authorization-config` section of the
  config file. The auth function can use this to pull in any arbitrary
  configuration data it requires.

  The `user` is a string username, the authenticated identity of the
  agent that initiated the attempt.
  
  The `verb` is a keyword describing what the user is attempting to do:
  one of #{ :create, :read, :update, :destroy, :access }. :access
  implies all of the above. 

  This set of verbs is not fixed per se; the exact interpretation of its 
  values is up to the auth function.

  The `object` is an implementation of the `Ownable` protocol that the
  user is attempting to manipulate, such as a
  `cook.mesos.api/Job`. Ownable objects are by definition owned by a
  single specific user.

  The authorization function evaluates this information to determine
  whether the user is permitted to perform that verb on the object. If
  so, the function returns true, else false.

  The special object cook.authorization/system represents, by
  definition, access to the Cook system itself. Auth functions should
  only allow users with superuser / admin access to manipulate this object.

  For example, a call to `(is-authorized? \"foobar\" :access cook.authorization/system)` is
  attempting to determine whether user `\"foobar\"` is a system
  administrator with global access to everything.
  
  If a particular auth system has no notion of superusers or systemwide
  admins, it should return false.



  Example authorization functions
  -------------------------------

  Two example authorization functions are provided:

    * **open-auth** allows any user to do anything to any object. This
      is suitable for development and testing.

    * **configfile-admins-auth** reads a set of admin usernames from
      the `:admins` config file. Users whose usernames are in this list are
      admins, who are allowed to do anything to any object. Non-admins are
       only allowed to manipulate their own objects.


  Writing new authorization functions
  ------------------------------------

  You can easily extend the authorization system by providing your own
  auth function that conforms to the above signature and pointing the
  `:authorization-fn` config file key to it. 

  For example, you could consult an external authorization system that
  manages user credits or billing, or you could implement a \"teams\"
  feature, where users are allowed to modify their teammates' jobs.

"
  (:require [cook.global-state :refer [global-state]]
            [cook.util :refer [lazy-load-var]]
            [clojure.tools.logging :as log]
            [plumbing.core :refer (fnk defnk)]))


;; The defonce is a kludge to work around
;; https://groups.google.com/forum/#!topic/clojure/SYYYwZIrFiY -
;; reloading a protocol causes existing objects of that type to become
;; invalid as instances of the new protocol.
;;
;; If you redefine this protocol at runtime, you'll have to manually
;; remove the defonce. Other than that, this should not cause issues.
(defonce __ownable
  (defprotocol Ownable
    "Functions to be defined on types that are able to be owned by a specific user."
    (owner [this] "Returns the username that owns this object.")))

(def system (reify Ownable (owner [this] ::system)))

;;
;; Authorization functions
;; 


(defn open-auth
  "This authorization function allows any user to do anything to any
  object. It is intended for testing and development."
  [settings user verb object]
  true)


(defn configfile-admins-auth
  "This authorization function consults the set of usernames specified
  in the :admins key of the :authorization-config section of the config file.

  Usernames in this set are administrators, who are allowed to do anything to any object.
  Non-admins are only allowed to manipulate objects that they own."
  [settings
   ^String user
   ^clojure.lang.Keyword verb
   ^cook.authorization.Ownable object ]
  (let [admins    (:admins settings)
        is-admin? (contains? admins user)
        owner     (owner object)]
    (log/debug "[configfile-admins-auth] Checking whether user" user
               "may perform" verb
               "on object" (str object) "."
               "Admins are:" admins)
    (cond is-admin? (do
                      (log/debug "[configfile-admins-auth] User" user "is an admin, allowing.")
                      true)
          (= owner user) (do
                           (log/debug "[configfile-admins-auth] Object is owned by user, allowing.")
                           true)
          :else (do 
                  (log/info "[configfile-admins-auth] Unauthorized access attempt: user" user
                            "is not allowed to perform" verb "on" (str object ",") "denying.")
                  false))))


(defn is-authorized?
  "Determines whether the given user can perform the given operation
  on the given object, according to the authorization function
  specified in the config file.

  Returns true if allowed, else false."
  [settings
   ^String user
   ^clojure.lang.Keyword verb
   ^cook.authorization.Ownable object]

  (log/debug "[is-authorized?] Checking whether user" user
             "may perform" verb "on" (str object) "...")
  (log/debug "[is-authorized?] Settings are:" settings)

  (let [authorization-fn (lazy-load-var (:authorization-fn settings))]
    (authorization-fn settings user verb object)))


