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
(ns cook.rest.basic-auth
  (:require [clojure.tools.logging :as log]
            [ring.util.response :refer [header response status]]))

(defn parse-auth-from-request
  "Given a request, extracts the HTTP basic auth username & password"
  [{{:strs [authorization]} :headers :as req}]
  (let [[[_ user pass :as data]]
        (try
          (re-seq #"([^:]*):(.*)"
                  (-> (re-matches #"\s*Basic\s+(.+)" authorization)
                      (second)
                      (.getBytes "utf-8")
                      (org.apache.commons.codec.binary.Base64/decodeBase64)
                      (String. "utf-8")))
          (catch Exception e
            ;;We log at debug if this seems to be a basic challenge/response issue
            (log/logp (if authorization :error :debug) e "Failed to parse the basic auth header:" (pr-str req))
            nil))]
    (when data [user pass])))

(defn make-user-password-valid?
  "Takes a validation type and arbitrary config and returns a function that given a user and password
   returns true if it is a valid pair.
   
   Example:
   ((make-user-password-valid? :config-file {:valid-logins #{[:wyegelwe :password1234]}})
    :wyegelwe :password1234)
   true
   "
  [validation config]
  (condp = validation
    :none (constantly true) 
    :config-file 
    (do
      (when-not (and (set? (:valid-logins config)) 
                     (every? #(= (count %) 2) (:valid-logins config)))
        (throw (ex-info (str "When using :config-file validation, must include key " 
                             ":valid-logins where the value is the set of valid [user password] pairs.") 
                        {:config config})))
      (fn config-file-validator 
        [user password]
        (contains? (:valid-logins config) [user password])))
    :else (throw (ex-info "Unknown validation selected" 
                          {:validation validation :config config }))))

(defn create-http-basic-middleware
  "Provides HTTP basic auth middleware. Uses `user-password-valid?` to verify requester"
  [user-password-valid?]
  (fn http-basic-middleware [h]
    (fn http-basic-auth-wrapper [req]
      (if-let [[user pass] (parse-auth-from-request req)]
        (do 
          (log/debug "Got http basic auth:" user pass)
          (if (user-password-valid? user pass)
            (h (assoc req :authorization/user user))
            (-> (response (str "User, password (" [user pass] ") not valid"))
                (status 401)
                (header "Content-Type" "text/plain")
                (header "WWW-Authenticate" "Basic realm=\"Cook\""))))
        (-> (response "malformed or missing authorization header in basic auth")
            (status 401)
            (header "Content-Type" "text/plain")
            (header "WWW-Authenticate" "Basic realm=\"Cook\""))))))

