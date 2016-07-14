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
(ns cook.basic-auth
  (:require [clojure.tools.logging :as log]
            [ring.util.response :refer (header status response)])
  (:import org.apache.commons.codec.binary.Base64))

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

(defn http-basic-middleware
  "Provides HTTP basic auth middleware. Doesn't verify the password at all; this should be used in a trusted network"
  [h]
  (fn [req]
    (if-let [[user pass] (parse-auth-from-request req)]
      (do
        (log/debug "Got http basic auth:" user pass)
        (h (assoc req :authorization/user user)))
      (-> (response "malformed or missing authorization header in basic auth")
          (status 401)
          (header "Content-Type" "text/plain")
          (header "WWW-Authenticate" "Basic realm=\"Cook\"")))))
