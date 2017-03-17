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
(ns cook.spnego
  (:require [clojure.data.codec.base64 :as b64]
            [clojure.string :as str]
            [clojure.string :refer (split lower-case)]
            [ring.util.response :refer (header status response)])
  (:import [org.ietf.jgss GSSManager GSSCredential Oid]))

(def krb5Mech (Oid. "1.2.840.113554.1.2.2"))
(def krb5PrincNameType (Oid. "1.2.840.113554.1.2.2.1"))
(def spnegoMech (Oid. "1.3.6.1.5.5.2"))

;; Decode the input token from the negotiate line
;; expects the authorization token to exist
;;
(defn decode-input-token
  [req]
  (let [enc_tok (get-in req [:headers "authorization"])
        tfields (split  enc_tok #" ")]
    (if (= "negotiate" (lower-case (first tfields)))
      (b64/decode (.getBytes (last tfields)))
      nil)))

(defn encode-output-token
  "Take a token from a gss accept context call and encode it for use in a -authenticate header"
  [token]
  (str "Negotiate " (String. (b64/encode token))))

(defn do-gss-auth-check [gss_context req]
  (if-let [intok (decode-input-token req)]
    (if-let [ntok (.acceptSecContext gss_context intok 0 (alength intok))]
      (encode-output-token ntok)
      nil)
    nil))

(defn response-401-negotiate
  "Tell the client you'd like them to use kerberos"
  []
  (-> (response "Unauthorized")
      (status 401)
      (header "Content-Type" "text/html")
      (header "WWW-Authenticate" "Negotiate")))

(defn gss-context-init
  "Initialize a new gss context with name 'svc_name'"
  []
  (let [manager (GSSManager/getInstance)
        creds(.createCredential manager GSSCredential/ACCEPT_ONLY)
        gss (.createContext manager creds)]
    gss))

(defn gss-get-princ [gss]
  (.toString (.getSrcName gss)))

;; Add a cookie check to avoid doing gss
;; everytime
;;
(defn require-gss
  "This middleware enables the application to require a SPNEGO
   authentication. If SPNEGO is successful then the handler `rh`
   will be run, otherwise the handler will not be run and 401
   returned instead.  This middleware doesn't handle cookies for
   authentication, but that should be stacked before this handler."
  [rh]
  (fn [req]
    (if (get-in req [:headers "authorization"])
      (let [gss_context (gss-context-init)]
        (if-let [token (do-gss-auth-check gss_context req)]
          (let [princ (gss-get-princ gss_context)]
            (-> req
                (assoc :krb5-authenticated-princ princ
                       :authorization/user (first (str/split princ #"@" 2)))
                (rh)
                (header "WWW-Authenticate" token)))
          (response-401-negotiate)))
      (response-401-negotiate))))
