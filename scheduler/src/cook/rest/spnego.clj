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
(ns cook.rest.spnego
  (:require [clojure.data.codec.base64 :as b64]
            [clojure.string :as str]
            [cook.util]
            [ring.util.response :refer [header response status]])
  (:import (org.ietf.jgss GSSCredential GSSManager Oid GSSContext)))

(def krb5Mech (Oid. "1.2.840.113554.1.2.2"))
(def krb5PrincNameType (Oid. "1.2.840.113554.1.2.2.1"))
(def spnegoMech (Oid. "1.3.6.1.5.5.2"))

;; Decode the input token from the negotiate line
;; expects the authorization token to exist
;;
(defn decode-input-token
  [req]
  (let [enc_tok (get-in req [:headers "authorization"])
        tfields (str/split  enc_tok #" ")]
    (if (= "negotiate" (str/lower-case (first tfields)))
      (b64/decode (.getBytes ^String (last tfields)))
      nil)))

(defn encode-output-token
  "Take a token from a gss accept context call and encode it for use in a -authenticate header"
  [token]
  (str "Negotiate " (String. ^bytes (b64/encode token))))

(defn do-gss-auth-check [^GSSContext gss_context req]
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

(defn gss-get-princ [^GSSContext gss]
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
      (let [^GSSContext gss_context (gss-context-init)
            token (do-gss-auth-check gss_context req)]
        ; Use isEstablished to confirm that the client authenticated correctly.
        ; If token is non-nil, add a WWW-Authenticate header to the response.
        (if (.isEstablished gss_context)
          (let [princ (gss-get-princ gss_context)]
            (cond-> (-> req
                        (assoc :krb5-authenticated-princ princ
                               :authorization/user (cook.util/principal->username princ))
                        (rh))
              token (header "WWW-Authenticate" token)))
          (response-401-negotiate)))
        (response-401-negotiate))))
