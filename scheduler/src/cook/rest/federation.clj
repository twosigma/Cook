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
(ns cook.rest.federation
  (:require byte-streams
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [clj-http-async-pool.core :as http-pool]
            [liberator.core :refer (map-values run-resource)]
            [liberator.representation :refer (render-map-generic render-seq-generic)]
            [liberator.util :refer (combine make-function)]
            [taoensso.nippy :as nippy])
  (:use [clj-http-async-pool.utils :only (map-xf)]))

(defn- render-nippy
  [data]
  (-> (nippy/freeze data)
      (byte-streams/to-input-stream)))

(defmethod render-map-generic "application/x-nippy+edn"
  [data _]
  (render-nippy data))

(defmethod render-seq-generic "application/x-nippy+edn"
  [data _]
  (render-nippy data))

(def ^:private is-federated-request-key "is-federated-request")

(defn- build-federated-request
  [req]
  (-> req
      (select-keys #{:form-params :params :query-params :scheme :uri})
      (update-in [:form-params] merge (:json-params req))
      (assoc-in [:query-params is-federated-request-key] true)
      (assoc-in [:headers "x-cook-on-behalf-of"] (:authorization/user req))
      (assoc :method (:request-method req)
             :spnego-auth true
             :accept :x-nippy+edn
             ;; We need to ask clj-http to give us a byte-array else
             ;; it will coerce compressed data to a string, which will
             ;; interpret null bytes
             :as :byte-array
             ;; Nippy already compresses
             :decompress-body false)))

(defn- http-response-ok?
  [response]
  (< (:status response) 400))

(defn- initiate-federated-requests
  [remotes router req]
  (->> remotes
       (map (fn [[server-name server-port]]
              (assoc req
                     :server-name server-name
                     :server-port server-port)))
       (map (partial http-pool/request router))
       (map (fn [response-chan]
              (async/pipe response-chan
                          (async/chan 1
                                      (map-xf #(if (http-response-ok? %)
                                                 (->> (:body %)
                                                      (nippy/thaw)
                                                      (assoc % :federated-result))
                                                 %))
                                      #(log/error % "Error thawing response body")))))
       (async/merge)))

(defn reduce-responses
  [f init ch should-log-failures?]
  (let [ok-chan (async/chan)]
    (async/go-loop []
      (if-let [response (async/<! ch)]
        (do (if (http-response-ok? response)
              (async/>! ok-chan response)
              (when should-log-failures?
                (log/error "Federated HTTP request failed with status" (:status response) ":" (pr-str response))
                (log/error (byte-streams/to-string (:body response)))))
            (recur))
        (async/close! ok-chan)))
    (->> [ok-chan]
         (async/map :federated-result)
         (async/reduce f init)
         (async/<!!))))

(defn make-reducer
  "Helper function to create a reducer that will handle a federated
  channel and block to return the result.
  "
  [ctx->f]
  (fn [ctx init ch]
    (let [f (ctx->f ctx)]
      (reduce-responses f init ch true))))

(defn choose-first-ok-result
  "Helper function to create a reducer that just picks the first OK
  result and returns it.  Optionally applies a function to the
  result (e.g. wrapping it in a map for liberator's :exists?).  If
  nothing is ok, returns nil and doesn't call the supplied function.
  "
  ([] (choose-first-ok-result (constantly identity)))
  ([ctx->f]
   (fn [ctx init ch]
     (let [f (ctx->f ctx)
           first-ok-val (reduce-responses (fn [a b] (if-not (nil? a) a b)) init ch false)]
       (when first-ok-val
         (f first-ok-val))))))

(defn- result->result-vector
  [result]
  (if (vector? result)
    result
    [(boolean result) (if (map? result)
                        result
                        {})]))

(defn make-federated-resource-fn
  "Constructs a liberator-friendly function that accepts a context and
  federates requests in a map/reduce style.

  On the primary server, we'll send out federated requests to the same
  endpoint, add a channel to receive those results to the context map,
  and then perform our own map and reduce locally.

  On secondary servers, we'll just perform the map, and return those
  results to the primary.

  A federated function specification is a map that contains the
  key :mapper and may also contain :preparer and :reducer.

    :mapper should be a one-arg function of the context, or keyword or
  constant value, similar to other liberator functions.  It should
  return a value suitable for liberator to consume.  If we're a
  secondary server and this value would be returned to the client,
  instead it's what gets passed back to the primary.  If we're a
  primary server, it's what will be considered the \"init\" for the
  reducer, or if we don't have a reducer, it's returned directly to
  liberator.  Not providing a reducer is a valid thing to do in an
  early liberator callback, to issue the secondary requests early in
  the pipeline, waiting until a later callback to do the reduce.  The
  federated function will only fire off secondary requests once, later
  callbacks will see there is already a response channel and won't
  fire off more requests.

    :reducer should be a three-arg function of the liberator context,
  the result of the original map function, plus a channel to receive
  federated responses.  The channel will produce clj-http response
  objects with an additional key, :federated-result, which will be a
  deserialized Clojure data structure as returned by the secondary, if
  the response has an ok status.  The default is to just return the
  result of the original map function; this enables an early liberator
  decision function to fire off federated requests and for a later
  function to block on those results.

    :preparer exists to allow the primary to add some information
  before federated requests are sent.  It should be a function of the
  context which may modify the request object and add to the context.
  Typically this would be used to add a parameter to the requests that
  go out to secondaries, e.g. to set an id that secondaries will use.
  Check build-federated-request to see which keys will be retained
  when forwarding a request to secondaries.

    :finalizer gets called on the final result of reducing, so that
  the response body can be different from the intermediate reduce
  values.  An example of this is when the reduced value is a set built
  up by union, but the final result is actually a string concatenating
  the results.  The finalizer is a function of the context and the
  reduced value, and should produce the final value, it may not modify
  the context.
  "
  [& kvs]
  (let [default-reducer (fn [ctx init chan] init)
        default-finalizer (fn [ctx val] val)
        {:keys [preparer mapper reducer finalizer]
         :or {preparer {}
              reducer default-reducer
              finalizer default-finalizer}}
        (apply hash-map kvs)]
    (assert (not (nil? mapper)) "Must have a definition for :mapper")
    (let [prepare-fn (make-function preparer)
          map-fn (make-function mapper)
          reduce-fn (make-function reducer)
          finalizer-fn (make-function finalizer)]
      (fn federated-resource-fn [ctx]
        (if (boolean (get-in ctx [:request :params is-federated-request-key]))
          (map-fn ctx)
          (let [[prepare-result prepare-context-update] (result->result-vector (prepare-fn ctx))
                prepared-ctx (merge-with combine ctx prepare-context-update)
                ctx-with-chan (if (contains? prepared-ctx ::chan)
                                ;; An earlier liberator callback fired
                                ;; off requests and we have a ::chan,
                                ;; so don't initiate more requests,
                                ;; just do local computation.
                                prepared-ctx
                                ;; We haven't yet sent off requests,
                                ;; so send them now and add the
                                ;; response channel to the context.
                                (let [req (:request prepared-ctx)
                                      remotes (::remotes req)
                                      router (::router req)
                                      federated-chan (->> (build-federated-request req)
                                                          (initiate-federated-requests remotes router))]
                                  (assoc prepared-ctx ::chan federated-chan)))
                map-result (map-fn prepared-ctx)
                reduce-ctx (dissoc ctx-with-chan ::chan)]
            (finalizer-fn reduce-ctx
                          (reduce-fn reduce-ctx
                                     map-result (::chan ctx-with-chan)))))))))

(defn wrap-handler
  "Injects remotes and router into the request object for later
  federated calls.
  "
  [handler remotes router]
  (fn [req]
    (let [response (handler (assoc req
                                   ::remotes remotes
                                   ::router router))]
      response)))
