(ns cook.rest.cors)

(defn preflight?
  [{:keys [request-method]}]
  (= :options request-method))

(defn same-origin?
  "Returns true if the request is from the same origin as the provided origin header"
  [{:keys [headers scheme]}]
  (let [{:strs [host origin x-forwarded-proto]} headers
        forwarded-or-scheme (or x-forwarded-proto
                                (when scheme (name scheme)))]
    (when (and host origin forwarded-or-scheme)
      (= origin (str forwarded-or-scheme "://" host)))))

(defn request-allowed?
  "Returns true if the request is either from the same origin or matches a pattern in cors-origins.
   The request should have a non-nil origin header."
  [req cors-origins]
  (or (same-origin? req)
      (let [origin (get-in req [:headers "origin"])]
        (some #(re-matches % origin) cors-origins))))

(defn wrap-preflight
  "Middleware for supporting CORS preflight requests"
  [handler cors-origins]
  (fn preflight-handler [{:keys [headers] :as req}]
    (let [{:strs [origin]} headers]
      (if (and (preflight? req) origin)
        (if (request-allowed? req cors-origins)
          (let [{:strs [access-control-request-headers]} headers]
            {:status 200
             :headers {"Access-Control-Allow-Credentials" "true"
                       "Access-Control-Allow-Headers" access-control-request-headers
                       "Access-Control-Allow-Methods" "PUT, GET, OPTIONS, DELETE"
                       "Access-Control-Allow-Origin" origin
                       "Access-Control-Max-Age" "86400"}}) ; 1 day
          {:status 403
           :body (str "Origin " origin " not allowed")})
        (handler req)))))

(defn wrap-cors
  "Middleware for supporting CORS requests"
  [handler cors-origins]
  (fn cors-handler [{:keys [headers] :as req}]
    (let [{:strs [origin]} headers]
      (if origin
        (if (request-allowed? req cors-origins)
          (let [resp (handler req)]
            (update-in resp [:headers] assoc
                       "Access-Control-Allow-Credentials" "true"
                       "Access-Control-Allow-Origin" origin))
          {:status 403
           :body (str "Cross origin request denied from " origin)})
        (handler req))))) ; If no origin is provided, pass the request through.

(defn cors-middleware
  "Wraps the provided handler with wrap-cors and wrap-preflight in the correct order"
  [handler cors-origins]
  (-> handler
      (wrap-cors cors-origins)
      (wrap-preflight cors-origins)))
