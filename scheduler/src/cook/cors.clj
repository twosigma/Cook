(ns cook.cors)

(defn preflight?
  [req]
  (= :options (:request-method req)))

(defn same-origin?
  "Returns true if the request is from the same origin as the provided origin header"
  [{:keys [headers scheme]}]
  (let [{:strs [host origin]} headers]
    (when (and host origin scheme)
      (= origin (str (name scheme) "://" host)))))

(defn request-allowed?
  "Returns true if the request is either from the same origin or matches a pattern in cors-origins"
  [req cors-origins]
  (or (same-origin? req)
      (let [origin (get-in req [:headers "origin"])]
        (some #(re-matches % origin) cors-origins))))

(defn wrap-preflight
  "Middleware for supporting CORS preflight requests"
  [handler cors-origins]
  (fn preflight-handler [{:keys [headers request-method] :as req}]
    (let [{:strs [origin]} headers]
      (if (and (preflight? req) origin)
        (if (request-allowed? req cors-origins)
          (let [{:strs [access-control-request-headers]} headers]
            {:status 200
             :headers {"Access-Control-Allow-Origin" origin
                       "Access-Control-Allow-Headers" access-control-request-headers
                       "Access-Control-Allow-Methods" "PUT, GET, OPTIONS, DELETE"
                       "Access-Control-Allow-Credentials" "true"
                       "Access-Control-Max-Age" "300"}})
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
                       "Access-Control-Allow-Origin" origin
                       "Access-Control-Allow-Credentials" "true"))
          {:status 403
           :body (str "Cross origin request denied from " origin)})
        (handler req)))))

(defn cors-middleware
  "Wraps the provided handler with wrap-cors and wrap-preflight in the correct order"
  [handler cors-origins]
  (-> handler
      (wrap-cors cors-origins)
      (wrap-preflight cors-origins)))
