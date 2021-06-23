(ns cook.passport
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [congestion.limits :refer [RateLimit]]
            [cook.config :as config]
            [cook.rest.impersonation :refer [impersonation-authorized-wrapper]]
            [plumbing.core :refer [fnk]]))

(defn log-passport-event
  [log-data]
  (log/log config/passport-logger-ns :info nil (json/write-str log-data)))

(def api-job-submission "raw-api-job-submission")
(def pod-launching "pod-launching")
(def pod-completed "pod-completed")