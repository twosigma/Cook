(ns cook.passport
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [cook.config :as config]))

(defn log-passport-event
  [log-data]
  (log/log config/passport-logger-ns :info nil (json/write-str log-data)))

(def api-job-submission "api-job-submission")
(def pod-launching "pod-launching")
(def pod-completed "pod-completed")