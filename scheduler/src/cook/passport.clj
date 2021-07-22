(ns cook.passport
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [cook.config :as config]))

(defn log-event
  "Log event to cook-passport log file"
  [{:keys [event-type] :as log-data}]
  (when (:enabled? (config/passport))
    (log/log config/passport-logger-ns :info nil (json/write-str
                                                   (assoc
                                                     log-data
                                                     :source :cook-scheduler
                                                     :event-type (str "cook-scheduler/" (name event-type)))))))

(def job-created :job-created)
(def job-submitted :job-submitted)
(def pod-launched :pod-launched)
(def pod-completed :pod-completed)
