(ns cook.plugins.adjustment
  (:require [clojure.tools.logging :as log]
            [cook.config :as config]
            [cook.plugins.definitions :refer [JobAdjuster]]
            [cook.plugins.util]
            [mount.core :as mount]))

(def no-op
  (reify JobAdjuster
    (adjust-job [_ job-map] job-map)))

(defn create-default-plugin-object
  "Returns the configured JobAdjuster, or a no-op if none is defined."
  [config]
  (let [factory-fn (get-in config [:settings :plugins :job-adjuster :factory-fn])]
    (if factory-fn
      (do
        (log/info "Creating job adjuster plugin with" factory-fn)
        (if-let [resolved-fn (cook.plugins.util/resolve-symbol (symbol factory-fn))]
          (resolved-fn config)
          (throw (ex-info (str "Unable to resolve factory fn " factory-fn) {}))))
      no-op)))

(mount/defstate plugin
                :start (create-default-plugin-object config/config))
