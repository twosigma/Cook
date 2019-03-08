(ns cook.plugins.completion
  (:require [clojure.tools.logging :as log]
            [cook.config :as config]
            [cook.plugins.definitions :refer [InstanceCompletionHandler]]
            [cook.plugins.util]
            [mount.core :as mount]))

(def no-op
  (reify InstanceCompletionHandler
    (on-instance-completion [_ _ _])))

(defn create-default-plugin-object
  "Returns the configured InstanceCompletionHandler, or a no-op if none is defined."
  [config]
  (let [factory-fn (get-in config [:settings :plugins :instance-completion :factory-fn])]
    (if factory-fn
      (do
        (log/info "Creating instance completion plugin with" factory-fn)
        (if-let [resolved-fn (cook.plugins.util/resolve-symbol (symbol factory-fn))]
          (resolved-fn config)
          (throw (ex-info (str "Unable to resolve factory fn " factory-fn)))))
      no-op)))

(mount/defstate plugin
  :start (create-default-plugin-object config/config))
