(ns cook.plugins.file
  (:require [clojure.tools.logging :as log]
            [cook.config :as config]
            [cook.plugins.definitions :refer [FileUrlGenerator]]
            [cook.plugins.util]
            [mount.core :as mount]))

(defrecord NilFileUrlPlugin []
  FileUrlGenerator
  (file-url [this instance]
    nil))

(defn create-plugin-object
  "Returns the configured FileUrlPlugin, or a NilFileUrlPlugin if none is defined."
  [config]
  (let [pool-selection (get-in config [:settings :plugins :file-url])
        factory-fn (:factory-fn pool-selection)]
    (if factory-fn
      (do
        (log/info "Creating file url plugin with" factory-fn)
        (if-let [resolved-fn (cook.plugins.util/resolve-symbol (symbol factory-fn))]
          (resolved-fn config)
          (throw (ex-info (str "Unable to resolve factory fn " factory-fn)))))
      (NilFileUrlPlugin.))))

(mount/defstate plugin
  :start (create-plugin-object config/config))
