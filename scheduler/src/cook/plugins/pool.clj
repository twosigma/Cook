(ns cook.plugins.pool
  (:require [clojure.tools.logging :as log]
            [cook.config :as config]
            [cook.plugins.definitions :refer [PoolSelector]]
            [cook.plugins.util]
            [mount.core :as mount]))

(defrecord AttributePoolSelector [attribute-name default-pool]
  PoolSelector
  (select-pool [this offer]
    (or (->> offer :attributes (filter #(= attribute-name (:name %))) first :text)
        default-pool)))

(defn create-default-plugin-object
  "Returns the configured PoolSelector, or a no-op if none is defined."
  [config]
  (let [pool-selection (get-in config [:settings :plugins :pool-selection])
        factory-fn (:factory-fn pool-selection)]
    (if factory-fn
      (do
        (log/info "Creating instance completion plugin with" factory-fn)
        (if-let [resolved-fn (cook.plugins.util/resolve-symbol (symbol factory-fn))]
          (resolved-fn config)
          (throw (ex-info (str "Unable to resolve factory fn " factory-fn)))))
      (AttributePoolSelector. (:attribute-name pool-selection)
                              (:default-pool pool-selection)))))

(mount/defstate plugin
  :start (create-default-plugin-object config/config))
