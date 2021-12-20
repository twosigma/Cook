(ns cook.postgres
  (:require [clojure.tools.logging :as log]
            [cook.config :as config]))

; With unit tests, we need to create a new configuration dictionary in the test fixture.
; That gets stored here.
(def saved-pg-config-dictionary (atom nil))

(defn pg-db
  "Return access information for a postgresql database suitable for database access.

   There are 2 main codepaths we take here:

   1. If there is a config/config with a pg-config in config.edn, (I.e., we're running
      integration tests or production) run with that config.

   2. If there's a saved configuration dictionary (used for unit tests), return that."
  []
  (or
    (-> config/config :settings :pg-config)
    @saved-pg-config-dictionary))
