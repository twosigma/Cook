(ns cook.postgres
  (:require [clojure.tools.logging :as log]
            [clojure.java.jdbc :as sql]
            [cook.config :as config])
  (:import (java.lang Runtime)
           (java.util Random)))

(defn setup-database
  "Setup a fresh cook schema from scratch. At present, calls out
  to a bash script, passing it the target schema name and hopes it does the right thing."
  [setup-script schema-name]
  ; This environmental variable contains the path of a setup script that, given a schema name, creates a database.
  (let [^Process p (-> (Runtime/getRuntime)
                       (.exec (str setup-script " " schema-name)))
        _ (log/info "Waiting for database schema production process" (.pid p) "to exit")
        exit-code (.waitFor p)]
    (assert (= exit-code 0) (str "Exit code for process creation is" exit-code "not zero"))))

(defn make-database-connection-dictionary-for-unit-tests
  "Given a target schema name and assuming a variety of environmental
   variables are set, creates the metadata to connect to a database."
  [currentSchema]
  (merge {:dbtype "postgresql"
          :dbname (or (System/getenv "COOK_DB_TEST_PG_DB") "cook_local")
          ; Note that we need to save the currentSchema in the connection info to
          :currentSchema currentSchema
          :host (or (System/getenv "COOK_DB_TEST_PG_SERVER") "127.0.0.1")
          :ssl false
          :sslfactory "org.postgresql.ssl.NonValidatingFactory"}
         ; PGPASSWORD is the default enviornmental variable used by postgres psql.
         (when-let [passwd (or  (System/getenv "COOK_DB_TEST_PG_PASSWORD")
                                (System/getenv "PGPASSWORD"))]
           {:password passwd})
         (when-let [username (System/getenv "COOK_DB_TEST_PG_USER")]
           {:user username})))


; With unit tests, we need to create a new configuration dictionary in the test fixture.
; That gets stored here.
(def saved-pg-config-dictionary (atom nil))


(let [custom-formatter (clj-time.format/formatter "yyyyMMdd")]
(defn make-new-schema-name
  "Generate the schema name we want to use for unit tests"
  []
  (str "db_tests_" (System/getenv "USER") "_" (clj-time.format/unparse-local-date custom-formatter (clj-time.core/today)) "_" (+ 1000 (.nextInt (Random.) 9000)))))

(defn configure-database-connection-for-unit-tests
  "Configure the database connection."
  []
  (if-let [schema-name (System/getenv "COOK_DB_TEST_PG_SCHEMA")]
    (do
      (let [pg-connection-meta (make-database-connection-dictionary-for-unit-tests schema-name)]
        (log/info "Using existing schema " schema-name " with connection " pg-connection-meta)
        (reset! saved-pg-config-dictionary pg-connection-meta)
        pg-connection-meta))
    ; If I have an environment saying to autocreate schema:
    (do
      (let [setup-script (System/getenv "COOK_DB_TEST_AUTOCREATE_SCHEMA")
            schema-name (make-new-schema-name)
            pg-connection-meta (make-database-connection-dictionary-for-unit-tests schema-name)]
        (log/info "Auto-creating schema " schema-name "with connection" pg-connection-meta)
        (setup-database setup-script schema-name)
        (reset! saved-pg-config-dictionary pg-connection-meta)
        pg-connection-meta))))

(defn deconfigure-database-connection-for-unit-tests
  "Deconfigure the database connection. Used when completing a test fixture."
  []
  (when-not (System/getenv "COOK_DB_TEST_PG_SCHEMA")
    (let [schema-name (:currentSchema @saved-pg-config-dictionary)]
      (log/info "Auto-destroying schema " schema-name "with connection" @saved-pg-config-dictionary)
      ; Can't use '?' here with parameter substitution.
      (sql/execute! @saved-pg-config-dictionary [(str "DROP SCHEMA " schema-name " CASCADE;")]))
    (reset! saved-pg-config-dictionary nil)))

(defn with-pg-db [f]
  "Test fixture that sets up a new postgres database (if needed), runs the unit tests, and demolishes it afterwards. Setup a postgres database test fixture that creates a database and destroys it afterwards"
  (configure-database-connection-for-unit-tests)
  (try
    (f)
    (finally
      (deconfigure-database-connection-for-unit-tests))))

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
