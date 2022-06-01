(ns cook.test.postgres
  (:require [clj-time.format]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [cook.postgres :as pg]
            [next.jdbc :as sql])
  (:import (java.lang Runtime)
           (java.util Random)))

(defn reset-c3p0-pool
  "If there's a c3p0 pool, cleanly close it. Used as part of test fixture shutdown for unit tests when we're doing a new
  db for each test."
  []
  (when-let [pg-dict @pg/c3p0-connection-pool]
    (.close pg-dict)
    (reset! pg/c3p0-connection-pool nil)))


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

(let [custom-formatter (clj-time.format/formatter "yyyyMMdd")]
  (defn make-new-schema-name
    "Generate the schema name we want to use for unit tests"
    []
    (str "db_tests_" (System/getenv "USER") "_" (clj-time.format/unparse-local-date custom-formatter (clj-time.core/today)) "_" (+ 1000 (.nextInt (Random.) 9000)))))

(defn configure-database-connection-for-unit-tests
  "Configure the database connection for unit tests and install it into the atom's in cook.postgres"
  []
  (if-let [schema-name (System/getenv "COOK_DB_TEST_PG_SCHEMA")]
    (do
      (let [pg-connection-meta (pg/make-database-connection-dictionary-from-env-vars schema-name)]
        (log/info "Using existing schema " schema-name " with connection " pg-connection-meta)
        (reset! pg/saved-pg-config-dictionary pg-connection-meta)
        pg-connection-meta))
    ; If I have an environment saying to autocreate schema:
    (do
      (let [setup-script (System/getenv "COOK_DB_TEST_AUTOCREATE_SCHEMA")
            schema-name (make-new-schema-name)
            pg-connection-meta (pg/make-database-connection-dictionary-from-env-vars schema-name)]
        (log/info "Auto-creating schema " schema-name "with connection" pg-connection-meta)
        (setup-database setup-script schema-name)
        ; Saving it here is critical. We need currentSchema to tear it down later.
        (reset! pg/saved-pg-config-dictionary pg-connection-meta)
        pg-connection-meta))))

(defn deconfigure-database-connection-for-unit-tests
  "Deconfigure the database connection. Used when completing a test fixture."
  []
  (when-not (System/getenv "COOK_DB_TEST_PG_SCHEMA")
    ; Only shutdown if we don't have a schema set (I.e., only if we're doing a schema per namespace.)
    (let [schema-name (:currentSchema @pg/saved-pg-config-dictionary)]
      (log/info "Auto-destroying schema " schema-name "with connection" @pg/saved-pg-config-dictionary)
      ; Can't use '?' here with parameter substitution.
      (sql/execute! @pg/saved-pg-config-dictionary [(str "DROP SCHEMA " schema-name " CASCADE;")])
      (reset-c3p0-pool))
    (reset! pg/saved-pg-config-dictionary nil)))

(defn with-pg-db [f]
  "Test fixture that sets up a new postgres database (if needed), runs the unit tests, and demolishes it afterwards.
  Setup a postgres database test fixture that creates a database and destroys it afterwards"
  (configure-database-connection-for-unit-tests)
  (try
    (f)
    (finally
      (deconfigure-database-connection-for-unit-tests))))
