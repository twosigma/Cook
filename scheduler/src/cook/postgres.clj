(ns cook.postgres
  (:require [clojure.tools.logging :as log]
            [cook.config :as config]
            [next.jdbc :as sql])
  (:import (com.mchange.v2.c3p0 ComboPooledDataSource)))

; With unit tests, we need to create a new configuration dictionary in the test fixture.
; That gets stored here.
(def saved-pg-config-dictionary (atom nil))

; We keep the c3p0 pool for talking to the database here. Lazily initialized.
(def c3p0-connection-pool (atom nil))

(defn make-database-connection-dictionary-from-env-vars
  "Given a target schema name and assuming a variety of environmental
   variables are set, creates the metadata to connect to a database."
  [currentSchema]
  (merge {:dbtype "postgresql"
          :dbname (or (System/getenv "COOK_DB_TEST_PG_DB") "cook_local")
          ; Note that we need to save the currentSchema in the connection info so that we can delete the schema in
          ; test fixture teardown.
          :currentSchema currentSchema
          :host (or (System/getenv "COOK_DB_TEST_PG_SERVER") "127.0.0.1")
          :ssl false
          :sslfactory "org.postgresql.ssl.NonValidatingFactory"}
         ; PGPASSWORD is the default environmental variable used by postgres psql.
         (when-let [passwd (or  (System/getenv "COOK_DB_TEST_PG_PASSWORD")
                                (System/getenv "PGPASSWORD"))]
           {:password passwd})
         (when-let [username (System/getenv "COOK_DB_TEST_PG_USER")]
           {:user username})))

(defn make-c3p0-datasource
  "Given a database configuration dictionary, make a c3p0 connection pool for it."
  [{:keys [dbtype dbname currentSchema host ssl sslfactory password user c3p0-min-pool-size c3p0-max-pool-size]
    :or {c3p0-min-pool-size 2 c3p0-max-pool-size 20}
    :as pg-config}]
  (let [driverclass "org.postgresql.Driver"
        subname ""
        cpds (doto (ComboPooledDataSource.)
               (.setDriverClass driverclass)
               (.setForceUseNamedDriverClass true)
               (.setJdbcUrl (str "jdbc:" dbtype ":" subname "//" host "/" dbname "?" "currentSchema=" currentSchema))
               (.setMinPoolSize c3p0-min-pool-size)
               (.setMaxPoolSize c3p0-max-pool-size);
               ;; expire excess connections after 30 minutes of inactivity:
               (.setMaxIdleTimeExcessConnections (* 30 60))
               ;; expire connections after 3 hours of inactivity:
               (.setMaxIdleTime (* 3 60 60)))]
    (when password
      (.setPassword cpds password))
    (when user
      (.setUser cpds user))
    (sql/get-datasource cpds)))

(defn get-pg-config
  "Return access information for a postgresql database suitable for database access.

   There are four situations where we can run with a postgres database:

   1. Cook is running for real or in integration tests. The db configuration is set in config.edn.

   2. seed_pools.clj is running. (This seeds pools into an empty database for integration tests). In this environment,
      there is no config.edn, so we need to get a configuration out of environmental variables. This codepath is implicit.
      seed_k8s_pools.clj and seed_pools.clj sets saved-pg-config-dictionary directly.
      (TODO: We can use liquibase contexts to seed integration test data when the
      postgres migration is complete and remove this case.)

   3. We are running unit tests when the database is already configured. This is triggered by COOK_DB_TEST_PG_SCHEMA.
      In this case, the configuration is parsed from the environment and stuffed into saved-pg-config-dictionary by the
      unit test fixture.

   4. We are running unit tests when the database is not configured. This is triggered by COOK_DB_TEST_PG_SCHEMA not existing
      when we are running the test fixture. In that case, we make and load a new schema, then load it here. The test fixture
      code then tears it down.

   Note that cases 2 and 3 are essentially the same: Look for COOK_DB_TEST_PG_SCHEMA and stuff into a configuration dictionary.
   If we didn't have the fixture code do it in case 3 we could simply default it with an '(or ..'.

   In any case, once there's a configuration dictionary. The first time we try to get a connection, we make a
   persistent connection pool and store it into c3p0-connection-pool."
  []
  (or
    (-> config/config :settings :pg-config)
    @saved-pg-config-dictionary))

(defn pg-db
  "Get a database connection for talking to postgresql. Something that can be used as part of the object argument to a
  JDBC API function."
  []
  (locking c3p0-connection-pool
    (when-not @c3p0-connection-pool
      (reset! c3p0-connection-pool (make-c3p0-datasource (get-pg-config))))
    @c3p0-connection-pool))
