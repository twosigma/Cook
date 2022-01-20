#!/usr/bin/env bash

DATOMIC_PROPERTIES_FILE=/opt/cook/datomic/datomic_transactor.properties

echo "alt-host=$(hostname -i | cut -d' ' -f2)" >> ${DATOMIC_PROPERTIES_FILE}
/opt/cook/datomic-free-0.9.5561.56/bin/transactor ${DATOMIC_PROPERTIES_FILE} &
echo "Seeding test data..."
# Needed because seeding pools uses codepaths that access the database.
export COOK_DB_TEST_PG_DB="cook_local"
export COOK_DB_TEST_PG_USER="cook_scheduler"
export COOK_DB_TEST_PG_SERVER="cook-postgres"
export COOK_DB_TEST_PG_SCHEMA="cook_local"
lein exec -p /opt/cook/datomic/data/seed_k8s_pools.clj ${COOK_DATOMIC_URI}
lein exec -p /opt/cook/datomic/data/seed_running_jobs.clj ${COOK_DATOMIC_URI}
lein with-profiles +docker run $1
