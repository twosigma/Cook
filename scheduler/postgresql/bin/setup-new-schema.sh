#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"

COOK_SCHEMA=${1}

echo "### Started script to create schema '${COOK_SCHEMA}' out of directory $DIR/../sql"

# Liquibase setup:
echo "## Running PSQL to create schema"
psql --set=cook_schema="${COOK_SCHEMA}" -h 127.0.0.1 -U cook_scheduler -d cook_local -f ${DIR}/../sql/init_cook_database.sql

echo "## Liquibase setup."
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"
LIQUIBASE="${DIR}/../../liquibase"

export COOK_DB_TEST_PG_DATABASE=cook_local
export COOK_DB_TEST_PG_USER=cook_scheduler
export COOK_DB_TEST_PG_SERVER=cook-postgres

PG_JDBC_URL="jdbc:postgresql://${COOK_DB_TEST_PG_SERVER}/${COOK_DB_TEST_PG_DATABASE}?user=${COOK_DB_TEST_PG_USER}&password=${PGPASSWORD}&currentSchema=${COOK_SCHEMA}"

# Note that --changeLogFile is relative to /liquibase in the container, so comes from the -v volume mountpoint, and MUST be a relative path.
docker run --network cook_nw --rm -v ${LIQUIBASE}/changelog:/liquibase/changelog liquibase/liquibase:4.6 --changeLogFile=./changelog/com/twosigma/cook/changelogs/setup.postgresql.sql --url ${PG_JDBC_URL} --liquibase-schema-name=${COOK_SCHEMA} update

echo "### Finished script creating schema ${COOK_SCHEMA}"
