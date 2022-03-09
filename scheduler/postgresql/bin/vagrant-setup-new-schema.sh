#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"

COOK_SCHEMA=${1}

echo "### Started script to create schema '${COOK_SCHEMA}' out of directory $DIR/../sql"

# Liquibase setup:
echo "## Running PSQL to create schema"
psql --set=cook_schema="${COOK_SCHEMA}" -h 127.0.0.1 -U cook_scheduler -d cook_local -f ${DIR}/../sql/init_cook_database.sql

echo "## Liquibase setup."

PG_JDBC_URL="jdbc:postgresql://${COOK_DB_TEST_PG_SERVER}/${COOK_DB_TEST_PG_DATABASE}?user=${COOK_DB_TEST_PG_USER}&password=${PGPASSWORD}&currentSchema=${COOK_SCHEMA}"

# Note that liquibase must run from scheduler/liquibase and --changeLogFile is relative to scheduler/liquibase and MUST be a relative path.
cd ${DIR}/../../liquibase
liquibase --classpath=/usr/share/java/postgresql.jar --changeLogFile=changelog/com/twosigma/cook/changelogs/setup.postgresql.sql --url ${PG_JDBC_URL} --liquibaseSchemaName=${COOK_SCHEMA} update

echo "### Finished script creating schema ${COOK_SCHEMA}"
