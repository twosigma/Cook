#!/usr/bin/env bash

# Create the initial cook account and database.
echo "#### Initializing new account and database."
psql --set=cook_user_password="$PGPASSWORD" -h 127.0.0.1 -U postgres -f sql/docker_init_new_database.sql

echo "#### Running script to create convenience SQL schema cook_local"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"

export COOK_DB_TEST_PG_DATABASE=cook_local
export COOK_DB_TEST_PG_USER=cook_scheduler
export COOK_DB_TEST_PG_SERVER=cook-postgres
${DIR}/setup-new-schema.sh ${COOK_SCHEMA}

echo "#### Setting up rows for opensource integration tests."
psql --set=cook_schema="${COOK_SCHEMA}" -h 127.0.0.1 -U cook_scheduler -d cook_local -f sql/insert_rows_for_opensource_integration_tests.sql

