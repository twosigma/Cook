#!/usr/bin/env bash

# Create the initial cook account and database.
echo "Initializing new account and database."
psql --set=cook_user_password="$PGPASSWORD" -h 127.0.0.1 -U postgres -f sql/docker_init_new_database.sql
echo "Setting up cook scheduler database"
psql --set=cook_schema="${COOK_SCHEMA}" -h 127.0.0.1 -U cook_scheduler -d cook_local -f sql/init_cook_database.sql
echo "Inserting initial rows for opensource integration tests"
psql --set=cook_schema="${COOK_SCHEMA}" -h 127.0.0.1 -U cook_scheduler -d cook_local -f sql/insert_rows_for_opensource_integration_tests.sql
