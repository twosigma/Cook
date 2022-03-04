#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"

# Create the initial cook account and database.
echo "#### Initializing new account and database."
sudo -u postgres psql --set=cook_user_password="$PGPASSWORD" -f ${DIR}/../sql/docker_init_new_database.sql

echo "#### Running script to create convenience SQL schema cook_local"
${DIR}/vagrant-setup-new-schema.sh ${COOK_SCHEMA}

echo "#### Setting up rows for opensource integration tests."
psql --set=cook_schema="${COOK_SCHEMA}" -h 127.0.0.1 -U cook_scheduler -d cook_local -f ${DIR}/../sql/insert_rows_for_opensource_integration_tests.sql

