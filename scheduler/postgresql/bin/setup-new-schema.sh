#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"


echo "Running script to create schema '$1' out of directory $DIR/../sql"

time psql -h localhost -U cook_scheduler -d cook_local --set=cook_schema="$1" -f ${DIR}/../sql/init_cook_database.sql

echo "Finished script"
