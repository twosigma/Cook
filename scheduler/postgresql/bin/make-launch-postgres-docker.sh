#!/usr/bin/env bash

###
### Reset any existing postgres docker container and make and configure one afresh.
###
### Sets the password to $PGPASSWORD
###

if [[ a"$PGPASSWORD" == a ]];
then
   echo "Need to set PGPASSWORD."
   exit 1
fi


## Copied from run-docker.sh
echo "About to: Setup and check docker networking"
if [ -z "$(docker network ls -q -f name=cook_nw)" ];
then
    # Using a separate network allows us to access hosts by name (cook-scheduler-12321)
    # instead of IP address which simplifies configuration
    echo "Creating cook_nw network"
    docker network create -d bridge --subnet 172.25.0.0/16 cook_nw
fi


echo "#### Flushing existing docker containers  `date`"

# Flush any existing containers.
docker kill cook-postgres || true
docker container rm cook-postgres || true

echo "#### Launching database  `date`"

# This launches the database. We give it a hostname of cook-postgres so that we can connect to
# the container using psql -h ...., later.
docker run --name cook-postgres --hostname cook-postgres --publish=5432:5432 --rm --network cook_nw -e POSTGRES_PASSWORD="${PGPASSWORD}" -d postgres:13

echo "#### Pausing for the DB to restart before setting it up."
sleep 4

export COOK_SCHEMA=cook_local

#
# Finish postgres setup in the container.
#

# Create the initial cook account and database.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"
${DIR}/setup-database.sh

# See the README.txt to see how to access this interactively.
