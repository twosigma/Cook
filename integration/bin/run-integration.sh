#!/usr/bin/env bash

# Usage: run-integration.sh COOK_PORT_1 COOK_PORT_2 COOK_SLAVE_PORT

# Runs the integration tests for cook scheduler in a docker container.
# COOK_PORT_1 (Optional) - Port for a cook instance running in docker.
# COOK_PORT_2 (Optional) - If COOK_MULTI_CLUSTER is set, port for a second cook instance
#                          running in docker.
# COOK_SLAVE_PORT (Optional) - If COOK_MASTER_SLAVE is set, port for a cook slave instance
#                              running in docker.

set -euf -o pipefail

INTEGRATION_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
COOK_PORT=${1:-12321}
COOK_PORT_2=${2:-22321}
COOK_SLAVE_PORT=${2:-12322}

NAME=cook-integration
if [ "$(docker ps -aq -f name=${NAME})" ]
then
    docker rm ${NAME}
fi

COOK_NAME=cook-scheduler-${COOK_PORT}

COOK_IP=$(docker inspect ${COOK_NAME} | jq -r '.[].NetworkSettings.IPAddress')
COOK_URL="http://${COOK_IP}:${COOK_PORT}"

COOK_MULTICLUSTER_ENV=""
if [ -n "${COOK_MULTI_CLUSTER+1}" ];
then
    COOK_NAME_2=cook-scheduler-${COOK_PORT_2}
    COOK_IP_2=$(docker inspect ${COOK_NAME_2} | jq -r '.[].NetworkSettings.IPAddress')
    COOK_URL_2="http://${COOK_IP_2}:${COOK_PORT_2}"
    COOK_MULTICLUSTER_ENV="-e COOK_SCHEDULER_URL_2=${COOK_URL_2} -e COOK_MULTI_CLUSTER=${COOK_MULTI_CLUSTER}"
fi
if [ -n "${COOK_MASTER_SLAVE+1}" ] ;
then
   COOK_SLAVE_NAME=cook-scheduler-${COOK_SLAVE_PORT}
   COOK_SLAVE_IP=$(docker inspect ${COOK_SLAVE_NAME} | jq -r '.[].NetworkSettings.IPAddress')
   COOK_SLAVE_URL="http://${COOK_SLAVE_IP}:${COOK_SLAVE_PORT}"
   COOK_MULTICLUSTER_ENV="${COOK_MULTICLUSTER_ENV} -e COOK_SLAVE_URL=${COOK_SLAVE_URL} -e COOK_MASTER_SLAVE=${COOK_MASTER_SLAVE}"
fi


docker create \
       --rm \
       --name=cook-integration \
       -e "COOK_SCHEDULER_URL=${COOK_URL}" \
       -e "USER=root" \
       ${COOK_MULTICLUSTER_ENV} \
       -v ${INTEGRATION_DIR}:/opt/cook/integration \
       -v /var/run/docker.sock:/var/run/docker.sock \
       cook-integration:latest

# Connect to the default bridge network (for talking to minimesos)
docker network connect bridge cook-integration
# Connect to the cook_nw network (for talking to datomic-free and other cook instances using hostnames)
docker network connect cook_nw cook-integration

docker start -ai cook-integration
