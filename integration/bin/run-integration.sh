#!/usr/bin/env bash

# Usage: run-integration.sh COOK_PORT_1 COOK_PORT_2

# Runs the integration tests for cook scheduler in a docker container.
# COOK_PORT_1 - Port for a cook instance running in docker.
# COOK_PORT_2 - If COOK_MULTI_CLUSTER is set, port for a second cook instance
#               running in docker.

set -euf -o pipefail

INTEGRATION_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
COOK_PORT=${1:-12321}
COOK_PORT_2=${2:-22321}

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
    COOK_MULTICLUSTER_ENV="-e COOK_SCHEDULER_URL_2=${COOK_URL_2} -e COOK_MULTI_CLUSTER=true"
fi

docker run \
       --name=cook-integration \
       -e "COOK_SCHEDULER_URL=${COOK_URL}" \
       ${COOK_MULTICLUSTER_ENV} \
       -v ${INTEGRATION_DIR}:/opt/cook/integration \
       cook-integration:latest
