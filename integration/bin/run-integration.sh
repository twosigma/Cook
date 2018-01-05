#!/usr/bin/env bash

# Usage: run-integration.sh [OPTIONS] ARGS...
#
# Runs the integration tests for cook scheduler in a docker container.
#
# Options:
#   --port NUMBER        Port for a cook instance running in docker.
#   --port-2 NUMBER      If COOK_MULTI_CLUSTER is set, port for a second cook instance running in docker.
#   --slave-port NUMBER  If COOK_MASTER_SLAVE is set, port for a cook slave instance running in docker.
#   --no-mount           Do not mount working directory as a docker volume (mounted by default).
#
# ARGS...                Any trailing arguments are passed through to the docker container,
#                        overriding the default command used for integration testing.

set -euf -o pipefail

INTEGRATION_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"

COOK_PORT=12321
COOK_PORT_2=22321
COOK_SLAVE_PORT=12322
DOCKER_VOLUME_ARGS="-v ${INTEGRATION_DIR}:/opt/cook/integration"

while (( $# > 0 )); do
   case "$1" in
      --port)
         COOK_PORT="$2"
         shift 2
         ;;
      --port-2)
         COOK_PORT_2="$2"
         shift 2
         ;;
      --slave-port)
         COOK_SLAVE_PORT="$2"
         shift 2
         ;;
      --no-mount)
         DOCKER_VOLUME_ARGS=''
         shift
         ;;
      *) break
   esac
done

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
       ${COOK_MULTICLUSTER_ENV} ${DOCKER_VOLUME_ARGS} \
       cook-integration:latest \
       "$@"

# Connect to the default bridge network (for talking to minimesos)
docker network connect bridge cook-integration
# Connect to the cook_nw network (for talking to datomic-free and other cook instances using hostnames)
docker network connect cook_nw cook-integration

docker start -ai cook-integration
