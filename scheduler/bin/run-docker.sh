#!/usr/bin/env bash

# Usage: ./bin/run-docker.sh
# Runs the cook scheduler inside a docker container.

# Defaults (overridable via environment)
: ${COOK_PORT:=${1:-12321}}
: ${COOK_NREPL_PORT:=${2:-8888}}
: ${COOK_FRAMEWORK_ID:=cook-framework-$(date +%s)}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
NAME=cook-scheduler-${COOK_PORT}

if [ "$(docker ps -aq -f name=${NAME})" ]; then
    # Cleanup
    docker rm ${NAME}
fi

$(minimesos info | grep MINIMESOS)
EXIT_CODE=$?
if [ ${EXIT_CODE} -eq 0 ]
then
    ZK=${MINIMESOS_ZOOKEEPER%;}
    echo "ZK = ${ZK}"
    echo "MINIMESOS_MASTER_IP = ${MINIMESOS_MASTER_IP}"
else
    echo "Could not get ZK URI from minimesos; you may need to restart minimesos"
    exit ${EXIT_CODE}
fi

SCHEDULER_DIR="$( dirname ${DIR} )"
SCHEDULER_EXECUTOR_DIR=${SCHEDULER_DIR}/resources/public

if [ -z "$(docker network ls -q -f name=cook_nw)" ];
then
    # Using a separate network allows us to access hosts by name (cook-scheduler-12321)
    # instead of IP address which simplifies configuration
    echo "Creating cook_nw network"
    docker network create -d bridge --subnet 172.25.0.0/16 cook_nw
fi

if [ -z "${COOK_DATOMIC_URI}" ];
then
    COOK_DATOMIC_URI="datomic:mem://cook-jobs"
fi

if [ "${COOK_ZOOKEEPER_LOCAL}" = false ] ; then
    COOK_ZOOKEEPER="${MINIMESOS_ZOOKEEPER_IP}:2181"
else
    COOK_ZOOKEEPER=""
    COOK_ZOOKEEPER_LOCAL=true
fi

echo "Starting cook..."

# NOTE: since the cook scheduler directory is mounted as a volume
# by the minimesos agents, they have access to the cook-executor binary
# using the absolute file path URI given for COOK_EXECUTOR below.
docker create \
    -i \
    -t \
    --rm \
    --name=${NAME} \
    --publish=${COOK_NREPL_PORT}:${COOK_NREPL_PORT} \
    --publish=${COOK_PORT}:${COOK_PORT} \
    -e "COOK_EXECUTOR=file://${SCHEDULER_EXECUTOR_DIR}/cook-executor-docker" \
    -e "COOK_EXECUTOR_COMMAND=./cook-executor-docker" \
    -e "COOK_PORT=${COOK_PORT}" \
    -e "COOK_NREPL_PORT=${COOK_NREPL_PORT}" \
    -e "COOK_FRAMEWORK_ID=${COOK_FRAMEWORK_ID}" \
    -e "MESOS_MASTER=${ZK}" \
    -e "MESOS_MASTER_HOST=${MINIMESOS_MASTER_IP}" \
    -e "COOK_ZOOKEEPER=${COOK_ZOOKEEPER}" \
    -e "COOK_ZOOKEEPER_LOCAL=${COOK_ZOOKEEPER_LOCAL}" \
    -e "COOK_HOSTNAME=${NAME}" \
    -e "COOK_DATOMIC_URI=${COOK_DATOMIC_URI}" \
    -e "COOK_LOG_FILE=log/cook-${COOK_PORT}.log" \
    -v ${DIR}/../log:/opt/cook/log \
    cook-scheduler:latest

docker network connect bridge ${NAME}
docker network connect cook_nw ${NAME}
docker start -ai ${NAME}

# If Cook is not starting, you may be able to troubleshoot by
# adding the following line right after the `docker run` line:
#
#    --entrypoint=/bin/bash \
#
# This will override the ENTRYPOINT baked into the Dockerfile
# and instead give you an interactive bash shell.
