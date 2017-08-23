#!/usr/bin/env bash

COOK_PORT=${1:-12321}
COOK_NREPL_PORT=${2:-8888}

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
EXECUTOR_DIR="$(dirname ${SCHEDULER_DIR})/executor"
COOK_EXECUTOR_FILE=${EXECUTOR_DIR}/dist/cook-executor
SCHEDULER_EXECUTOR_DIR=${SCHEDULER_DIR}/resources/public

if [ ! -f ${COOK_EXECUTOR_FILE} ]; then
    echo "cook-executor not found at ${COOK_EXECUTOR_FILE}"
    echo "Triggering build of cook-executor before proceeding."
    ${EXECUTOR_DIR}/bin/build-cook-executor.sh
fi
echo "Copying cook-executor from ${COOK_EXECUTOR_FILE} to ${SCHEDULER_EXECUTOR_DIR}"
mkdir -p ${SCHEDULER_EXECUTOR_DIR}
cp -f ${COOK_EXECUTOR_FILE} ${SCHEDULER_EXECUTOR_DIR}

if [ -z "$(docker network ls -q -f name=cook_nw)" ];
then
    echo "Creating cook_nw network"
    docker network create -d bridge --subnet 172.25.0.0/16 cook_nw
fi

. $DIR/start-datomic.sh

echo "Starting cook..."
docker create \
    -i \
    -t \
    --rm \
    --name=${NAME} \
    --publish=${COOK_NREPL_PORT}:${COOK_NREPL_PORT} \
    --publish=${COOK_PORT}:${COOK_PORT} \
    -e "COOK_EXECUTOR=file://${SCHEDULER_EXECUTOR_DIR}/cook-executor" \
    -e "COOK_PORT=${COOK_PORT}" \
    -e "COOK_NREPL_PORT=${COOK_NREPL_PORT}" \
    -e "COOK_FRAMEWORK_ID=${COOK_FRAMEWORK_ID:-cook-framework-${COOK_PORT}}" \
    -e "MESOS_MASTER=${ZK}" \
    -e "MESOS_MASTER_HOST=${MINIMESOS_MASTER_IP}" \
    -e "COOK_ZOOKEEPER=${MINIMESOS_ZOOKEEPER_IP}:2181" \
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
