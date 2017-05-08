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

echo "Starting cook..."
docker run \
    -i \
    -t \
    --network=bridge \
    --name=${NAME} \
    --publish=${COOK_NREPL_PORT}:${COOK_NREPL_PORT} \
    --publish=${COOK_PORT}:${COOK_PORT} \
    -e "COOK_PORT=${COOK_PORT}" \
    -e "COOK_NREPL_PORT=${COOK_NREPL_PORT}" \
    -e "MESOS_MASTER=${ZK}" \
    -e "MESOS_MASTER_HOST=${MINIMESOS_MASTER_IP}" \
    -v ${DIR}/../log:/opt/cook/log \
    cook-scheduler:latest

# If Cook is not starting, you may be able to troubleshoot by
# adding the following line right after the `docker run` line:
#
#    --entrypoint=/bin/bash \
#
# This will override the ENTRYPOINT baked into the Dockerfile
# and instead give you an interactive bash shell.
