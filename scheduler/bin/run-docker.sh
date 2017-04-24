#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
NAME=cook-scheduler

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
    --publish=8888:8888 \
    --publish=12321:12321 \
    -e "COOK_PORT=12321" \
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
