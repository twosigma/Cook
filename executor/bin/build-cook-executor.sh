#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
NAME=cook-executor-build

if [ "$(docker ps -aq -f name=${NAME})" ]; then
    # Cleanup
    docker rm ${NAME}
fi

EXECUTOR_DIR="$(dirname ${DIR})"

mkdir -p ${EXECUTOR_DIR}/dist
docker build -t ${NAME} -f ${EXECUTOR_DIR}/Dockerfile.build .
docker run --name ${NAME} ${NAME}
docker cp ${NAME}:/opt/cook/dist/cook-executor ${EXECUTOR_DIR}/dist/cook-executor
