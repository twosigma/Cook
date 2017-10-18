#!/usr/bin/env bash

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
NAME=cook-executor-build

EXECUTOR_DIR="$(dirname ${DIR})"

mkdir -p ${EXECUTOR_DIR}/dist

# build cook-executor inside docker image to avoid local python environment and architecture hassles
cd ${EXECUTOR_DIR}
docker build -t ${NAME} -f ${EXECUTOR_DIR}/Dockerfile.build .
docker run --name ${NAME} ${NAME}
docker cp ${NAME}:/opt/cook/dist/cook-executor ${EXECUTOR_DIR}/dist/cook-executor
docker rm ${NAME}
