#!/usr/bin/env bash

# Usage: build-docker-image.sh
# Builds a docker image containing the cook scheduler.

SCHEDULER_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
NAME=cook-scheduler

EXECUTOR_DIR="$(dirname ${SCHEDULER_DIR})/executor"
COOK_EXECUTOR_FILE=${EXECUTOR_DIR}/dist/cook-executor-docker
SCHEDULER_EXECUTOR_DIR=${SCHEDULER_DIR}/resources/public
SCHEDULER_EXECUTOR_FILE=${SCHEDULER_EXECUTOR_DIR}/cook-executor-docker

${EXECUTOR_DIR}/bin/prepare-executor.sh docker ${SCHEDULER_EXECUTOR_DIR}

echo "Building docker images for ${NAME}"
docker build -t ${NAME} ${SCHEDULER_DIR}
