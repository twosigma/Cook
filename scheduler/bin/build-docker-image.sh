#!/usr/bin/env bash

# Usage: build-docker-image.sh
# Builds a docker image containing the cook scheduler.

set -e

SCHEDULER_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
NAME=cook-scheduler

EXECUTOR_DIR="$(dirname ${SCHEDULER_DIR})/executor"
EXECUTOR_NAME=cook-executor
COOK_EXECUTOR_FILE=${EXECUTOR_DIR}/dist/${EXECUTOR_NAME}
SCHEDULER_EXECUTOR_DIR=${SCHEDULER_DIR}/resources/public
SCHEDULER_EXECUTOR_FILE=${SCHEDULER_EXECUTOR_DIR}/${EXECUTOR_NAME}

${EXECUTOR_DIR}/bin/prepare-executor.sh docker ${SCHEDULER_EXECUTOR_DIR}

echo "Building docker images for ${NAME}"
docker build -t ${NAME} ${SCHEDULER_DIR}
