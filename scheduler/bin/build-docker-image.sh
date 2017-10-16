#!/usr/bin/env bash

SCHEDULER_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
NAME=cook-scheduler

EXECUTOR_DIR="$(dirname ${SCHEDULER_DIR})/executor"
COOK_EXECUTOR_FILE=${EXECUTOR_DIR}/dist/cook-executor
SCHEDULER_EXECUTOR_DIR=${SCHEDULER_DIR}/resources/public

if [ ! -f ${COOK_EXECUTOR_FILE} ]; then
    echo "cook-executor not found at ${COOK_EXECUTOR_FILE}"
    DO_EXECUTOR_REBUILD=true
elif ! (cd ../executor && ./bin/check-version.sh -q); then
    echo "cook-executor appears to be out of date"
    DO_EXECUTOR_REBUILD=true
else
    DO_EXECUTOR_REBUILD=false
fi

if $DO_EXECUTOR_REBUILD; then
    echo "Triggering build of cook-executor before proceeding."
    ${EXECUTOR_DIR}/bin/build-cook-executor.sh
fi

echo "Copying cook-executor from ${COOK_EXECUTOR_FILE} to ${SCHEDULER_EXECUTOR_DIR}"
mkdir -p ${SCHEDULER_EXECUTOR_DIR}
cp -f ${COOK_EXECUTOR_FILE} ${SCHEDULER_EXECUTOR_DIR}


echo "Building docker images for ${NAME}"
docker build -t ${NAME} ${SCHEDULER_DIR}
