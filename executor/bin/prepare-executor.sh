#!/usr/bin/env bash

# USAGE: ./bin/prepare-executor.sh MODE TARGET_DIR
# Builds the cook executor and then copies it to TARGET_DIR
# Examples:
#   ./bin/prepare-executor.sh docker /target/directory
#   ./bin/prepare-executor.sh local /target/directory

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
EXECUTOR_DIR="$(dirname ${DIR})"
MODE=${1}
TARGET_DIR=${2}

set +e

if [ -z "${MODE}" ]; then
    echo "ERROR: mode has not been specified!"
    exit 1
fi

if [[ ! "${MODE}" =~ ^(docker|local)$ ]]; then
    echo "ERROR: invalid mode (${MODE}) specified!"
    exit 1
fi

if [ -z "${TARGET_DIR}" ]; then
    echo "ERROR: target directory has not been specified!"
    exit 1
fi

COOK_EXECUTOR_FILE="${EXECUTOR_DIR}/dist/cook-executor-${MODE}"
if [ ! -f ${COOK_EXECUTOR_FILE} ]; then
    echo "cook-executor not found at ${COOK_EXECUTOR_FILE}"
    DO_EXECUTOR_REBUILD=true
elif ! ${EXECUTOR_DIR}/bin/check-version.sh -q; then
    echo "cook-executor appears to be out of date"
    DO_EXECUTOR_REBUILD=true
else
    DO_EXECUTOR_REBUILD=false
fi

if $DO_EXECUTOR_REBUILD; then
    echo "Triggering build of cook-executor before proceeding."
    ${EXECUTOR_DIR}/bin/build-${MODE}.sh
fi

if [ "$COOK_EXECUTOR_FILE" -nt "$TARGET_FILE" ]; then
    echo "Copying cook-executor from ${COOK_EXECUTOR_FILE} to ${TARGET_DIR}"
    mkdir -p ${TARGET_DIR}
    cp -f ${COOK_EXECUTOR_FILE} ${TARGET_DIR}
else
    echo "ERROR: cook-executor file ${COOK_EXECUTOR_FILE} does not exist!"
    exit 1
fi
