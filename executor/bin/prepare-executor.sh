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

set -e

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

COOK_EXECUTOR_NAME="cook-executor-${MODE}"
if [[ "${MODE}" == docker ]]; then
    COOK_EXECUTOR_NAME="cook-executor"
fi

COOK_EXECUTOR_PATH="${EXECUTOR_DIR}/dist/${COOK_EXECUTOR_NAME}"
if [ ! -d ${COOK_EXECUTOR_PATH} ]; then
    echo "${COOK_EXECUTOR_NAME} not found at ${COOK_EXECUTOR_PATH}"
    DO_EXECUTOR_REBUILD=true
elif ! ${EXECUTOR_DIR}/bin/check-version.sh -q ${COOK_EXECUTOR_NAME}; then
    echo "${COOK_EXECUTOR_NAME} appears to be out of date"
    DO_EXECUTOR_REBUILD=true
else
    DO_EXECUTOR_REBUILD=false
fi

COOK_EXECUTOR_ZIP_NAME="${COOK_EXECUTOR_NAME}.tar.gz"
COOK_EXECUTOR_ZIP_FILE="${EXECUTOR_DIR}/dist/${COOK_EXECUTOR_ZIP_NAME}"
if $DO_EXECUTOR_REBUILD; then
    echo "Triggering build of ${COOK_EXECUTOR_NAME} before proceeding."
    ${EXECUTOR_DIR}/bin/build-${MODE}.sh
    echo "Zipping contents of ${COOK_EXECUTOR_PATH}"
    pushd ${EXECUTOR_DIR}/dist
    tar -cvzf ${COOK_EXECUTOR_ZIP_FILE} ${COOK_EXECUTOR_NAME}
    popd
else
    echo "Not triggering build of ${COOK_EXECUTOR_NAME}"
fi


if [ "${COOK_EXECUTOR_ZIP_FILE}" -nt "${TARGET_DIR}/${COOK_EXECUTOR_ZIP_NAME}" ]; then
    echo "Copying ${COOK_EXECUTOR_ZIP_NAME} from ${COOK_EXECUTOR_ZIP_FILE} to ${TARGET_DIR}"
    mkdir -p ${TARGET_DIR}
    cp -f ${COOK_EXECUTOR_ZIP_FILE} ${TARGET_DIR}
else
    echo "Not copying ${COOK_EXECUTOR_ZIP_NAME} to ${TARGET_DIR}"
fi
