#!/usr/bin/env bash

# Usage: ./bin/build-local.sh
# Builds the version of cook executor that can execute locally.

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
NAME=cook-executor-build

EXECUTOR_DIR="$(dirname ${DIR})"

mkdir -p ${EXECUTOR_DIR}/dist
rm -rf ${EXECUTOR_DIR}/dist/cook-executor-local

cd ${EXECUTOR_DIR}
pyinstaller --onedir --name cook-executor-local --paths cook cook/__main__.py
