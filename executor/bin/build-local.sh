#!/usr/bin/env bash

# Usage: ./bin/build-local.sh
# Builds the version of cook executor that can execute locally.

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
NAME=cook-executor-build

EXECUTOR_DIR="$(dirname ${DIR})"

mkdir -p ${EXECUTOR_DIR}/dist

cd ${EXECUTOR_DIR}
pyinstaller -F -n cook-executor-local -p cook cook/__main__.py
