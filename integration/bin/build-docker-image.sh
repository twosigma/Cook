#!/usr/bin/env bash

# Usage: build-docker-image.sh
# Builds a docker image containing the cook scheduler integration tests.

INTEGRATION_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
NAME=cook-integration

echo "Building docker images for ${NAME}"
cd $(dirname ${INTEGRATION_DIR})/cli
tar -czf ${INTEGRATION_DIR}/cli.tar.gz .
cd ${INTEGRATION_DIR}
docker build -t ${NAME} ${INTEGRATION_DIR}
rm cli.tar.gz
