#!/usr/bin/env bash

# Usage: build-docker-image.sh
# Builds a docker image containing the cook scheduler integration tests.

INTEGRATION_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
NAME=cook-integration

echo "Building docker images for ${NAME}"
docker build -t ${NAME} ${INTEGRATION_DIR}
