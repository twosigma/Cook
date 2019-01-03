#!/usr/bin/env bash

# Usage: run-demo-hook-service.sh
# Builds a docker image and launches the demo hook service.

INTEGRATION_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
SRC_DIR="${INTEGRATION_DIR}/src/demo_hook_service"
NAME=demo-hook-service

echo "Building docker images for ${NAME}"
docker build -t ${NAME} ${SRC_DIR}

# Expose port 5131 (used for demo hook service)
docker create \
       -i \
       -t \
       --rm \
       -p 5131:5131 \
       --name=$NAME \
       ${NAME}:latest

docker network connect bridge ${NAME}
docker network connect cook_nw ${NAME}

docker start -ai ${NAME}
