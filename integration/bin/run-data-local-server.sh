#!/usr/bin/env bash

# Usage: run-data-local-server.sh
# Builds a docker image and launches the data local service.

INTEGRATION_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
SRC_DIR="${INTEGRATION_DIR}/src/data_locality"
NAME=data-local

echo "Building docker images for ${NAME}"
docker build -t ${NAME} ${SRC_DIR}

# Expose port 35847 (used for data-local service)
docker create \
       -i \
       -t \
       --rm \
       -p 35847:35847 \
       --name=$NAME \
       ${NAME}:latest

docker network connect bridge ${NAME}
docker network connect cook_nw ${NAME}

docker start -ai ${NAME}
