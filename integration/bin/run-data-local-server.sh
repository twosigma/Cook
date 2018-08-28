#!/usr/bin/env bash

# Usage: run-data-local-server.sh
# Builds a docker image and launches the data local service.

INTEGRATION_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
SRC_DIR="${INTEGRATION_DIR}/src"
NAME=data-local

echo "Building docker images for ${NAME}"
docker build -t ${NAME} ${SRC_DIR}

# Expose port 5000 (default flask port)
docker create \
       -i \
       -t \
       --rm \
       -p 5000:5000 \
       --name=$NAME \
       ${NAME}:latest

docker network connect bridge ${NAME}
docker network connect cook_nw ${NAME}

docker start -ai ${NAME}
