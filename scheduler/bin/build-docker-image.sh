#!/usr/bin/env bash

SCHEDULER_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
NAME=cook-scheduler

echo "Building docker images for ${NAME}"
docker build -t ${NAME} ${SCHEDULER_DIR}
