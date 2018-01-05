#!/bin/bash

set -ev

SCHEDULER_EXECUTOR_DIR=$(dirname $PROJECT_DIR)/scheduler/resources/public

cd $(dirname $PROJECT_DIR)/executor
pip install -r requirements.txt
./bin/prepare-executor.sh local $SCHEDULER_EXECUTOR_DIR
tar -C $(dirname $PROJECT_DIR)/travis -xzf $SCHEDULER_EXECUTOR_DIR/cook-executor-local.tar.gz
