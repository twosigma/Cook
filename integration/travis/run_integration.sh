#!/bin/bash

# Relies on the COOK_EXECUTOR environment variable to choose which edn file to load.
# When set to 1, the Cook executor is enabled.
# Else, the Mesos Command executor is used.

set -ev

NOSE_ATTRIBUTES=${1:-'!explicit'}

function wait_for_cook {
    COOK_PORT=${1:-12321}
    while ! curl -s localhost:${COOK_PORT} >/dev/null;
    do
        echo "$(date +%H:%M:%S) Cook is not listening on ${COOK_PORT} yet"
        sleep 2.0
    done
    echo "$(date +%H:%M:%S) Connected to Cook on ${COOK_PORT}!"
}
export -f wait_for_cook

export PROJECT_DIR=`pwd`

CONFIG_FILE="scheduler_config.edn"
COOK_EXECUTOR_COMMAND=""
if [ "${COOK_EXECUTOR}" = "1" ]
then
  echo "$(date +%H:%M:%S) Cook executor has been enabled"
  COOK_EXECUTOR_COMMAND="/home/travis/build/twosigma/Cook/travis/cook-executor"
fi

# Build cook-executor
${PROJECT_DIR}/../travis/build_cook_executor.sh

# Start minimesos
cd ${PROJECT_DIR}/../travis
./minimesos up

# Start two cook schedulers
cd ${PROJECT_DIR}/../scheduler
## on travis, ports on 172.17.0.1 are bindable from the host OS, and are also
## available for processes inside minimesos containers to connect to
export COOK_EXECUTOR_COMMAND=${COOK_EXECUTOR_COMMAND}
LIBPROCESS_IP=172.17.0.1 COOK_PORT=12321 COOK_ZOOKEEPER_LOCAL_PORT=3291 COOK_FRAMEWORK_ID=cook-framework-1 lein run ${PROJECT_DIR}/travis/${CONFIG_FILE} &
LIBPROCESS_IP=172.17.0.1 COOK_PORT=22321 COOK_ZOOKEEPER_LOCAL_PORT=4291 COOK_FRAMEWORK_ID=cook-framework-2 lein run ${PROJECT_DIR}/travis/${CONFIG_FILE} &

# Wait for the cooks to be listening
timeout 180s bash -c "wait_for_cook 12321" || curl_error=true
if [ "$curl_error" = true ]; then
  echo "$(date +%H:%M:%S) Timed out waiting for cook to start listening, displaying cook log"
  cat ${PROJECT_DIR}/../scheduler/log/cook.log
  exit 1
fi
timeout 180s bash -c "wait_for_cook 22321" || curl_error=true
if [ "$curl_error" = true ]; then
  echo "$(date +%H:%M:%S) Timed out waiting for cook to start listening, displaying cook log"
  cat ${PROJECT_DIR}/../scheduler/log/cook.log
  exit 1
fi

# Run the integration tests
cd ${PROJECT_DIR}
COOK_MULTI_CLUSTER= python3 setup.py nosetests --attr ${NOSE_ATTRIBUTES} || test_failures=true

# If there were failures, dump the executor logs
if [ "$test_failures" = true ]; then
  echo "Displaying executor logs"
  ${PROJECT_DIR}/../travis/show_executor_logs.sh
  exit 1
fi
