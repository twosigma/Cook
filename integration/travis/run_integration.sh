#!/bin/bash

# Relies on the COOK_EXECUTOR environment variable to choose which edn file to load.
# When set to 1, the Cook executor is enabled.
# Else, the Mesos Command executor is used.

set -ev

PYTEST_MARKS=''

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
  COOK_EXECUTOR_COMMAND="${TRAVIS_BUILD_DIR}/travis/cook-executor-local/cook-executor-local"
fi

# Build cook-executor
${PROJECT_DIR}/../travis/build_cook_executor.sh

# Start minimesos
cd ${PROJECT_DIR}/../travis
./minimesos up
$(./minimesos info | grep MINIMESOS)
export COOK_ZOOKEEPER="${MINIMESOS_ZOOKEEPER_IP}:2181"
export MINIMESOS_ZOOKEEPER=${MINIMESOS_ZOOKEEPER%;}

./datomic-free-0.9.5394/bin/transactor $(pwd)/datomic_transactor.properties &

# Start three cook schedulers. We want one cluster with two cooks to run MasterSlaveTest, and a second cluster to run MultiClusterTest.
# The basic tests will run against cook-framework-1
cd ${PROJECT_DIR}/../scheduler
## on travis, ports on 172.17.0.1 are bindable from the host OS, and are also
## available for processes inside minimesos containers to connect to
export COOK_EXECUTOR_COMMAND=${COOK_EXECUTOR_COMMAND}
# Start one cook listening on port 12321, this will be the master of the "cook-framework-1" framework
LIBPROCESS_IP=172.17.0.1 COOK_DATOMIC="datomic:free://localhost:4334/cook-jobs" COOK_PORT=12321 COOK_FRAMEWORK_ID=cook-framework-1 COOK_LOGFILE="log/cook-12321.log" lein run ${PROJECT_DIR}/travis/${CONFIG_FILE} &
# Start a second cook listening on port 22321, this will be the master of the "cook-framework-2" framework
LIBPROCESS_IP=172.17.0.1 COOK_DATOMIC="datomic:mem://cook-jobs" COOK_PORT=22321 COOK_ZOOKEEPER_LOCAL=true COOK_ZOOKEEPER_LOCAL_PORT=4291 COOK_FRAMEWORK_ID=cook-framework-2 COOK_LOGFILE="log/cook-22321.log" lein run ${PROJECT_DIR}/travis/${CONFIG_FILE} &

# Wait for the cooks to be listening
timeout 180s bash -c "wait_for_cook 12321" || curl_error=true
if [ "$curl_error" = true ]; then
  echo "$(date +%H:%M:%S) Timed out waiting for cook to start listening, displaying cook log"
  cat ${PROJECT_DIR}/../scheduler/log/cook-12321.log
  exit 1
fi

# Start a third cook listening on port 12322, this will be a slave on the "cook-framework-1" framework
LIBPROCESS_IP=172.17.0.1 COOK_DATOMIC="datomic:free://localhost:4334/cook-jobs" COOK_PORT=12322 COOK_FRAMEWORK_ID=cook-framework-1 COOK_LOGFILE="log/cook-12322.log" lein run ${PROJECT_DIR}/travis/${CONFIG_FILE} &

timeout 180s bash -c "wait_for_cook 12322" || curl_error=true
if [ "$curl_error" = true ]; then
  echo "$(date +%H:%M:%S) Timed out waiting for cook to start listening, displaying cook log"
  cat ${PROJECT_DIR}/../scheduler/log/cook-12322.log
  exit 1
fi
timeout 180s bash -c "wait_for_cook 22321" || curl_error=true
if [ "$curl_error" = true ]; then
    echo "$(date +%H:%M:%S) Timed out waiting for cook to start listening, displaying cook log"
    cat ${PROJECT_DIR}/../scheduler/log/cook-22321.log
    exit 1
fi

# Install the CLI
cd ${PROJECT_DIR}/../cli
python --version
pip install -e .
CLI=$(pyenv which cs)
export PATH=${PATH}:$(dirname ${CLI})
cs --help

# Run the integration tests
# We use pytest's --lf option to rerun only the tests that failed previously
# on each iteration of the loop. All tests are always run on the first iteration.
# This should help us avoid the need to rerun the whole suite due to a flakey test.
# We can also scrape the Travis logs to find which tests frequently flake.
cd ${PROJECT_DIR}
export COOK_MULTI_CLUSTER=
export COOK_MASTER_SLAVE=
export COOK_SLAVE_URL=http://localhost:12322
for ((i=1; i<5; i++)); do
    pytest -n4 -v --lf --color=no --timeout-method=thread --boxed -m "${PYTEST_MARKS}" && break
    test_failures=true
    failure_file="$(find .cache -name lastfailed)"
    failure_count=$(tail -n+2 $failure_file | wc -l)
    if [[ $failure_count > 3 ]]; then
        echo "Too many failures ($failure_count), not retrying..."
        break
    fi
    echo "Retrying tests due to failures (attempt $i)..."
done

# If there were failures, dump the executor logs
if [ "$test_failures" = true ]; then
  echo "FAILURE: Exhausted test retry attempts"
  echo "Displaying executor logs"
  ${PROJECT_DIR}/../travis/show_executor_logs.sh
  exit 1
fi
