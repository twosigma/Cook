#!/bin/bash

# Usage: ./run_integration [OPTIONS...]

set -ev

export PROJECT_DIR=`pwd`

CONFIG_FILE=scheduler_travis_config.edn

function wait_for_cook {
    COOK_PORT=${1:-12321}
    while ! curl -s localhost:${COOK_PORT} >/dev/null;
    do
        echo "$(date +%H:%M:%S) Cook is not listening on ${COOK_PORT} yet"
        sleep 2.0
    done
    echo "$(date +%H:%M:%S) Connected to Cook on ${COOK_PORT}!"
    curl -s localhost:${COOK_PORT}/info
    echo
}
export -f wait_for_cook

# Start minimesos
cd ${TRAVIS_BUILD_DIR}/travis
./minimesos up
$(./minimesos info | grep MINIMESOS)
export COOK_ZOOKEEPER="${MINIMESOS_ZOOKEEPER_IP}:2181"
export MINIMESOS_ZOOKEEPER=${MINIMESOS_ZOOKEEPER%;}
export MINIMESOS_MASTER=${MINIMESOS_MASTER%;}

SCHEDULER_DIR=${TRAVIS_BUILD_DIR}/scheduler
COOK_DATOMIC_URI_1=datomic:mem://cook-jobs

# Generate SSL certificate
COOK_KEYSTORE_PATH=${SCHEDULER_DIR}/cook.p12
keytool -genkeypair -keystore ${COOK_KEYSTORE_PATH} -storetype PKCS12 -storepass cookstore -dname "CN=cook, OU=Cook Developers, O=Two Sigma Investments, L=New York, ST=New York, C=US" -keyalg RSA -keysize 2048
export COOK_KEYSTORE_PATH=${COOK_KEYSTORE_PATH}

mkdir ${SCHEDULER_DIR}/log

# Launch the demo hook manager.
export DEMO_HOOKS_PORT=5131
export DEMO_HOOKS_SERVICE="http://localhost:${DEMO_HOOKS_PORT}"
export COOK_DEMO_HOOKS_SUBMIT_URL="${DEMO_HOOKS_SERVICE}/get-submit-status"
export COOK_DEMO_HOOKS_LAUNCH_URL="${DEMO_HOOKS_SERVICE}/get-launch-status"
FLASK_APP=${PROJECT_DIR}/src/demo_hook_service/service.py flask run --port=${DEMO_HOOKS_PORT} > ${SCHEDULER_DIR}/log/demo-hooks.log 2>&1 &


cd ${SCHEDULER_DIR}

# Start two cook schedulers.
export COOK_HTTP_BASIC_AUTH=true
export COOK_EXECUTOR_COMMAND=""
## We launch two instances, with different configurations for the different unit tests.
## on travis, ports on 172.17.0.1 are bindable from the host OS, and are also
## available for processes inside minimesos containers to connect to
# Start one cook listening on port 12321, this will be the master of the "cook-framework-1" framework
export GLOBAL_JOB_LAUNCH_RATE_LIMIT_BUCKET_SIZE=10000
export GLOBAL_JOB_LAUNCH_RATE_LIMIT_REPLENISHED_PER_MINUTE=10000
export JOB_LAUNCH_RATE_LIMIT_BUCKET_SIZE=10
export JOB_LAUNCH_RATE_LIMIT_REPLENISHED_PER_MINUTE=5
LIBPROCESS_IP=172.17.0.1 COOK_DATOMIC="${COOK_DATOMIC_URI_1}" COOK_PORT=12321 COOK_SSL_PORT=12322 COOK_COOKEEPER_LOCAL=true COOK_COOKEEPER_LOCAL_PORT=5291 COOK_FRAMEWORK_ID=cook-framework-1 COOK_LOGFILE="log/cook-12321.log" COOK_DEFAULT_POOL=${DEFAULT_POOL} lein run ${PROJECT_DIR}/travis/${CONFIG_FILE} &
# Start a second cook listening on port 22321, this will be the master of the "cook-framework-2" framework
export JOB_LAUNCH_RATE_LIMIT_BUCKET_SIZE=10000
export JOB_LAUNCH_RATE_LIMIT_REPLENISHED_PER_MINUTE=10000
export GLOBAL_JOB_LAUNCH_RATE_LIMIT_BUCKET_SIZE=10
export GLOBAL_JOB_LAUNCH_RATE_LIMIT_REPLENISHED_PER_MINUTE=5
LIBPROCESS_IP=172.17.0.1 COOK_DATOMIC="${COOK_DATOMIC_URI_1}" COOK_PORT=22321 COOK_SSL_PORT=22322 COOK_ZOOKEEPER_LOCAL=true COOK_ZOOKEEPER_LOCAL_PORT=4291 COOK_FRAMEWORK_ID=cook-framework-2 COOK_LOGFILE="log/cook-22321.log" lein run ${PROJECT_DIR}/travis/${CONFIG_FILE} &

# Wait for the cooks to be listening
timeout 180s bash -c "wait_for_cook 12321" || curl_error=true
if [ "$curl_error" = true ]; then
  echo "$(date +%H:%M:%S) Timed out waiting for cook to start listening"
  ${TRAVIS_BUILD_DIR}/travis/upload_logs.sh
  exit 1
fi

timeout 180s bash -c "wait_for_cook 22321" || curl_error=true
if [ "$curl_error" = true ]; then
  echo "$(date +%H:%M:%S) Timed out waiting for cook to start listening"
  ${TRAVIS_BUILD_DIR}/travis/upload_logs.sh
  exit 1
fi

# Ensure the Cook Scheduler CLI is available
command -v cs

# Run the integration tests
cd ${PROJECT_DIR}
export COOK_MESOS_LEADER_URL=${MINIMESOS_MASTER}
{
  echo "Using Mesos leader URL: ${COOK_MESOS_LEADER_URL}"
  export COOK_SCHEDULER_URL=http://localhost:12321
  pytest -n0 -v --color=no --timeout-method=thread --boxed -m multi_user tests/cook/test_multi_user.py -k test_rate_limit_launching_jobs || test_failures=true


  export COOK_SCHEDULER_URL=http://localhost:22321
  pytest -n0 -v --color=no --timeout-method=thread --boxed -m multi_user tests/cook/test_multi_user.py -k test_global_rate_limit_launching_jobs || test_failures=true
 } &> >(tee ./log/pytest.log)
 

# If there were failures, then we should save the logs
if [ "$test_failures" = true ]; then
  echo "Uploading logs..."
  ${TRAVIS_BUILD_DIR}/travis/upload_logs.sh
  exit 1
fi
