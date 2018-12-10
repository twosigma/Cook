#!/bin/bash

# Usage: ./run_integration [OPTIONS...]

set -ev

export PROJECT_DIR=`pwd`

CONFIG_FILE=scheduler_travis_config.edn
JOB_LAUNCH_RATE_LIMIT=off
GLOBAL_JOB_LAUNCH_RATE_LIMIT=off

while (( $# > 0 )); do
  case "$1" in
    --job-launch-rate-limit=*)
      JOB_LAUNCH_RATE_LIMIT="${1#--job-launch-rate-limit=}"
      shift
      ;;
    --global-job-launch-rate-limit=*)
      GLOBAL_JOB_LAUNCH_RATE_LIMIT="${1#--global-job-launch-rate-limit=}"
      shift
      ;;
    *)
      echo "Unrecognized option: $1"
      exit 1
  esac
done

export COOK_HTTP_BASIC_AUTH=true
COOK_EXECUTOR_COMMAND=""

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

cd ${SCHEDULER_DIR}

# Start two cook schedulers.
# The basic tests will run against cook-framework-1
## on travis, ports on 172.17.0.1 are bindable from the host OS, and are also
## available for processes inside minimesos containers to connect to
export COOK_EXECUTOR_COMMAND=${COOK_EXECUTOR_COMMAND}
# Start one cook listening on port 12321, this will be the master of the "cook-framework-1" framework
LIBPROCESS_IP=172.17.0.1 COOK_DATOMIC="${COOK_DATOMIC_URI_1}" COOK_PORT=12321 COOK_SSL_PORT=12322 COOK_COOKEEPER_LOCAL=true COOK_COOKEEPER_LOCAL_PORT=5291 COOK_FRAMEWORK_ID=cook-framework-1 COOK_LOGFILE="log/cook-12321.log" COOK_DEFAULT_POOL=${DEFAULT_POOL} lein run ${PROJECT_DIR}/travis/${CONFIG_FILE} &
# Start a second cook listening on port 22321, this will be the master of the "cook-framework-2" framework
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

export JOB_LAUNCH_RATE_LIMIT_BUCKET_SIZE=10000
export JOB_LAUNCH_RATE_LIMIT_REPLENISHED_PER_MINUTE=10000
export GLOBAL_JOB_LAUNCH_RATE_LIMIT_BUCKET_SIZE=10000
export GLOBAL_JOB_LAUNCH_RATE_LIMIT_REPLENISHED_PER_MINUTE=10000


{
    echo "Using Mesos leader URL: ${COOK_MESOS_LEADER_URL}"
    if [ "$JOB_LAUNCH_RATE_LIMIT" = on ]; then
      export COOK_SCHEDULER_URL=http://localhost:12321
      export JOB_LAUNCH_RATE_LIMIT_BUCKET_SIZE=10
      export JOB_LAUNCH_RATE_LIMIT_REPLENISHED_PER_MINUTE=5
      pytest -n0 -v --color=no --timeout-method=thread --boxed -m multi_user tests/cook/test_multi_user.py -k test_rate_limit_launching_jobs || test_failures=true
    else if [ "$GLOBAL_JOB_LAUNCH_RATE_LIMIT" = on ]; then
      export COOK_SCHEDULER_URL=http://localhost:22321
      export GLOBAL_JOB_LAUNCH_RATE_LIMIT_BUCKET_SIZE=10
      export GLOBAL_JOB_LAUNCH_RATE_LIMIT_REPLENISHED_PER_MINUTE=5
      pytest -n0 -v --color=no --timeout-method=thread --boxed -m multi_user tests/cook/test_multi_user.py -k test_global_rate_limit_launching_jobs || test_failures=true
    fi
    fi
} &> >(tee ./log/pytest.log)
 

# If there were failures, then we should save the logs
if [ "$test_failures" = true ]; then
  echo "Uploading logs..."
  ${TRAVIS_BUILD_DIR}/travis/upload_logs.sh
  exit 1
fi
