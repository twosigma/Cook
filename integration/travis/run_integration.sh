#!/bin/bash

# Usage: ./run_integration [OPTIONS...]
#   --auth={http-basic,one-user}    Use the specified authentication scheme. Default is one-user.
#   --executor={cook,mesos}         Use the specified job executor. Default is mesos.
#   --pools={on,off}                Use or don't use pools in the Cook under test. Default is on.

set -ev

export PROJECT_DIR=`pwd`

COOK_AUTH=one-user
COOK_EXECUTOR=mesos
COOK_POOLS=on
CONFIG_FILE=scheduler_travis_config.edn
COOK_TEST_DOCKER_IMAGE=""

while (( $# > 0 )); do
  case "$1" in
    --auth=*)
      COOK_AUTH="${1#--auth=}"
      shift
      ;;
    --executor=*)
      COOK_EXECUTOR="${1#--executor=}"
      shift
      ;;
    --pools=*)
      COOK_POOLS="${1#--pools=}"
      shift
      ;;
    --image=*)
      COOK_TEST_DOCKER_IMAGE="${1#--image=}"
      shift
      ;;
    *)
      echo "Unrecognized option: $1"
      exit 1
  esac
done

case "$COOK_AUTH" in
  http-basic)
    export COOK_HTTP_BASIC_AUTH=true
    ;;
  one-user)
    export COOK_EXECUTOR_PORTION=1
    # One user auth is configured to use root
    export COOK_DOCKER_UID=0
    export COOK_DOCKER_GID=0
    ;;
  *)
    echo "Unrecognized auth scheme: $COOK_AUTH"
    exit 1
esac

case "$COOK_EXECUTOR" in
  cook)
    echo "$(date +%H:%M:%S) Cook executor has been enabled"
    COOK_EXECUTOR_COMMAND="${GITHUB_WORKSPACE}/travis/cook-executor-local/cook-executor-local"
    # Build cook-executor
    ${GITHUB_WORKSPACE}/travis/build_cook_executor.sh
    # Run with docker
    ;;
  mesos)
    COOK_EXECUTOR_COMMAND=""
    ;;
  *)
    echo "Unrecognized executor: $EXECUTOR"
    exit 1
esac

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
cd ${GITHUB_WORKSPACE}/travis
./minimesos up
$(./minimesos info | grep MINIMESOS)
export COOK_ZOOKEEPER="${MINIMESOS_ZOOKEEPER_IP}:2181"
export MINIMESOS_ZOOKEEPER=${MINIMESOS_ZOOKEEPER%;}
export MINIMESOS_MASTER=${MINIMESOS_MASTER%;}

SCHEDULER_DIR=${GITHUB_WORKSPACE}/scheduler
./datomic-free-0.9.5394/bin/transactor ${SCHEDULER_DIR}/datomic/datomic_transactor.properties &
COOK_DATOMIC_URI_1=datomic:free://localhost:4334/cook-jobs
COOK_DATOMIC_URI_2=datomic:mem://cook-jobs

# Generate SSL certificate
COOK_KEYSTORE_PATH=${SCHEDULER_DIR}/cook.p12
keytool -genkeypair -keystore ${COOK_KEYSTORE_PATH} -storetype PKCS12 -storepass cookstore -dname "CN=cook, OU=Cook Developers, O=Two Sigma Investments, L=New York, ST=New York, C=US" -keyalg RSA -keysize 2048
export COOK_KEYSTORE_PATH=${COOK_KEYSTORE_PATH}

case "$COOK_POOLS" in
  on)
    echo "Pools are turned on for this run"
    DEFAULT_POOL=mesos-gamma
    cd ${SCHEDULER_DIR}
    lein exec -p datomic/data/seed_pools.clj ${COOK_DATOMIC_URI_1}
    ;;
  off)
    echo "Pools are turned off for this run"
    ;;
  *)
    echo "Unrecognized pools toggle: $COOK_POOLS"
    exit 1
esac

pip install flask
export DATA_LOCAL_PORT=35847
export DATA_LOCAL_SERVICE="http://localhost:${DATA_LOCAL_PORT}"
export DATA_LOCAL_ENDPOINT="${DATA_LOCAL_SERVICE}/retrieve-costs"
mkdir ${SCHEDULER_DIR}/log
FLASK_APP=${PROJECT_DIR}/src/data_locality/service.py flask run --port=${DATA_LOCAL_PORT} > ${SCHEDULER_DIR}/log/data-local.log 2>&1 &

# Seed running jobs, which are used to test the task reconciler
cd ${SCHEDULER_DIR}
COOK_FRAMEWORK_ID=cook-framework-1 lein exec -p datomic/data/seed_running_jobs.clj ${COOK_DATOMIC_URI_1}

# Start three cook schedulers.
# We want one cluster with two cooks to run MasterSlaveTest, and a second cluster to run MultiClusterTest.
# The basic tests will run against cook-framework-1
## on travis, ports on 172.17.0.1 are bindable from the host OS, and are also
## available for processes inside minimesos containers to connect to
export COOK_EXECUTOR_COMMAND=${COOK_EXECUTOR_COMMAND}
if [[ ! -z "${COOK_TEST_DOCKER_IMAGE}" ]]; then
    export COOK_TEST_DOCKER_IMAGE=${COOK_TEST_DOCKER_IMAGE}
fi

# Start one cook listening on port 12321, this will be the master of the "cook-framework-1" framework
LIBPROCESS_IP=172.17.0.1 COOK_DATOMIC="${COOK_DATOMIC_URI_1}" COOK_PORT=12321 COOK_SSL_PORT=12322 COOK_FRAMEWORK_ID=cook-framework-1 COOK_LOGFILE="log/cook-12321.log" COOK_DEFAULT_POOL=${DEFAULT_POOL} lein run ${PROJECT_DIR}/travis/${CONFIG_FILE} &
# Start a second cook listening on port 22321, this will be the master of the "cook-framework-2" framework
LIBPROCESS_IP=172.17.0.1 COOK_DATOMIC="${COOK_DATOMIC_URI_2}" COOK_PORT=22321 COOK_SSL_PORT=22322 COOK_ZOOKEEPER_LOCAL=true COOK_ZOOKEEPER_LOCAL_PORT=4291 COOK_FRAMEWORK_ID=cook-framework-2 COOK_LOGFILE="log/cook-22321.log" lein run ${PROJECT_DIR}/travis/${CONFIG_FILE} &

# Wait for the cooks to be listening
timeout 180s bash -c "wait_for_cook 12321" || curl_error=true
if [ "$curl_error" = true ]; then
  echo "$(date +%H:%M:%S) Timed out waiting for cook to start listening"
  ${GITHUB_WORKSPACE}/travis/upload_logs.sh
  exit 1
fi

# Start a third cook listening on port 12323, this will be a slave on the "cook-framework-1" framework
LIBPROCESS_IP=172.17.0.1 COOK_DATOMIC="${COOK_DATOMIC_URI_1}" COOK_PORT=12323 COOK_SSL_PORT=12324 COOK_FRAMEWORK_ID=cook-framework-1 COOK_LOGFILE="log/cook-12323.log" COOK_DEFAULT_POOL=${DEFAULT_POOL} lein run ${PROJECT_DIR}/travis/${CONFIG_FILE} &

timeout 180s bash -c "wait_for_cook 12323" || curl_error=true
if [ "$curl_error" = true ]; then
  echo "$(date +%H:%M:%S) Timed out waiting for cook to start listening"
  ${GITHUB_WORKSPACE}/travis/upload_logs.sh
  exit 1
fi
timeout 180s bash -c "wait_for_cook 22321" || curl_error=true
if [ "$curl_error" = true ]; then
  echo "$(date +%H:%M:%S) Timed out waiting for cook to start listening"
  ${GITHUB_WORKSPACE}/travis/upload_logs.sh
  exit 1
fi

# Ensure the Cook Scheduler CLI is available
command -v cs

# Run the integration tests
cd ${PROJECT_DIR}
export COOK_MULTI_CLUSTER=
export COOK_MASTER_SLAVE=
export COOK_SLAVE_URL=http://localhost:12323
export COOK_MESOS_LEADER_URL=${MINIMESOS_MASTER}
{
    echo "Using Mesos leader URL: ${COOK_MESOS_LEADER_URL}"
    pytest -n4 -v --color=no --timeout-method=thread --boxed -m "not serial and not travis_skip" || test_failures=true
    pytest -n0 -v --color=no --timeout-method=thread --boxed -m "serial and not travis_skip" || test_failures=true
} &> >(tee ./log/pytest.log)
 

# If there were failures, then we should save the logs
if [ "$test_failures" = true ]; then
  echo "Uploading logs..."
  ${GITHUB_WORKSPACE}/travis/upload_logs.sh
  exit 1
fi
