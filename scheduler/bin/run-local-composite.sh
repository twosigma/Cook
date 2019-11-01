#!/usr/bin/env bash

# Usage: ./bin/run-local-composite.sh
# Runs the cook scheduler locally.

set -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SCHEDULER_DIR="$( dirname ${DIR} )"

# Defaults (overridable via environment)
: ${COOK_DATOMIC_URI="datomic:mem://cook-jobs"}
: ${COOK_FRAMEWORK_ID:=cook-framework-$(date +%s)}
: ${COOK_KEYSTORE_PATH:="${SCHEDULER_DIR}/cook.p12"}
: ${COOK_NREPL_PORT:=${2:-8888}}
: ${COOK_PORT:=${1:-12321}}
: ${COOK_SSL_PORT:=${3:-12322}}
: ${MASTER_IP:="127.0.0.2"}
: ${ZOOKEEPER_IP:="127.0.0.1"}
: ${MESOS_NATIVE_JAVA_LIBRARY:="/usr/local/lib/libmesos.dylib"}

if [ -z "$( curl -Is --connect-timeout 2 --max-time 4 http://${MASTER_IP}:5050/state.json | head -1 )" ]; then
  echo "Mesos master (${MASTER_IP}) is unavailable"
  exit 1
fi

NAME=cook-scheduler-${COOK_PORT}

SCHEDULER_EXECUTOR_DIR=${SCHEDULER_DIR}/resources/public

EXECUTOR_DIR="$(dirname ${SCHEDULER_DIR})/executor"
EXECUTOR_NAME=cook-executor-local
COOK_EXECUTOR_COMMAND="${EXECUTOR_DIR}/dist/${EXECUTOR_NAME}/${EXECUTOR_NAME}"
SCHEDULER_EXECUTOR_DIR=${SCHEDULER_DIR}/resources/public

${EXECUTOR_DIR}/bin/prepare-executor.sh local ${SCHEDULER_EXECUTOR_DIR}

if [ "${COOK_ZOOKEEPER_LOCAL}" = false ] ; then
    COOK_ZOOKEEPER="${ZOOKEEPER_IP}:2181"
    echo "Cook ZooKeeper configured to ${COOK_ZOOKEEPER}"
else
    COOK_ZOOKEEPER=""
    COOK_ZOOKEEPER_LOCAL=true
    echo "Cook will use local ZooKeeper"
fi

if [ ! -f "${COOK_KEYSTORE_PATH}" ];
then
    keytool -genkeypair -keystore ${COOK_KEYSTORE_PATH} -storetype PKCS12 -storepass cookstore -dname "CN=cook, OU=Cook Developers, O=Two Sigma Investments, L=New York, ST=New York, C=US" -keyalg RSA -keysize 2048
fi

if ! [ -x "$(command -v flask)" ]; then
    echo "Please install flask"
    exit 1
fi

INTEGRATION_DIR="$(dirname ${SCHEDULER_DIR})/integration"
FLASK_APP=${INTEGRATION_DIR}/src/data_locality/service.py flask run -p 35847 &

echo "Mesos Master IP is ${MASTER_IP}"

echo "Creating environment variables..."
export COOK_DATOMIC_URI="${COOK_DATOMIC_URI}"
export COOK_FRAMEWORK_ID="${COOK_FRAMEWORK_ID}"
export COOK_EXECUTOR=""
export COOK_EXECUTOR_COMMAND="${COOK_EXECUTOR_COMMAND}"
export COOK_EXECUTOR_PORTION=1
export COOK_ONE_USER_AUTH=$(whoami)
export COOK_HOSTNAME="cook-scheduler-${COOK_PORT}"
export COOK_LOG_FILE="log/cook-${COOK_PORT}.log"
export COOK_NREPL_PORT="${COOK_NREPL_PORT}"
export COOK_PORT="${COOK_PORT}"
export COOK_ZOOKEEPER="${COOK_ZOOKEEPER}"
export COOK_ZOOKEEPER_LOCAL="${COOK_ZOOKEEPER_LOCAL}"
export LIBPROCESS_IP="${MASTER_IP}"
export MESOS_MASTER="${MASTER_IP}:5050"
export MESOS_NATIVE_JAVA_LIBRARY="${MESOS_NATIVE_JAVA_LIBRARY}"
export COOK_SSL_PORT="${COOK_SSL_PORT}"
export COOK_KEYSTORE_PATH="${COOK_KEYSTORE_PATH}"
export DATA_LOCAL_ENDPOINT="http://localhost:35847/retrieve-costs"

echo "Starting cook..."
lein run config-composite.edn
