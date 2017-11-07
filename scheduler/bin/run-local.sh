#!/usr/bin/env bash

# Usage: ./bin/run-local.sh
# Runs the cook scheduler locally.

# Defaults (overridable via environment)
: ${COOK_DATOMIC_URI="datomic:mem://cook-jobs"}
: ${COOK_FRAMEWORK_ID:=cook-framework-$(date +%s)}
: ${COOK_NREPL_PORT:=${2:-8888}}
: ${COOK_PORT:=${1:-12321}}
: ${MASTER_IP:="127.0.0.2"}
: ${ZOOKEEPER_IP:="127.0.0.1"}

if [ -z "$( curl -Is --connect-timeout 2 --max-time 4 http://${MASTER_IP}:5050/state.json | head -1 )" ]; then
  echo "Mesos master (${MASTER_IP}) is unavailable"
  exit 1
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
NAME=cook-scheduler-${COOK_PORT}

SCHEDULER_DIR="$( dirname ${DIR} )"
SCHEDULER_EXECUTOR_DIR=${SCHEDULER_DIR}/resources/public

EXECUTOR_DIR="$(dirname ${SCHEDULER_DIR})/executor"
SCHEDULER_EXECUTOR_DIR=${SCHEDULER_DIR}/resources/public
SCHEDULER_EXECUTOR_FILE=${EXECUTOR_DIR}/cook-executor-local

${EXECUTOR_DIR}/bin/prepare-executor.sh local ${SCHEDULER_EXECUTOR_DIR}

if [ "${COOK_ZOOKEEPER_LOCAL}" = false ] ; then
    COOK_ZOOKEEPER="${ZOOKEEPER_IP}:2181"
    echo "Cook ZooKeeper configured to ${COOK_ZOOKEEPER}"
else
    COOK_ZOOKEEPER=""
    COOK_ZOOKEEPER_LOCAL=true
    echo "Cook will use local ZooKeeper"
fi

echo "Mesos Master IP is ${MASTER_IP}"

echo "Creating environment variables..."
export COOK_DATOMIC_URI="${COOK_DATOMIC_URI}"
export COOK_FRAMEWORK_ID="${COOK_FRAMEWORK_ID}"
export COOK_EXECUTOR="file://${SCHEDULER_EXECUTOR_FILE}"
export COOK_EXECUTOR_COMMAND="./cook-executor-local"
export COOK_HOSTNAME="cook-scheduler-${COOK_PORT}"
export COOK_LOG_FILE="log/cook-${COOK_PORT}.log"
export COOK_NREPL_PORT="${COOK_NREPL_PORT}"
export COOK_PORT="${COOK_PORT}"
export COOK_ZOOKEEPER="${COOK_ZOOKEEPER}"
export COOK_ZOOKEEPER_LOCAL="${COOK_ZOOKEEPER_LOCAL}"
export LIBPROCESS_IP="${MASTER_IP}"
export MESOS_MASTER="${MASTER_IP}:5050"
export MESOS_MASTER_HOST="${MASTER_IP}"
export MESOS_NATIVE_JAVA_LIBRARY="/usr/local/lib/libmesos.dylib"

echo "Starting cook..."
lein run container-config.edn
