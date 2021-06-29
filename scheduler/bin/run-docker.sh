#!/usr/bin/env bash

# Usage: ./bin/run-docker.sh [OPTIONS...]
# Runs the cook scheduler inside a docker container.
#   --auth={http-basic,one-user}    Use the specified authentication scheme. Default is one-user.
#   --executor={cook,mesos}         Use the specified job executor. Default is cook.
#   --zk={local,minimesos}          Use local (in-memory) or minimesos zookeper. Default is local.

set -e

# Defaults (overridable via environment)
: ${COOK_PORT:=12321}
: ${COOK_SSL_PORT:=12322}
: ${COOK_NREPL_PORT:=8888}
: ${COOK_FRAMEWORK_ID:=cook-framework-$(date +%s)}
: ${COOK_AUTH:=one-user}
: ${COOK_EXECUTOR:=cook}
: ${COOK_ZOOKEEPER_LOCAL:=true}

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
    --zk=local)
      COOK_ZOOKEEPER_LOCAL=true
      shift
      ;;
    --zk=minimesos)
      COOK_ZOOKEEPER_LOCAL=false
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
    export COOK_EXECUTOR_PORTION=0
    ;;
  one-user)
    export COOK_EXECUTOR_PORTION=1
    ;;
  *)
    echo "Unrecognized auth scheme: $COOK_AUTH"
    exit 1
esac

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
NAME=cook-scheduler-${COOK_PORT}.localhost

echo "About to: Clean up existing image"
if [ "$(docker ps -aq -f name=${NAME})" ]; then
    # Cleanup
    docker stop ${NAME}
fi

SCHEDULER_DIR="$( dirname ${DIR} )"
SCHEDULER_EXECUTOR_DIR=${SCHEDULER_DIR}/resources/public
EXECUTOR_NAME=cook-executor

case "$COOK_EXECUTOR" in
  cook)
    echo "$(date +%H:%M:%S) Cook executor has been enabled"
    COOK_EXECUTOR_COMMAND="./${EXECUTOR_NAME}/${EXECUTOR_NAME}"
    ;;
  mesos)
    COOK_EXECUTOR_COMMAND=""
    ;;
  *)
    echo "Unrecognized executor: $EXECUTOR"
    exit 1
esac

echo "About to: Setup and check docker networking"
if [ -z "$(docker network ls -q -f name=cook_nw)" ];
then
    # Using a separate network allows us to access hosts by name (cook-scheduler-12321)
    # instead of IP address which simplifies configuration
    echo "Creating cook_nw network"
    docker network create -d bridge --subnet 172.25.0.0/16 cook_nw
fi

if [ -z "${COOK_DATOMIC_URI}" ];
then
    COOK_DATOMIC_URI="datomic:free://localhost:4334/cook-jobs"
fi

if [ "${COOK_ZOOKEEPER_LOCAL}" = false ] ; then
    COOK_ZOOKEEPER="${MINIMESOS_ZOOKEEPER_IP}:2181"
else
    COOK_ZOOKEEPER=""
fi

echo "Starting cook..."

# NOTE: since the cook scheduler directory is mounted as a volume
# by the minimesos agents, they have access to the cook-executor binary
# using the absolute file path URI given for COOK_EXECUTOR below.
docker create \
    -i \
    -t \
    --rm \
    --name=${NAME} \
    --publish=${COOK_NREPL_PORT}:${COOK_NREPL_PORT} \
    --publish=${COOK_PORT}:${COOK_PORT} \
    --publish=${COOK_SSL_PORT}:${COOK_SSL_PORT} \
    --publish=4334:4334 \
    --publish=4335:4335 \
    --publish=4336:4336 \
    -e "COOK_EXECUTOR=file://${SCHEDULER_EXECUTOR_DIR}/${EXECUTOR_NAME}.tar.gz" \
    -e "COOK_EXECUTOR_COMMAND=${COOK_EXECUTOR_COMMAND}" \
    -e "COOK_PORT=${COOK_PORT}" \
    -e "COOK_SSL_PORT=${COOK_SSL_PORT}" \
    -e "COOK_KEYSTORE_PATH=${COOK_KEYSTORE_PATH}" \
    -e "COOK_NREPL_PORT=${COOK_NREPL_PORT}" \
    -e "COOK_FRAMEWORK_ID=${COOK_FRAMEWORK_ID}" \
    -e "MESOS_MASTER=${ZK}" \
    -e "COOK_ZOOKEEPER=${COOK_ZOOKEEPER}" \
    -e "COOK_ZOOKEEPER_LOCAL=${COOK_ZOOKEEPER_LOCAL}" \
    -e "COOK_HOSTNAME=${NAME}" \
    -e "COOK_DATOMIC_URI=${COOK_DATOMIC_URI}" \
    -e "COOK_LOG_FILE=log/cook-${COOK_PORT}.log" \
    -e "COOK_HTTP_BASIC_AUTH=${COOK_HTTP_BASIC_AUTH:-false}" \
    -e "COOK_ONE_USER_AUTH=root" \
    -e "COOK_EXECUTOR_PORTION=${COOK_EXECUTOR_PORTION:-0}" \
    -e "COOK_KEYSTORE_PATH=/opt/ssl/cook.p12" \
    -e "PGPASSWORD=${PGPASSWORD}" \
    -v ${DIR}/../log:/opt/cook/log \
    cook-scheduler:latest ${COOK_CONFIG:-}

echo "About to: Connect cook networking"
docker network connect bridge ${NAME}
docker network connect cook_nw ${NAME}

echo "About to: 'docker start ${NAME}'"
docker start ${NAME}

echo "Attaching to container..."
docker attach ${NAME}
