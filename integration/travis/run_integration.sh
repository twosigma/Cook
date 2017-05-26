#!/bin/bash
set -ev

export PROJECT_DIR=`pwd`

# Start minimesos
cd ${PROJECT_DIR}/../travis
./minimesos up

# Start two cook schedulers
cd ${PROJECT_DIR}/../scheduler
## on travis, ports on 172.17.0.1 are bindable from the host OS, and are also
## available for processes inside minimesos containers to connect to
LIBPROCESS_IP=172.17.0.1 COOK_PORT=12321 COOK_ZOOKEEPER_LOCAL_PORT=3291 lein run ${PROJECT_DIR}/travis/scheduler_config.edn &
LIBPROCESS_IP=172.17.0.1 COOK_PORT=22321 COOK_ZOOKEEPER_LOCAL_PORT=4291 lein run ${PROJECT_DIR}/travis/scheduler_config.edn &

# Install the CLI
cd ${PROJECT_DIR}/../cli
python3 setup.py install
cs --help

# Run the integration tests
set +e
cd ${PROJECT_DIR}
COOK_MULTI_CLUSTER= python3 setup.py nosetests --verbosity=3
TESTS_EXIT_CODE=$?

# If there were failures, dump the executor logs
if [ ${TESTS_EXIT_CODE} -ne 0 ]; then
  echo "Displaying executor logs"
  ${PROJECT_DIR}/../travis/show_executor_logs.sh
fi

exit ${TESTS_EXIT_CODE}