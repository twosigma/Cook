#!/bin/bash
set -ev

export PROJECT_DIR=`pwd`

# Start minimesos
cd ${PROJECT_DIR}/../travis
./minimesos up

# Start cook scheduler
cd ${PROJECT_DIR}/../scheduler
## on travis, ports on 172.17.0.1 are bindable from the host OS, and are also
## available for processes inside minimesos containers to connect to
LIBPROCESS_IP=172.17.0.1 lein run ${PROJECT_DIR}/travis/scheduler_config.edn &

# Run the integration tests
set +e
cd ${PROJECT_DIR}
python setup.py nosetests
TESTS_EXIT_CODE=$?

# If there were failures, dump the executor logs
if [ ${TESTS_EXIT_CODE} -ne 0 ]; then
  echo "Displaying executor logs"
  ${PROJECT_DIR}/../travis/show_executor_logs.sh
fi

exit ${TESTS_EXIT_CODE}