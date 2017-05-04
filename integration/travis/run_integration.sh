#!/bin/bash
set -ev

export PROJECT_DIR=`pwd`
$PROJECT_DIR/../travis/start_scheduler.sh

set +e
cd $PROJECT_DIR
python setup.py nosetests
TESTS_EXIT_CODE=$?

if [ ${TESTS_EXIT_CODE} -ne 0 ]; then
  echo "Displaying executor logs"
  $PROJECT_DIR/../travis/show_executor_logs.sh
fi

exit ${TESTS_EXIT_CODE}