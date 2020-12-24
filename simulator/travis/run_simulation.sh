#!/bin/bash
set -ev

export PROJECT_DIR=`pwd`
${GITHUB_WORKSPACE}/travis/start_scheduler.sh

cd ${PROJECT_DIR}
lein run -c config/settings.edn setup-database -c travis/simulator_config.edn

set +e
lein run -c config/settings.edn travis -c travis/simulator_config.edn
SIM_EXIT_CODE=$?

if [ ${SIM_EXIT_CODE} -ne 0 ]; then
  echo "Displaying executor logs"
  ${GITHUB_WORKSPACE}/travis/show_executor_logs.sh
fi

exit ${SIM_EXIT_CODE}
