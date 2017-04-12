#!/bin/bash
set -ev

export PROJECT_DIR=`pwd`
$PROJECT_DIR/../travis/start_scheduler.sh

cd $PROJECT_DIR
lein run -c config/settings.edn setup-database -c travis/simulator_config.edn

set e
lein run -c config/settings.edn travis -c travis/simulator_config.edn
SIM_EXIT_CODE=$?

$PROJECT_DIR/../travis/show_executor_logs.sh

exit $SIM_EXIT_CODE
