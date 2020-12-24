#!/bin/bash
set -ev

cd ${GITHUB_WORKSPACE}/travis

./build_cook_executor.sh
./datomic-free-0.9.5394/bin/transactor ${GITHUB_WORKSPACE}/scheduler/datomic/datomic_transactor.properties &
./minimesos up

cd ${GITHUB_WORKSPACE}/scheduler
# on travis, ports on 172.17.0.1 are bindable from the host OS, and are also
# available for processes inside minimesos containers to connect to
LIBPROCESS_IP=172.17.0.1 lein run ${PROJECT_DIR}/travis/scheduler_config.edn &
