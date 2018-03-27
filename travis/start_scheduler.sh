#!/bin/bash
set -ev

cd ${TRAVIS_BUILD_DIR}/travis

./build_cook_executor.sh
./datomic-free-0.9.5394/bin/transactor ${TRAVIS_BUILD_DIR}/scheduler/datomic/datomic_transactor.properties &
./minimesos up

cd ${TRAVIS_BUILD_DIR}/scheduler
# on travis, ports on 172.17.0.1 are bindable from the host OS, and are also
# available for processes inside minimesos containers to connect to
LIBPROCESS_IP=172.17.0.1 lein run ${PROJECT_DIR}/travis/scheduler_config.edn &
