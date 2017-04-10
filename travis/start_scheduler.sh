#!/bin/bash
set -ev

cd $PROJECT_DIR/../travis
./datomic-free-0.9.5394/bin/transactor $(pwd)/datomic_transactor.properties &
./minimesos up

cd $PROJECT_DIR/../scheduler
# on travis, ports on 172.17.0.1 are bindable from the host OS, and are also
# available for processes inside minimesos containers to connect to
LIBPROCESS_IP=172.17.0.1 lein run $PROJECT_DIR/travis/scheduler_config.edn &
