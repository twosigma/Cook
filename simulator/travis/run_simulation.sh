#!/bin/bash
set -ev

cd travis/
./datomic-free-0.9.5394/bin/transactor $(pwd)/datomic_transactor.properties &
./minimesos up

cd ../../scheduler
# on travis, ports on 172.17.0.1 are bindable from the host OS, and are also
# available for processes inside minimesos containers to connect to
LIBPROCESS_IP=172.17.0.1 lein run ../simulator/travis/scheduler_config.edn &

cd ../simulator
lein run -c config/settings.edn setup-database -c travis/simulator_config.edn
lein run -c config/settings.edn travis -c travis/simulator_config.edn
