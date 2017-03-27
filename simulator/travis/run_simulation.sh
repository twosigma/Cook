#!/bin/bash
set -ev

cd travis/
docker build -t mesos-agent:latest -f Dockerfile.agent .
./datomic-free-0.9.5394/bin/transactor $(pwd)/datomic_transactor.properties &
./minimesos up

cd ../../scheduler
# on travis, ports on 172.17.0.1 are bindable from the host OS, and are also
# available for processes inside minimesos containers to connect to
LIBPROCESS_IP=172.17.0.1 lein run ../simulator/travis/scheduler_config.edn &

# wait until we can download the executor binary so we can fail fast if it's unavailable
wget \
    --retry-connrefused \
    --waitretry=5 \
    --timeout=10 \
    --tries 20 \
    http://172.17.0.1:12321/resource/cook-executor

tail -f log/cook.log | grep -i "error\|exception" &

cd ../simulator

{
    while :; do
        echo "printing all the logs..."

        while read path; do
            tail "$path" || true
        done <<< "$(find travis/.minimesos -name 'stdout' -o -name 'stderr' -o -name 'executor.log')"

        sleep 5
    done
} &

lein run -c config/settings.edn setup-database -c travis/simulator_config.edn

set e
lein run -c config/settings.edn travis -c travis/simulator_config.edn
SIM_EXIT_CODE=$?

cat ../scheduler/log/cook.log | grep -C 5 "error\|exception"

echo "Printing out all executor logs..."
while read path; do
    cat "$path"
done <<< "$(find travis/.minimesos -name 'stdout' -o -name 'stderr' -o -name 'executor.log')"

exit $SIM_EXIT_CODE
