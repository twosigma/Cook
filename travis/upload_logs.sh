#!/usr/bin/env bash

set -e

cd ${TRAVIS_BUILD_DIR}

# List the last 10 containers
docker ps --all --last 10

# Grab the Mesos master logs
mkdir -p ./mesos/master-logs
mesos_master_container=$(docker ps --all --latest --filter "name=minimesos-master-" --format "{{.ID}}")
docker cp --follow-link $mesos_master_container:/var/log/mesos-master.INFO ./mesos/master-logs/
docker cp --follow-link $mesos_master_container:/var/log/mesos-master.WARNING ./mesos/master-logs/

# Grab the Mesos agent logs
mesos_agent_containers=$(docker ps --all --last 6 --filter "name=minimesos-agent-" --format "{{.ID}}")
for container in $mesos_agent_containers;
do
  destination=./mesos/agent-logs/$container
  mkdir -p $destination
  docker cp --follow-link $container:/var/log/mesos-slave.INFO $destination
  docker cp --follow-link $container:/var/log/mesos-slave.WARNING $destination
  docker cp --follow-link $container:/var/log/mesos-slave.ERROR $destination
  docker cp --follow-link $container:/var/log/mesos-fetcher.INFO $destination || echo "Container $container does not have mesos-fetcher.INFO"
done

tarball=./dump.txz
tar -cJf $tarball --transform="s|\./[^/]*/\.*|${TRAVIS_JOB_NUMBER}/|" --warning=no-file-changed ./scheduler/log ./travis/.minimesos ./mesos/master-logs ./mesos/agent-logs || exitcode=$?
# GNU tar always exits with 0, 1 or 2 (https://www.gnu.org/software/tar/manual/html_section/tar_19.html)
# 0 = Successful termination
# 1 = Some files differ (we're OK with this)
# 2 = Fatal error
if [ "$exitcode" == "2" ]; then
  echo "The tar command exited with exit code $exitcode, exiting..."
  exit $exitcode
fi
./travis/gdrive_upload "travis-${TRAVIS_JOB_NUMBER:-dump}" $tarball
