#!/usr/bin/env bash

set -e

cd ${TRAVIS_BUILD_DIR}

# Grab the Mesos master logs
mkdir -p ./mesos/master-logs
docker ps --all --last 10
mesos_master_container=$(docker ps --all --latest --filter "name=minimesos-master-" --format "{{.ID}}")
docker cp --follow-link $mesos_master_container:/var/log/mesos-master.INFO ./mesos/master-logs/
docker cp --follow-link $mesos_master_container:/var/log/mesos-master.WARNING ./mesos/master-logs/

tarball=./dump.txz
tar -cJf $tarball --transform="s|\./[^/]*/\.*|${TRAVIS_JOB_NUMBER}/|" --warning=no-file-changed ./scheduler/log ./travis/.minimesos ./mesos/master-logs || exitcode=$?
if [ "$exitcode" != "" ] && [ "$exitcode" != "1" ] && [ "$exitcode" != "0" ]; then
  echo "The tar command exited with exit code $exitcode, exiting..."
  exit $exitcode
fi
./travis/gdrive_upload "travis-${TRAVIS_JOB_NUMBER:-dump}" $tarball
