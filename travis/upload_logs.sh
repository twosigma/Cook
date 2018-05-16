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
# GNU tar always exits with 0, 1 or 2 (https://www.gnu.org/software/tar/manual/html_section/tar_19.html)
# 0 = Successful termination
# 1 = Some files differ (we're OK with this)
# 2 = Fatal error
if [ "$exitcode" == "2" ]; then
  echo "The tar command exited with exit code $exitcode, exiting..."
  exit $exitcode
fi
./travis/gdrive_upload "travis-${TRAVIS_JOB_NUMBER:-dump}" $tarball
