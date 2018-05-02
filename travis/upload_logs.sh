#!/usr/bin/env bash

cd ${TRAVIS_BUILD_DIR}

# Grab the Mesos master logs
mkdir -p ./mesos/master-logs
mesos_master_container=$(docker ps --filter "name=minimesos-master-" --format "{{.ID}}")
docker cp --follow-link $mesos_master_container:/var/log/mesos-master.INFO ./mesos/master-logs/
docker cp --follow-link $mesos_master_container:/var/log/mesos-master.WARNING ./mesos/master-logs/

tarball=./dump.txz
tar -cJf $tarball --transform="s|\./[^/]*/\.*|${TRAVIS_JOB_NUMBER}/|" ./scheduler/log ./travis/.minimesos ./mesos/master-logs
./travis/gdrive_upload "travis-${TRAVIS_JOB_NUMBER:-dump}" $tarball
