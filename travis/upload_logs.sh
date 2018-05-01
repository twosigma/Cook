#!/usr/bin/env bash

cd ${TRAVIS_BUILD_DIR}

tarball=./dump.txz
tar -cJf $tarball --transform='s|\./[^/]*/\.*||' ./scheduler/log ./travis/.minimesos
./travis/gdrive_upload "travis-${TRAVIS_JOB_NUMBER:-dump}" $tarball
