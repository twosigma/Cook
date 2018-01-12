#!/usr/bin/env bash

set -e

export PROJECT_DIR=`pwd`

# Install lein dependencies
lein with-profiles +test deps

# Install the current version of the jobclient
cd ${TRAVIS_BUILD_DIR}/jobclient
lein do clean, compile, install
cd ${PROJECT_DIR}
