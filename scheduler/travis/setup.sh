#!/usr/bin/env bash

set -e

# Install the current version of the jobclient
pushd ${TRAVIS_BUILD_DIR}/jobclient
mvn install
popd

# Setup the executor build environment
apt-get install libzookeeper-mt-dev

# Install lein dependencies
lein with-profiles +test deps

