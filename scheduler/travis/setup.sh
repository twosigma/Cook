#!/usr/bin/env bash

set -e

# Install the current version of the jobclient
pushd ${GITHUB_WORKSPACE}/jobclient/java
mvn install
popd

# Install lein dependencies
lein with-profiles +test deps

