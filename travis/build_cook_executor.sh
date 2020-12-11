#!/bin/bash

set -ev

cd ${TRAVIS_BUILD_DIR}/executor
pip install --user -r requirements.txt
./bin/prepare-executor.sh local ${TRAVIS_BUILD_DIR}/scheduler/resources/public
tar -C ${TRAVIS_BUILD_DIR}/travis -xzf ./dist/cook-executor-local.tar.gz
