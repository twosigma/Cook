#!/bin/bash

set -ev

cd ${GITHUB_WORKSPACE}/executor
pip install --user -r requirements.txt
./bin/prepare-executor.sh local ${GITHUB_WORKSPACE}/scheduler/resources/public
tar -C ${GITHUB_WORKSPACE}/travis -xzf ./dist/cook-executor-local.tar.gz
