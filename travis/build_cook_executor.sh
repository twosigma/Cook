#!/bin/bash

set -ev

cd ${GITHUB_WORKSPACE}/executor
/opt/hostedtoolcache/Python/3.6.12/x64/bin/pip install --user -r requirements.txt
./bin/prepare-executor.sh local ${GITHUB_WORKSPACE}/scheduler/resources/public
tar -C ${GITHUB_WORKSPACE}/travis -xzf ./dist/cook-executor-local.tar.gz
