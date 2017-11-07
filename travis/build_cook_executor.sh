#!/bin/bash
set -ev

cd $PROJECT_DIR/../executor
bin/build-docker.sh
cp -fv $PROJECT_DIR/../executor/dist/cook-executor-docker $PROJECT_DIR/../travis/
mkdir -p $PROJECT_DIR/../scheduler/resources/public/
cp -fv $PROJECT_DIR/../executor/dist/cook-executor-docker $PROJECT_DIR/../scheduler/resources/public/
