#!/bin/bash
set -ev

cd $PROJECT_DIR/../executor
bin/build-docker.sh
cp -rfv $PROJECT_DIR/../executor/dist/cook-executor $PROJECT_DIR/../travis/
mkdir -p $PROJECT_DIR/../scheduler/resources/public/
cp -rfv $PROJECT_DIR/../executor/dist/cook-executor $PROJECT_DIR/../scheduler/resources/public/
