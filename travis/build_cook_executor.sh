#!/bin/bash
set -ev

cd $PROJECT_DIR/../executor
bin/build-cook-executor.sh
cp -fv $PROJECT_DIR/../executor/dist/cook-executor $PROJECT_DIR/../travis/
mkdir -p $PROJECT_DIR/../scheduler/resources/public/
cp -fv $PROJECT_DIR/../executor/dist/cook-executor $PROJECT_DIR/../scheduler/resources/public/
