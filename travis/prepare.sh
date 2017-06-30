#!/bin/bash
set -ev

cd $PROJECT_DIR/../scheduler
lein deps
lein uberjar
VERSION=$(lein print :version | tr -d '"')

cd  $PROJECT_DIR/../travis
unzip datomic-free-0.9.5394.zip
cp "${PROJECT_DIR}/../scheduler/target/cook-${VERSION}.jar" datomic-free-0.9.5394/lib/
