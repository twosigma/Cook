#!/bin/bash
set -ev

cd ${TRAVIS_BUILD_DIR}/scheduler
lein deps
lein uberjar
VERSION=$(lein print :version | tr -d '"')

cd  ${TRAVIS_BUILD_DIR}/travis
unzip ${TRAVIS_BUILD_DIR}/scheduler/datomic/datomic-free-0.9.5394.zip
cp "${TRAVIS_BUILD_DIR}/scheduler/target/cook-${VERSION}.jar" datomic-free-0.9.5394/lib/
