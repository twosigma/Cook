#!/bin/bash
set -ev

cd ${GITHUB_WORKSPACE}/scheduler
lein deps
lein uberjar
VERSION=$(lein print :version | tr -d '"')

cd  ${GITHUB_WORKSPACE}/travis
unzip ${GITHUB_WORKSPACE}/scheduler/datomic/datomic-free-0.9.5394.zip
cp "${GITHUB_WORKSPACE}/scheduler/target/cook-${VERSION}.jar" datomic-free-0.9.5394/lib/
