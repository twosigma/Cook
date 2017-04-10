#!/bin/bash
set -ev

cd $PROJECT_DIR/../scheduler
lein deps
lein uberjar

cd  $PROJECT_DIR/../travis
unzip datomic-free-0.9.5394.zip
cp $PROJECT_DIR/../scheduler/target/cook-1.0.1-SNAPSHOT.jar datomic-free-0.9.5394/lib/
