#!/bin/bash
set -ev

lein deps

cd ../scheduler
lein deps
lein uberjar

cd ../simulator/travis
unzip datomic-free-0.9.5394.zip
cp ../../scheduler/target/cook-1.0.1-SNAPSHOT.jar datomic-free-0.9.5394/lib/
