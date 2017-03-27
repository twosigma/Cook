#!/bin/bash
set -ev

lein deps

cd ../executor
pip3 install pyinstaller
pip3 install -r requirements.txt
pyinstaller -F -n cook-executor -p cook cook/__main__.py
mkdir -p ../scheduler/resources/public
cp dist/cook-executor ../scheduler/resources/public/

cd ../scheduler
lein deps
lein uberjar

cd ../simulator/travis
unzip datomic-free-0.9.5394.zip
cp ../../scheduler/target/cook-1.0.1-SNAPSHOT.jar datomic-free-0.9.5394/lib/

docker pull python:3
