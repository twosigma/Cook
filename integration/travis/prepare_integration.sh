#!/bin/bash

set -ev

PROJECT_DIR=`pwd`
cd ${PROJECT_DIR}/../scheduler
lein deps
