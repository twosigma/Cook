#!/bin/bash

# Sets up the travis worker to be able to run executor tests.

export PROJECT_DIR=`pwd`
cd ${PROJECT_DIR}

python --version
pip install -r requirements.txt
pip install --user -e '.[test]'
