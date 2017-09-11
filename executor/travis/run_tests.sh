#!/bin/bash

# Runs the nosetests for the executor

export PROJECT_DIR=`pwd`
cd ${PROJECT_DIR}

python --version
python setup.py nosetests --attr '!explicit'
