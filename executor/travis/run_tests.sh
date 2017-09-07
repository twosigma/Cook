#!/bin/bash

# Runs the nosetests for the executor

export PROJECT_DIR=`pwd`
cd ${PROJECT_DIR}

source py35/bin/activate py35
python setup.py nosetests --attr '!explicit'
deactivate
