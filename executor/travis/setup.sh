#!/bin/bash

# Sets up the travis worker to be able to run executor tests.

export PROJECT_DIR=`pwd`
cd ${PROJECT_DIR}

pip install virtualenv
virtualenv -p python3 py35
source py35/bin/activate py35
python setup.py install
deactivate
