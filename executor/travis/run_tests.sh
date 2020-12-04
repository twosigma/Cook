#!/bin/bash

# Runs the Cook Executor tests

export PROJECT_DIR=`pwd`
cd ${PROJECT_DIR}

python --version
python -m pytest --version

python -m pytest pytest -n4
