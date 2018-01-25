#!/bin/bash

# Runs the Cook Executor tests

export PROJECT_DIR=`pwd`
cd ${PROJECT_DIR}

python --version
pytest --version

pytest -n4
