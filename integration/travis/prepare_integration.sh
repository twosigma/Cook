#!/bin/bash

set -ev

PROJECT_DIR=`pwd` ../travis/prepare.sh
python --version

# Explicitly uninstall cli
if [[ $(pip list --format=columns | grep cook-client) ]];
then
    pip uninstall -y cook-client
fi

pip install --user -r requirements.txt
