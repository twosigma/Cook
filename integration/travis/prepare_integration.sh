#!/bin/bash

set -ev

PROJECT_DIR=`pwd` ../travis/prepare.sh
python --version
pip install --user -r requirements.txt
