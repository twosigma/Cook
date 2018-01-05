#!/bin/bash

set -ev

export PROJECT_DIR=`pwd`
../travis/prepare.sh
pip install -r requirements.txt
