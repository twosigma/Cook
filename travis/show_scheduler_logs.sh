#!/usr/bin/env bash

for log in ${TRAVIS_BUILD_DIR}/scheduler/log/cook*.log;
do
    echo "Contents of ${log}"
    cat "${log}";
    echo "------------------------------------"
done
