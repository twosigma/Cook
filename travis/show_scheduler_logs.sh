#!/usr/bin/env bash

for log in ${GITHUB_WORKSPACE}/scheduler/log/cook*.log;
do
    echo "Contents of ${log}"
    cat "${log}";
    echo "------------------------------------"
done
