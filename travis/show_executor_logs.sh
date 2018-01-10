#!/bin/bash
set -v

echo "what logs are there"
find $PROJECT_DIR/../scheduler/log -name 'cook*.log'
echo "Printing out all executor logs..."
while read path; do
    echo "Contents of ${path}";
    cat "${path}";
    echo "------------------------------------"
done <<< "$(find $PROJECT_DIR/../travis/.minimesos -name 'stdout' -o -name 'stderr' -o -name 'executor.log')"

for log in $PROJECT_DIR/../scheduler/log/cook*.log;
do
    echo "Contents of ${log}"
    cat "${log}";
    echo "------------------------------------"
done
