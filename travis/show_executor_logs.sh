#!/bin/bash
set -ev

echo "Printing out all executor logs..."
while read path; do
    echo "Contents of ${path}";
    cat "${path}";
    echo "------------------------------------"
done <<< "$(find $PROJECT_DIR/../travis/.minimesos -name 'stdout' -o -name 'stderr' -o -name 'executor.log')"

echo "Contents of scheduler/log/cook.log"
cat $PROJECT_DIR/../scheduler/log/cook.log
echo "------------------------------------"
