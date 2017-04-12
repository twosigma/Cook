#!/bin/bash
set -ev

cat $PROJECT_DIR/../scheduler/log/cook.log | grep -C 5 "error\|exception"

echo "Printing out all executor logs..."
while read path; do
    cat "$path"
done <<< "$(find $PROJECT_DIR/../travis/.minimesos -name 'stdout' -o -name 'stderr' -o -name 'executor.log')"
