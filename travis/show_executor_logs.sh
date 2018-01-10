#!/bin/bash
set -v

echo "Printing out all executor logs..."
while read path; do
    echo "Contents of ${path}";
    cat "${path}";
    echo "------------------------------------"
done <<< "$(find ${TRAVIS_BUILD_DIR}/travis/.minimesos -name 'stdout' -o -name 'stderr' -o -name 'executor.log')"

${TRAVIS_BUILD_DIR}/travis/show_scheduler_logs.sh
