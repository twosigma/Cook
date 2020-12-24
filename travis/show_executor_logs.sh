#!/bin/bash
set -v

echo "Printing out all executor logs..."
while read path; do
    echo "Contents of ${path}";
    cat "${path}";
    echo "------------------------------------"
done <<< "$(find ${GITHUB_WORKSPACE}/travis/.minimesos -name 'stdout' -o -name 'stderr' -o -name 'executor.log')"

${GITHUB_WORKSPACE}/travis/show_scheduler_logs.sh
