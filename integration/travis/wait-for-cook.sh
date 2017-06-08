#!/usr/bin/env bash
# Usage: wait-for-cook.sh [PORT]
#
# Examples:
#   timeout 60s wait-for-cook.sh 12321
#   timeout 60s wait-for-cook.sh
#
# Waits for cook to be listening on the provided port (defaults to 12321).

COOK_PORT=${1:-12321}

while ! curl -s localhost:${COOK_PORT} >/dev/null;
do
    echo "$(date +%H:%M:%S) Cook is not listening on ${COOK_PORT} yet"
    sleep 2.0
done
echo "$(date +%H:%M:%S) Connected to Cook on ${COOK_PORT}!"
