#!/bin/bash

UUID=$(uuidgen)

curl -XPOST -H"Content-Type: application/json" http://localhost:12321/rawscheduler -d"{\"jobs\": [{\"uuid\": \"$UUID\", \"env\": {\"EXECUTOR_TEST_EXIT\": \"1\"}, \"executor\": \"cook\", \"mem\": 128, \"cpus\": 1, \"command\": \"echo progress: 50 test_progress && exit 0\", \"max_retries\": 1, \"container\": {\"type\": \"DOCKER\", \"docker\": {\"image\": \"python:3.5.9-stretch\", \"network\": \"HOST\", \"force-pull-image\": false}, \"volumes\": [{\"container-path\": \"/Users/paul/src/Cook/executor/dist\", \"host-path\": \"/Users/paul/src/Cook/executor/dist\"}]}}]}"
