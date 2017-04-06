#!/bin/sh
uuid=$(uuidgen)
curl -u vagrant:password -H "content-type: application/json" -XPOST http://localhost:12321/rawscheduler -d '{"jobs": [{"max_retries": 3, "max_runtime": 86400000, "mem": 1000, "cpus": 1.5, "uuid": "'${uuid}'", "command": "echo hello my friend", "name": "test", "priority": 0}]}'
printf "\n"
