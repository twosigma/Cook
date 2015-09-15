#!/bin/sh
curl -u vagrant:password -H "content-type: application/json" -XPOST http://localhost:12321/rawscheduler -d '{"jobs": [{"max_retries": 3, "max_runtime": 86400000, "mem": 1000, "cpus": 1.5, "uuid": "26719da8-394f-44f9-9e6d-8a17500f5109", "command": "echo hello my friend"}]}'
