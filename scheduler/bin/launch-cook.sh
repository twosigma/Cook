#!/usr/bin/env bash

export COOK_IP="$(hostname -i)"
export COOK_EXECUTOR="http://$COOK_IP:$COOK_PORT/resource/cook-executor"

exec lein run $@
