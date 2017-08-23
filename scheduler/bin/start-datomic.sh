#!/bin/bash

set -euf -o pipefail

PROJECT_DIR="$(dirname $0)/.."

if [ "$(docker inspect datomic-free | jq -r ' .[] | .State.Status')" != "running" ];
then
    echo "Starting datomic"
    if [ "$(docker ps -aq -f name=datomic-free)" ];
    then
        docker rm datomic-free
    fi

    docker create -p 4334-4336:4334-4336 --network=cook_nw -e ALT_HOST=datomic-free --name datomic-free akiel/datomic-free:0.9.5206

    echo "Building cook"
    cd $PROJECT_DIR
    lein jar
    COOK_VERSION=$(lein print :version | tr -d '"')
    docker cp target/cook-${COOK_VERSION}.jar datomic-free:/datomic-free-0.9.5206/lib

    echo "Starting datomic"
    docker start datomic-free
fi
