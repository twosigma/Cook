#!/bin/bash

set -euf -o pipefail

PROJECT_DIR="$(dirname $0)/.."

if [ "$(docker inspect datomic-free | jq -r ' .[] | .State.Status')" != "running" ];
then
    echo "Starting datomic"
    docker create --rm -p 4334-4336:4334-4336 --network=cook_nw -e ALT_HOST=datomic-free --name datomic-free akiel/datomic-free:0.9.5206

    # Datomic needs the metatransaction code in it's classpath, so we need to build cook and copy the jar into the container
    echo "Building cook"
    cd $PROJECT_DIR
    lein jar
    COOK_VERSION=$(lein print :version | tr -d '"')
    docker cp target/cook-${COOK_VERSION}.jar datomic-free:/datomic-free-0.9.5206/lib

    echo "Starting datomic"
    docker start datomic-free
fi
