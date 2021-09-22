#!/bin/bash

set -euf -o pipefail

PROJECT_DIR="$(dirname $0)/.."
DATOMIC_VERSION="0.9.5394"
DATOMIC_DIR="${PROJECT_DIR}/datomic/datomic-free-${DATOMIC_VERSION}"

if [ ! -d "${DATOMIC_DIR}" ];
then
    unzip "${PROJECT_DIR}/datomic/datomic-free-${DATOMIC_VERSION}.zip" -d "${PROJECT_DIR}/datomic"
fi

COOK_VERSION=$(lein print :version | tr -d '"')

if [ ! -f "${DATOMIC_DIR}/lib/cook-${COOK_VERSION}.jar" ];
then
    lein uberjar
    cp "${PROJECT_DIR}/target/cook-${COOK_VERSION}.jar" "${DATOMIC_DIR}/lib/"
fi

"${DATOMIC_DIR}/bin/transactor" $(realpath "${PROJECT_DIR}/datomic/datomic_transactor.properties")


