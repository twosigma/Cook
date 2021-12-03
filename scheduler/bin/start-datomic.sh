#!/bin/bash

set -euf -o pipefail

PROJECT_DIR="$(dirname $0)/.."
DATOMIC_VERSION="0.9.5561.56"
DATOMIC_DIR="${PROJECT_DIR}/datomic/datomic-free-${DATOMIC_VERSION}"

if [ ! -d "${DATOMIC_DIR}" ];
then
    unzip "${PROJECT_DIR}/datomic/datomic-free-${DATOMIC_VERSION}.zip" -d "${PROJECT_DIR}/datomic"
fi

COOK_VERSION=$(lein print :version | tr -d '"')

if [ ! -f "${DATOMIC_DIR}/lib/cook-${COOK_VERSION}.jar" ];
then
    lein uberjar
    # `lein print :version` would not have worked if nothing was built, so need to
    # get version again after building
    COOK_VERSION=$(lein print :version | tr -d '"')
    cp "${PROJECT_DIR}/target/cook-${COOK_VERSION}.jar" "${DATOMIC_DIR}/lib/"
fi

"${DATOMIC_DIR}/bin/transactor" $(realpath "${PROJECT_DIR}/datomic/datomic_transactor.properties")


