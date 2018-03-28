#!/usr/bin/env bash

DATOMIC_PROPERTIES_FILE=/opt/cook/datomic/datomic_transactor.properties

echo "alt-host=$(hostname -i | cut -d' ' -f2)" >> ${DATOMIC_PROPERTIES_FILE}
/opt/cook/datomic-free-0.9.5394/bin/transactor ${DATOMIC_PROPERTIES_FILE} &
lein with-profiles +docker run $1
