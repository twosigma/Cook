#!/usr/bin/env bash

DATOMIC_PROPERTIES_FILE=/opt/cook/datomic/datomic_transactor.properties

echo "alt-host=$(hostname -i | cut -d' ' -f2)" >> ${DATOMIC_PROPERTIES_FILE}
/opt/cook/datomic-free-0.9.5394/bin/transactor ${DATOMIC_PROPERTIES_FILE} &
echo "Seeding test data..."
lein exec -p /opt/cook/datomic/data/seed_pools.clj ${COOK_DATOMIC_URI}
lein exec -p /opt/cook/datomic/data/seed_k8s_pools.clj ${COOK_DATOMIC_URI}
lein exec -p /opt/cook/datomic/data/seed_running_jobs.clj ${COOK_DATOMIC_URI}
lein with-profiles +docker run $1
