#!/usr/bin/env bash

/opt/cook/datomic-free-0.9.5394/bin/transactor /opt/cook/datomic/datomic_transactor.properties &
lein with-profiles +docker run $1
