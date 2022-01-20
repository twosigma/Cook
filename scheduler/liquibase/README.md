# Background

To manage Cook's SQL schema, we use liquibase. In the case of a unit test environment,
or in other cases where we need to create a new database entirely from scratch, `psql`
is also used to create a temporary database into which liquibase installs the schema.
This is also used in the docker-postgresql implementation.

# Automatic setup

The operations to create a new postgresql database server, create the cook database
inside it, and initialize the schema are done for you by the
`postgresql/bin/make-launch-postgres-docker.sh` script.

# Manual setup

If you choose to forgo the fully automatic setup above, you can setup the
liquibase environment manually.

## Getting the liquibase docker image

In opensource, download the liquibase docker image to manage the database:

    docker pull liquibase/liquibase:4.6

## Telling liquibase where everything is

You need to teach liquibase where the database and database schema is.

    export COOK_DB_TEST_PG_SCHEMA=cook_local
    export COOK_DB_TEST_PG_DATABASE=cook_local
    export COOK_DB_TEST_PG_USER=cook_scheduler

We also need to give liquibase the database host name so that it can reach it. Here, I
set it to the docker postgres container name so that we can use the DNS name that
docker has created for us.

    export COOK_DB_TEST_PG_SERVER=cook-postgres

The final piece is we assume `PGPASSWORD` is already set to the postgres password.

Liquibase needs a JDBC URL to connect to the database:

    PG_JDBC_URL="jdbc:postgresql://${COOK_DB_TEST_PG_SERVER}/${COOK_DB_TEST_PG_DATABASE}?user=${COOK_DB_TEST_PG_USER}&password=${PGPASSWORD}&currentSchema=${COOK_DB_TEST_PG_SCHEMA}"

## Using liquibase

Because we run liquibase inside of a container and run postgres inside of a container, we need to connect the networks: `--network cook_nw`

Liquibase in docker has some surprises. It is hardcoded to assume that all of its
changelog files are in the directory `/liquibase/changelog/` within the container.
To make this work, when running with docker, we use volume mount from the Cook
git checkout, `-v $(pwd)/liquibase/changelog:/liquibase/changelog`
to mount the liquibase directory from the checkout into the docker container.

A final note, in some cases, liquibase requires absolute paths. In other cases, it
refuses to accept them. 

###  Interactively using liquibase on a new database schema

In Cook, we support muliplexing multiple Cook databases within a single postgresql database. The different Cook databases are distinguished by the schema name.

First, set some variables. The most important of which is the schema name and the (derived) JDBC URL which liquibase needs. The other vars are assumed to be set from above.

    export COOK_DB_TEST_PG_SCHEMA=cook_liquibase_2
    PG_JDBC_URL="jdbc:postgresql://${COOK_DB_TEST_PG_SERVER}/${COOK_DB_TEST_PG_DATABASE}?user=${COOK_DB_TEST_PG_USER}&password=${PGPASSWORD}&currentSchema=${COOK_DB_TEST_PG_SCHEMA}"

Second, create the schema with psql.

    (cd postgresql ; time psql -h localhost -U postgres -d cook_local --set=cook_schema="$COOK_DB_TEST_PG_SCHEMA" -f sql/init_cook_database.sql)

Third, verify liquibase can connect. We will get the status. (Note that `--changeLogFile` must be a relative path. It is relative to `/liquitbase` within the container, as volume mounted from the git checkout.)

    docker run --network cook_nw --rm -v `pwd`/liquibase/changelog:/liquibase/changelog liquibase/liquibase:4.6 --changeLogFile=./changelog/com/twosigma/cook/changelogs/setup.postgresql.sql --url ${PG_JDBC_URL} status --verbose

Fourth, we may use liquibase to update the schema:

    docker run --network cook_nw --rm -v `pwd`/liquibase/changelog:/liquibase/changelog liquibase/liquibase:4.6 --changeLogFile=./changelog/com/twosigma/cook/changelogs/setup.postgresql.sql --url ${PG_JDBC_URL} --liquibase-schema-name=${COOK_DB_TEST_PG_SCHEMA} update


## Using liquibase: Appendix of command line snippets.

These are command line snippets used i exploring liquibase, documented for later use.

### Dumping the current schema.

This exists for experimentation and play and was used in testing. Because this is mutating the changelog, we need to give `--changeLogFile` an absolute path with two path components:

    mkdir -p liquibase/changelog/com/twosigma/cook/changelogs/
    docker run --network cook_nw --rm -v `pwd`/liquibase/changelog:/liquibase/changelog liquibase/liquibase:4.6 --changeLogFile=/liquibase/changelog/com/twosigma/cook/changelogs/root.changelog.xml generateChangeLog --url ${PG_JDBC_URL} --log-level=debug


### To get an interactive shell in the liquibase container for diagnosing things:

    docker run -i --entrypoint '/bin/bash'  --network cook_nw -v `pwd`/liquibase/changelog:/liquibase/changelog liquibase/liquibase:4.6 -i
/liquibase/docker-entrypoint.sh --changeLogFile=./changelog/com/twosigma/cook/changelogs/setup.postgresql.sql status --url "jdbc:postgresql://cook-postgres/cook_local?user=cook_scheduler&password=fooght&currentSchema=cook_liquibase_1"
