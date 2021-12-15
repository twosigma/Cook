# For cook to function, it must have a postgresql server available. There are two approaches
# 1. Run postgresql in docker (this script)
# 2. Run a database outside of docker, for, e.g., development.

# For #2, it's pretty easy. The main thing you need to do is create the database and a user, set COOK_DB_TEST_PG_DB, PGPASSWORD and COOK_DB_TEST_PG_USER, (or put a :pg-config section in config.edn, for integration tests), and then you can set it up using
#   psql -h localhost -U $COOK_DB_TEST_PG_USER -d $COOK_DB_TEST_PG_DB -f sql/reset_init_cook_database.sql

##
## To use a docker-launched database:
##

# NOTE: this will destroy and create a new database from scratch
# Choose a password. This will be threaded to various downstream code.
# export PGPASSWORD='somepassword'
# Note that you need to remember PGPASSWORD and reuse it later when attempting to connect to the
# database directly. The scripts here know how to consume that environmental variable.
# You should have that in your environment when you do bin/run-docker.sh to startup Cook.

# Make the docker container with that password and default the username ('cook_scheduler') and
# database name ('cook_local')':
bin/make-launch-postgres-docker.sh

# To connect as the database superuser:
docker run -it --rm  -e PGPASSWORD=${PGPASSWORD} --network cook_nw postgres:13 psql -h cook-postgres -U postgres

# To connect as the same user as cook will:
docker run -it --rm  -e PGPASSWORD=${PGPASSWORD} --network cook_nw postgres:13 psql -h cook-postgres -U cook_scheduler -d cook_local

# To connect directly from outside the container:
# (this works because of PGPASSWORD, set above)
psql -h localhost -U cook_scheduler -d cook_local





###
### If you want to manage the database container yourself e.g., for unit tests or out-of-line:
###

docker kill cook-postgres
docker container rm cook-postgres

# Launch it with this password.
docker run --name cook-postgres -e POSTGRES_PASSWORD=fsadms8x7dsnmd7 -d postgres:13
# Connect it with Cook's network:
docker network connect cook_nw cook-postgres

## Next you need to setup the database there for Cook:

# As postgresql superuser. user is the user/role account that you do cook dev as (in my case, I do opensource dev as user=twosigma

user=twosigma
createuser $user
createdb -O $user cook_dev

# Now as that role user, can get interactive access:
psql -d cook_dev
alter user with password = "Some Passowrd"

# Then either set the appropriate environmental variables so that Cook can find the database:
COOK_DB_TEST_PG_DB
COOK_DB_TEST_PG_SERVER
COOK_DB_TEST_PG_PASSWD

Note that Cook, by default, uses COOK_DB_TEST_PG_DB=cook_local and COOK_DB_TEST_PG_SERVER is 127.0.0.1

## Then can connect to the postgres interactively with
docker run -it --rm  -e PGPASSWORD=${PGPASSWORD} --network cook_nw postgres:13 psql -h cook-postgres -U postgres

## To run unit tests (assumes PGPASSWORD is already set). Note you need to set COOK_DB_TEST_AUTOCREATE_SCHEMA to the startup path.
(export COOK_DB_TEST_PG_USER=cook_scheduler COOK_DB_TEST_PG_SERVER=127.0.0.1 COOK_DB_TEST_AUTOCREATE_SCHEMA=/home/twosigma/source/Cook/scheduler/postgresql/bin/setup-new-schema.sh ; time lein test)


## Alternately, if you want to use the pre-existing cook_local persistent schema name, you can do;
(export COOK_DB_TEST_PG_SCHEMA=cook_local COOK_DB_TEST_PG_USER=cook_scheduler COOK_DB_TEST_PG_SERVER=127.0.0.1 ; time lein test)

## As a third alternate, if you want to use a new persistent schema name:
bin/setup-new-schema.sh cook_persistent_dbtest
export COOK_DB_TEST_PG_SCHEMA=cook_persistent_dbtest COOK_DB_TEST_PG_USER=cook_scheduler COOK_DB_TEST_PG_SERVER=127.0.0.1 ; lein test


# To launch integration tests, you need to pass PGPASSWORD to the bin/run-docker so that 

# The other variables here are needed because the scripts that seed datomic pools also currently seed the quota.
export COOK_DB_TEST_PG_SCHEMA=cook_local ; gc lein compile && bin/build-docker-image.sh && export COOK_CONFIG='config-k8s.edn' && bin/run-docker.sh
