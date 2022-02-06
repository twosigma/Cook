--liquibase formatted sql

-- Initialize a cook database from scratch --- creating the schemas and such.
-- Assumes we already have an appropriately configured postgresql database and
-- have psql connected to it.

-- If you get a crazy error where 'No schema has been selected to
-- create in' when running the first CREATE TABLE. It can be caused by there being a capital
-- letter in cook_schema. Schema names are lowercased when created, but case-sensitive when
-- in the search path.

--- Setup the tables.
--changeset scrosby:2021-12-22-resources
create table resources (
   resource_name varchar(30) PRIMARY KEY,
   resource_description text
);

--changeset scrosby:2021-12-22-pools
create table pools (
  pool_name varchar(60) PRIMARY KEY,
  pool_active bool NOT NULL,
  pool_description text NOT NULL
);

--changeset scrosby:2021-12-22-resource_limits
create table resource_limits (
  resource_limit_type varchar(8) NOT NULL CHECK (resource_limit_type IN ('quota', 'share')),
  pool_name varchar(60) NOT NULL,  -- references pool(pool_name) NOT NULL,
  user_name varchar(60) NOT NULL, -- 'default' is default user.
  resource_name varchar(30) NOT NULL,
  amount float NOT NULL,
  reason text NOT NULL,
-- TODO:  deletion_timestamp, -- NOT NULL means deleted.
  PRIMARY KEY (resource_limit_type, pool_name, user_name, resource_name)
);

-- Just show the tables at the end.
--ignoreLines:1
\dt :cook_schema.*