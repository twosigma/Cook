-- Initialize a cook database from scratch --- creating the schemas and such.
-- Assumes we already have an appropriately configured postgresql database and
-- have psql connected to it.

-- TODO: LOOK AT LIQUIDBASE FOR SETTING UP SQL
BEGIN TRANSACTION;
-- Always run this in the transaction so that if the set schema fails for any reason, we abort instead of possibly writing to the wrong schema's tables.
CREATE SCHEMA :cook_schema;
SET SCHEMA :'cook_schema';

-- If you get a crazy error where the two above lines pass, but 'No schema has been selected to
-- create in' when running the first CREATE TABLE. It can be caused by there being a capital
-- letter in cook_schema. Schema names are lowercased when created, but case-sensitive when
-- in the search path.

COMMIT

-- Just show the tables at the end.
\dt :cook_schema.*
