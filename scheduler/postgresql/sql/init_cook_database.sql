-- Initialize a cook database from scratch --- creating the schemas and such.
-- Assumes we already have an appropriately configured postgresql database and
-- have psql connected to it.

BEGIN TRANSACTION;
-- Always run this in the transaction so that if the set schema fails for any reason, we abort instead of possibly writing to the wrong schema's tables.
CREATE SCHEMA :cook_schema;
SET SCHEMA :'cook_schema';

COMMIT

-- Just show the tables at the end.
\dt :'cook_schema'.
