-- WIPE THE DATABASE!
-- Assumes we already have an appropriately configured postgresql database and
-- have psql connected to it.

-- Drops the schema of a cook database so it can be recreated.
DROP SCHEMA IF EXISTS :cook_schema CASCADE;
