-- WIPES THE DATABASE
-- Reinitialize a cook database, wiping all of the contents first.
-- Assumes we already have an appropriately configured postgresql database and
-- have psql connected to it.

\ir reset_cook_database.sql
\ir init_cook_database.sql
