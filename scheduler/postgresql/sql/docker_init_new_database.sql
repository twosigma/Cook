-- Create a cook scheduler database from a branch new database, including
-- creating the initial user. Intended to be run for docker setup in opensource only.

-- When in docker-land, we use the database username cook_scheduler
DROP ROLE IF EXISTS cook_scheduler;
CREATE ROLE cook_scheduler with password :'cook_user_password' LOGIN;
CREATE DATABASE cook_local WITH owner cook_scheduler;
