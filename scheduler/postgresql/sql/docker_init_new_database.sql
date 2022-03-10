-- Create a cook scheduler database from a branch new database, including
-- creating the initial user. Intended to be run for docker setup in opensource only.

-- When in docker-land, we use the database username cook_scheduler
DROP DATABASE IF EXISTS cook_local;
ALTER DEFAULT PRIVILEGES REVOKE ALL ON TABLES FROM cook_scheduler;
ALTER DEFAULT PRIVILEGES REVOKE ALL ON SCHEMAS FROM cook_scheduler;
DROP ROLE IF EXISTS cook_scheduler;
CREATE ROLE cook_scheduler with password :'cook_user_password' LOGIN;
CREATE DATABASE cook_local WITH owner cook_scheduler;

-- Ensure that all schemas on this database are writeable by cook_scheduler user.
ALTER DEFAULT PRIVILEGES GRANT ALL ON SCHEMAS TO cook_scheduler;
ALTER DEFAULT PRIVILEGES GRANT ALL ON TABLES TO cook_scheduler;
