--liquibase formatted sql

-- Initialize a cook database from scratch --- creating the schemas and such.
-- Assumes we already have an appropriately configured postgresql database and
-- have psql connected to it.

-- If you get a crazy error where 'No schema has been selected to
-- create in' when running the first CREATE TABLE. It can be caused by there being a capital
-- letter in cook_schema. Schema names are lowercased when created, but case-sensitive when
-- in the search path.