/* TODO's:'
We should isolate seperate unit test runs and/or integration test runs in different schemas.
Per stackoverflow, we can set this in the connection string:
     jdbc:postgresql://localhost:5432/mydatabase?currentSchema=myschema
Or when makign the datasource:
dataSource.setCurrentSchema ( "your_schema_name_here_" );  // <----------


*/

SET SCHEMA 'public';

-- DROP TABLE IF EXISTS resources;
create table resources (
   resource_name varchar(20) NOT NULL,
   resource_description text
);


-- drop table if exists pools;
create table pools (
  pool_name varchar(60) PRIMARY KEY,
  pool_active bool NOT NULL,
  pool_description text
);

-- drop table if exists resource_limits;
create table resource_limits (
  resource_limit_type varchar(8) NOT NULL CHECK (resource_limit_type IN ('quota', 'share')),
  pool_name varchar(60) NOT NULL,  -- references pool(pool_name) NOT NULL,
  user_name varchar(60) NOT NULL, -- 'default' is default user.
  resource_name varchar(30) NOT NULL,
  amount float NOT NULL,
  reason text,
  PRIMARY KEY (resource_limit_type,pool_name,user_name,resource_name)
);

insert into pools VALUES ('k8s-alpha',true,'');
insert into pools VALUES ('k8s-beta',false,'');
insert into pools VALUES ('k8s-gamma',true,'');
insert into pools VALUES ('k8s-delta',false,'');

insert into resource_limits VALUES ('quota','k8s-alpha','default','mem',1000000, NULL);
insert into resource_limits VALUES ('quota','k8s-alpha','default','cpus',1000000, NULL);
insert into resource_limits VALUES ('quota','k8s-beta','default','mem',1000000, NULL);
insert into resource_limits VALUES ('quota','k8s-beta','default','cpus',1000000, NULL);
commit;
-- Note about schema:
-- Historic talbe in another schema.
-- Probably some jsonp columns for e.g., attributes.

select * from resource_limits;

-- MOre notes about schema design. Tuples can have at most 1600 columns. However, max size is about 8000 bytes (with headers). Columns are 1-18 bytes (18 being used for char/varchar/text). So, realistic column limit is 400 columns.

--We CANNOT use table partitioning by time. We require a unique UUID index over all of the data to guarantee that UUIDs never repeat. We either need to partition based on UUID (which is weird, not very useful) or not use it.

--I expect we will implement a historic data archive. Move old jobs and instances to that archive.

--Postgresql has TOAST, we can store excessively sized values out-of-line (e.g. command lines). See TOAST_TUPLE_TARGET so we don't need a sepearate command line table!'

-- For uniformity, we use jsonp to store environment, constraints, and labels. (We could use hstore, but I think this is better). We're not selecting on these fields.

--- I'd like to put historic data into a seperate table for OLAP queries. *IF* we do that, then it is very convenient if we try to denormalize the data to that an instance, job are one row. It also makes queries a lot easier. THen its easy to move it (and all of its components). Note that we don't have to do this. given a set of job rowid's to move, we can identify all of the corresponding rows easily. E.g., a seperate job_rowid, label_key,label_val is pretty easy to setup. What we don't want to do is try to coalesce identical values, its always a 1:N, not a M:N relationship.





*/



/* Notes:

This sql library is eager, not lazy and loads all data. We should be selective and not load everything.

