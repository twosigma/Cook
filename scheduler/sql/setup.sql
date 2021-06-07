SET SCHEMA 'public';

DROP TABLE IF EXISTS resources;
create table resources (
   resource_name varchar(20) NOT NULL,
   resource_description text
);


drop table if exists pools;
create table pools (
  pool_name varchar(60) PRIMARY KEY,
  pool_active bool NOT NULL,
  pool_description text
);

drop table if exists resource_limits;
create table resource_limits (
  resource_limit_type varchar(8) NOT NULL CHECK (resource_limit_type IN ('quota', 'share')),
  pool_name varchar(60) NOT NULL,  -- references pool(pool_name) NOT NULL,
  user_name varchar(60) NOT NULL, -- 'default' is default user.
  resource_name varchar(10) NOT NULL,
  amount float NOT NULL,
  unique (resource_limit_type,pool_name,user_name,resource_name)
);

insert into pools VALUES ('k8s-alpha',true,'') ON CONFLICT UPDATE;
insert into pools VALUES ('k8s-beta',false,'') ON CONFLICT UPDATE;
insert into pools VALUES ('k8s-gamma',true,'') ON CONFLICT UPDATE;
insert into pools VALUES ('k8s-delta',false,'') ON CONFLICT UPDATE;

insert into resource_limits VALUES ('quota','k8s-alpha','default','mem',1000000);
insert into resource_limits VALUES ('quota','k8s-alpha','default','cpu',1000000);
commit;
-- Note about schema:
-- We want to put the command line into a seperate table. (Its huge, and poor locality)
-- Historic talbe in another schema.
-- Probably some jsonp columns for e.g., attributes.

