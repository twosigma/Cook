-- Insert some rows for development and running tests in open source, including initial quotas and pools
-- for the integration tests.

--- DO NOT RUN IN PRODUCTION.
begin transaction;
-- Always run this in the transaction so that if the set schema fails for any reason, we abort instead of possibly writing to the wrong schema's tables.
SET SCHEMA :'cook_schema';


insert into pools VALUES ('k8s-alpha',true,'');
insert into pools VALUES ('k8s-beta',false,'');
insert into pools VALUES ('k8s-gamma',true,'');
insert into pools VALUES ('k8s-delta',false,'');

insert into resource_limits VALUES ('quota','k8s-alpha','default','mem',1000000, '');
insert into resource_limits VALUES ('quota','k8s-alpha','default','cpus',1000000, '');
insert into resource_limits VALUES ('quota','k8s-beta','default','mem',1000000, '');
insert into resource_limits VALUES ('quota','k8s-beta','default','cpus',1000000, '');
end transaction;
