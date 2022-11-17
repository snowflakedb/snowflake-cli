use role {role};
use warehouse {warehouse};
use database {database};
use schema {schema};
show user procedures like '{like}';
select "name", "created_on", "arguments" from table(result_scan(last_query_id()));
