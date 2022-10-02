use role {role};
use warehouse {warehouse};
use database {database};
use schema {schema};
show user functions like '{like}';
select "name", "created_on", "arguments", "language" from table(result_scan(last_query_id()));
