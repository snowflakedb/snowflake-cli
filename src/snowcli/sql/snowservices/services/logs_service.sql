use role {role};
use warehouse {warehouse};
use database {database};
use schema {schema};

call SYSTEM$GET_SNOWSERVICE_LOGS('{name}', '{instance_id}', '{container_name}');
