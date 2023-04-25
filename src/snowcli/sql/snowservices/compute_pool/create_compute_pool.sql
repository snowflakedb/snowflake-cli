use role {role};
use warehouse {warehouse};
use database {database};
use schema {schema};

CREATE COMPUTE POOL {name}
  MIN_NODES = {min_node}
  MAX_NODES = {max_node}
  INSTANCE_FAMILY = {instance_family};
