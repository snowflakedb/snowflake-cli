use role {role};
use warehouse {warehouse};
use database {database};
use schema {schema};

put file:///Users/aivanou/code/snowcli-internal/files/spec.yaml @test_stage auto_compress=false OVERWRITE = TRUE;

CREATE SERVICE IF NOT EXISTS {name}
  MIN_INSTANCES = 3
  MAX_INSTANCES = 3
  COMPUTE_POOL =  {compute_pool}
  spec=@test_stage/spec.yaml;
