use role {role};
use warehouse {warehouse};
use database {database};
use schema {schema};

CREATE STAGE IF NOT EXISTS SOURCE_STAGE;

put file://{spec_path} @source_stage auto_compress=false OVERWRITE = TRUE;

CREATE SERVICE IF NOT EXISTS {name}
  MIN_INSTANCES = 1
  MAX_INSTANCES = 1
  COMPUTE_POOL =  {compute_pool}
  spec=@source_stage/{spec_filename};
