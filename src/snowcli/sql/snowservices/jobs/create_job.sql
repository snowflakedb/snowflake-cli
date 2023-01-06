use role {role};
use warehouse {warehouse};
use database {database};
use schema {schema};

CREATE STAGE IF NOT EXISTS SOURCE_STAGE;

put file://{spec_path} @source_stage/{stage_dir} auto_compress=false OVERWRITE = TRUE;

EXECUTE SERVICE {name}
  COMPUTE_POOL =  {compute_pool}
  spec=@source_stage/{stage_dir}/{stage_filename};
