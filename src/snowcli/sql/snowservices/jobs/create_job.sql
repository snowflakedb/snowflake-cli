use role {{ role }};
use warehouse {{ warehouse }};
use database {{ database }};
use schema {{ schema }};

CREATE STAGE IF NOT EXISTS {{ stage }};

put file://{{ spec_path }} @{{ stage }}/{{ stage_dir }} auto_compress=false OVERWRITE = TRUE;

EXECUTE SERVICE
  COMPUTE_POOL =  {{ compute_pool }}
  spec=@{{ stage }}/{{ stage_dir }}/{{ stage_filename }};
