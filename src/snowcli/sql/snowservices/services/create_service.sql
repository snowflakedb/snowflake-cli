use role {{ role }};
use warehouse {{ warehouse }};
use database {{ database }};
use schema {{ schema }};

CREATE STAGE IF NOT EXISTS {{ stage }};

put file://{{ spec_path }} @{{ stage }}/{{ stage_dir }} auto_compress=false OVERWRITE = TRUE;

CREATE SERVICE IF NOT EXISTS {{ name }}
  MIN_INSTANCES = {{ num_instances }}
  MAX_INSTANCES = {{ num_instances }}
  COMPUTE_POOL =  {{ compute_pool }}
  spec=@{{ stage }}/{{ stage_dir }}/{{ stage_filename }};
