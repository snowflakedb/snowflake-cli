{% include "set_env.sql" %}

CREATE STAGE IF NOT EXISTS {{ stage }};

put file://{{ spec_path }} @{{ stage }}/{{ stage_dir }} auto_compress=false OVERWRITE = TRUE;

EXECUTE SERVICE
  COMPUTE_POOL =  {{ compute_pool }}
  spec=@{{ stage }}/{{ stage_dir }}/{{ stage_filename }};
