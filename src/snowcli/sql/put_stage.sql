use role {{ role }};
use warehouse {{ warehouse }};
use database {{ database }};
use schema {{ schema }};

{{ create_stage_command }}

put file://{{ path }} {{ name }}{{ destination_path }} auto_compress=false parallel={{ parallel }} overwrite={{ overwrite }};
