use role {{ role }};
use warehouse {{ warehouse }};
use database {{ database }};
use schema {{ schema }};


call SYSTEM$GET_JOB_LOGS('{{ id }}', '{{ container_name }}');
