use role {{ role }};
use warehouse {{ warehouse }};
use database {{ database }};
use schema {{ schema }};

call SYSTEM$GET_SERVICE_LOGS('{{ name }}', '{{ instance_id }}', '{{ container_name }}');
