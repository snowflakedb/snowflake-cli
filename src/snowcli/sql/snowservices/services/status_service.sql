use role {{ role }};
use warehouse {{ warehouse }};
use database {{ database }};
use schema {{ schema }};

CALL SYSTEM$GET_SERVICE_STATUS('{{ name }}');
