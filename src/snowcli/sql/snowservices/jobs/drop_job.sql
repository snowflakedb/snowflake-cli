use role {{ role }};
use warehouse {{ warehouse }};
use database {{ database }};
use schema {{ schema }};

call SYSTEM$CANCEL_JOB('{{ id }}');
