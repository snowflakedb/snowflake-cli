use role {{ role }};
use warehouse {{ warehouse }};
use database {{ database }};
use schema {{ schema }};

call SYSTEM$SHOW_SERVICE_EXECUTION_HISTORY();
