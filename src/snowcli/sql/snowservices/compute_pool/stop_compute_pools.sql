use role {{ role }};
use warehouse {{ warehouse }};
use database {{ database }};
use schema {{ schema }};

alter compute pool {{ name }} stop all services;
