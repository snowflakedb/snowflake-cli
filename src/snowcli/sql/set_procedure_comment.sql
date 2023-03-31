use role {{ role }};
use warehouse {{ warehouse }};
use database {{ database }};
use schema {{ schema }};
alter PROCEDURE {{ signature }} SET COMMENT = $${{ comment }}$$;
