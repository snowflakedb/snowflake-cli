use role {{ role }};
use database {{ database }};
use schema {{ schema }};
use warehouse {{ warehouse }};

create or replace stage {{ name }}_stage;

create streamlit {{ name }}
  FROM @{{ database }}.{{ schema }}.{{ name }}_stage
  MAIN_FILE = '/{{ file_name }}'
  QUERY_WAREHOUSE = {{ warehouse }};

show streamlits;
describe streamlit {{ name }};

ALTER STREAMLIT {{ name }} CHECKOUT;
