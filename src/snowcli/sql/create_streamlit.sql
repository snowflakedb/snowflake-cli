use role {{ role }};
use database {{ database }};
use schema {{ schema }};
use warehouse {{ warehouse }};

create streamlit {{ name }}
  MAIN_FILE = '{{ file_name }}'
  QUERY_WAREHOUSE = {{ warehouse }};

show streamlits;
describe streamlit {{ name }};

alter streamlit {{ name }} checkout;
