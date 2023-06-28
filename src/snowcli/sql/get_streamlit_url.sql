{% include "set_env.sql" %}

CALL SYSTEM$GENERATE_STREAMLIT_URL_FROM_NAME('{{ name }}');
