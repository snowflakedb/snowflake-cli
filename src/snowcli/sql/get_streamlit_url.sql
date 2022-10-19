use database {database};
use schema {schema};
use role {role};

CALL SYSTEM$GENERATE_STREAMLIT_URL_FROM_NAME('{name}');
