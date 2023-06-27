{% include "set_env.sql" %}

grant usage on streamlit {{ name }} to role {{ to_role }};
