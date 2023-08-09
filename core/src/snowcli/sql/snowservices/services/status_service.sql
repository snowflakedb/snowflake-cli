{% include "set_env.sql" %}

CALL SYSTEM$GET_SERVICE_STATUS('{{ name }}');
