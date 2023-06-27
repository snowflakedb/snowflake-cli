{% include "set_env.sql" %}

CALL SYSTEM$GET_JOB_STATUS('{{ id }}');
