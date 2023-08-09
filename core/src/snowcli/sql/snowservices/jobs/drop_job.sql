{% include "set_env.sql" %}

call SYSTEM$CANCEL_JOB('{{ id }}');
