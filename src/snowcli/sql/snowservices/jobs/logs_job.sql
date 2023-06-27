{% include "set_env.sql" %}

call SYSTEM$GET_JOB_LOGS('{{ id }}', '{{ container_name }}');
