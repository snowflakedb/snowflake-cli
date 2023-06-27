{% include "set_env.sql" %}

call SYSTEM$GET_SERVICE_LOGS('{{ name }}', '{{ instance_id }}', '{{ container_name }}');
