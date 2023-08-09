{% include "set_env.sql" %}

alter compute pool {{ name }} stop all services;
