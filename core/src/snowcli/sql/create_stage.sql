{% include "set_env.sql" %}

create stage if not exists {{ name }};
