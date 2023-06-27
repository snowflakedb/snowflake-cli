{% include "set_env.sql" %}
show user procedures like '{{ like }}';
select "name", "created_on", "arguments" from table(result_scan(last_query_id()));
