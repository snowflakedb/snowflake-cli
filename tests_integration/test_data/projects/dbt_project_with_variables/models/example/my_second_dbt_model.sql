select *, '{{ var("env") }}' as env_tmpl, '{{ var("user") }}' as user_tmpl
from {{ ref('my_first_dbt_model') }}
where id = 1
