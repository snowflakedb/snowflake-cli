{% include "set_env.sql" %}
alter PROCEDURE {{ signature }} SET COMMENT = $${{ comment }}$$;
