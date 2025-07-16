{% macro get_current_timestamp() -%}
    {{ return(adapter.dispatch('get_current_timestamp')()) }}
{%- endmacro %}

{% macro default__get_current_timestamp() -%}
    current_timestamp()
{%- endmacro %}

{% macro snowflake__get_current_timestamp() -%}
    current_timestamp()
{%- endmacro %}

{% macro add_audit_columns() -%}
    , {{ get_current_timestamp() }} as created_at
    , {{ get_current_timestamp() }} as updated_at
{%- endmacro %} 
