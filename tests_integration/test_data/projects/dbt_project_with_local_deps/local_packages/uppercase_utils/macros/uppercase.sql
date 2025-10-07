{% macro uppercase(column_name) %}
    upper({{ column_name }})
{% endmacro %}
