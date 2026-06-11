{{ config(materialized='table') }}

select
  '{{ env_var("DBT_FOO", "unset") }}' as foo,
  '{{ env_var("DBT_BAR", "unset") }}' as bar
