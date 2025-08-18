{{ config(materialized='table') }}

with source_data as (
    select 1 as id, 'test' as name
    union all
    select 2 as id, 'another' as name
    union all
    select null as id, 'null_test' as name
)

select 
    id,
    name,
    {{ dbt_utils.generate_surrogate_key(['id', 'name']) }} as surrogate_key
from source_data
where id is not null 
