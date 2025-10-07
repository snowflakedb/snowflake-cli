{{ config(materialized='table') }}

select 
    id,
    name,
    uppercase_name
from {{ ref('first_model_with_local') }}
where id = 1
