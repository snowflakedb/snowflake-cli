{{ config(materialized='table') }}

select 
    id,
    name,
    surrogate_key,
    'test_value' as processed_field
from {{ ref('first_model_with_utils') }}
where id = 1 
