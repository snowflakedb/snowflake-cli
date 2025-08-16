{{ config(materialized='table') }}

with customer_data as (
    select 1 as customer_id, 'Alice' as customer_name, 100 as total_orders
    union all
    select 2 as customer_id, 'Bob' as customer_name, 50 as total_orders
    union all
    select 3 as customer_id, 'Charlie' as customer_name, 200 as total_orders
)

select
    customer_id,
    customer_name,
    total_orders
    {{ add_audit_columns() }}
from customer_data 
