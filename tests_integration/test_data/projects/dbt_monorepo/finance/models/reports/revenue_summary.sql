{{ config(materialized='table') }}

with revenue_data as (
    select 'Q1' as quarter, 150000 as revenue
    union all
    select 'Q2' as quarter, 175000 as revenue
    union all
    select 'Q3' as quarter, 190000 as revenue
    union all
    select 'Q4' as quarter, 220000 as revenue
)

select
    quarter,
    revenue,
    revenue * 0.1 as tax_amount
    {{ add_audit_columns() }}
from revenue_data 
