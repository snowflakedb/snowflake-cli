select
    sum(revenue) as total_annual_revenue,
    sum(tax_amount) as total_tax_amount
    {{ add_audit_columns() }}
from {{ ref('revenue_summary') }} 
