select
    customer_id,
    customer_name,
    total_orders,
    created_at,
    updated_at
from {{ ref('customer_metrics') }}
where total_orders > 75 
