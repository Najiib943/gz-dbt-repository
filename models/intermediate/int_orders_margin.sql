with sales_margin as (

    select * 
    from {{ ref('int_sales_margin') }}

),

aggregated_orders as (

    select
        orders_id,
        min(date_date) as order_date,
        sum(revenue) as total_revenue,
        sum(purchase_cost) as total_purchase_cost,
        sum(margin) as total_margin

    from sales_margin
    group by orders_id

)

select * 
from aggregated_orders
