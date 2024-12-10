with sales_margin as (

    -- Use the int_sales_margin model as the source
    select * 
    from {{ ref('int_sales_margin') }}

),

aggregated_orders as (

    -- Aggregate data to calculate order-level metrics
    select
        orders_id,
        -- Use the earliest date_date for each orders_id
        min(date_date) as order_date,
        
        -- Calculate order-level metrics
        sum(revenue) as total_revenue,
        sum(purchase_cost) as total_purchase_cost,
        sum(margin) as total_margin

    from sales_margin
    group by orders_id

)

select * 
from aggregated_orders
