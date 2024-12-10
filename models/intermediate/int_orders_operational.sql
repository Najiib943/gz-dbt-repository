with orders_margin as (

    -- Use the int_orders_margin model for order-level margin data
    select
        orders_id,
        order_date as date_date,  -- Rename for clarity
        total_margin as margin
    from {{ ref('int_orders_margin') }}

),

shipping_data as (

    -- Use the stg_ship model for shipping-related data
    select
        orders_id,
        cast(ship_cost as FLOAT64) as ship_cost,  -- Ensure ship_cost is numeric
        shipping_fee,
        log_cost
    from {{ source('raw', 'ship') }}

),

joined_data as (

    -- Join orders_margin with shipping_data on orders_id
    select
        om.orders_id,
        om.date_date,
        om.margin,
        sd.shipping_fee,
        sd.log_cost,
        sd.ship_cost
    from orders_margin om
    inner join shipping_data sd
        on om.orders_id = sd.orders_id

),

operational_margin_calc as (

    -- Calculate operational margin
    select
        orders_id,
        date_date,
        margin + shipping_fee - log_cost - ship_cost as operational_margin
    from joined_data

)

select * 
from operational_margin_calc
