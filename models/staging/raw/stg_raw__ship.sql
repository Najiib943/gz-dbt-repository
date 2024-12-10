with

 source as (

    select * from {{ source('raw', 'ship') }}

),

diff_check as (

  
    select
        orders_id,
        shipping_fee,
        shipping_fee_1
        logcost,
    from source
    where shipping_fee <> shipping_fee_1 
),

renamed as (

    select
        orders_id,
        shipping_fee, 
        logcost,
        cast(ship_cost as FLOAT64) as ship_cost 
    from source

)

select * from renamed

