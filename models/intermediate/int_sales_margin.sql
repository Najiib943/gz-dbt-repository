with
    sales_data as (
        select orders_id, products_id, revenue, quantity
        from {{ source("raw", "sales") }}

    ),

    product_data as (
        select products_id, purchase_price from {{ source("raw", "product") }}
    ),

    joined_data as (
        select
            s.orders_id,
            s.products_id,
            s.revenue,
            s.quantity,
            p.purchase_price,
            s.quantity * p.purchase_price as purchase_cost
        from sales_data s
        inner join product_data p on s.products_id = p.products_id
    ),

    calculated_data as (
        select
            orders_id,
            products_id,
            revenue,
            quantity,
            purchase_price,
            purchase_cost,
            revenue - purchase_cost as margin

        from joined_data
    )

