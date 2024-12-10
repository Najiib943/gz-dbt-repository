WITH sales_data AS (
    SELECT
        s.orders_id,
        s.products_id,
        s.quantity,
        s.revenue
    FROM {{ ref('stg_sales') }} s
),
product_data AS (
    SELECT
        p.products_id,
        p.purchase_price
    FROM {{ ref('stg_product') }} p
)

SELECT
    sd.orders_id,
    sd.products_id,
    sd.quantity,
    sd.revenue,
    pd.purchase_price,
    sd.quantity * pd.purchase_price AS purchase_cost,
    sd.revenue - (sd.quantity * pd.purchase_price) AS margin
FROM sales_data sd
JOIN product_data pd
    ON sd.products_id = pd.products_id

