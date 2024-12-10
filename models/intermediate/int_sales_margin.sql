WITH 
sales_data AS (
    SELECT 
        orders_id,
        products_id,
        revenue,
        quantity
    FROM {{ source('raw', 'sales') }}

),

product_data AS (
    SELECT 
        products_id,
        purchase_price
    FROM {{ source('raw', 'product') }}
),

joined_data AS (
    SELECT
        s.orders_id,
        s.products_id,
        s.revenue,
        s.quantity,
        p.purchase_price,
        -- Calculate purchase_cost
        s.quantity * p.purchase_price AS purchase_cost
    FROM sales_data s
    INNER JOIN product_data p
        ON s.products_id = p.products_id
),


calculated_data AS (
    SELECT
        orders_id,
        products_id,
        revenue,
        quantity,
        purchase_price,
        purchase_cost,
        -- Calculate margin
        revenue - purchase_cost AS margin
    FROM joined_data
)

-- Final Output
SELECT * 
FROM calculated_data;

