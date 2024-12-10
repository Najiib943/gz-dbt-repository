WITH sales_data AS (
    SELECT 
        orders_id,
        products_id,
        revenue,
        -- Cast quantity to INT64 if it is currently stored as STRING
        CAST(quantity AS INT64) AS quantity
    FROM `galvanic-flame-442110-g0.gz_raw_data.raw_gz_sales`
),

product_data AS (
    SELECT 
        products_id,
        -- Cast purchase_price to FLOAT64 if it is stored as STRING
        CAST(purchase_price AS FLOAT64) AS purchase_price
    FROM `galvanic-flame-442110-g0.gz_raw_data.raw_gz_product`
),

joined_data AS (
    SELECT
        s.orders_id,
        s.products_id,
        s.revenue,
        s.quantity,
        p.purchase_price,
        -- Calculate purchase cost as quantity * purchase price
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
        -- Calculate margin as revenue - purchase cost
        revenue - purchase_cost AS margin
    FROM joined_data
)

-- Final select query to retrieve the results without LIMIT
SELECT * 
FROM calculated_data;
