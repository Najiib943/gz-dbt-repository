WITH sales_data AS (
    SELECT
        date_date,
        orders_id,
        revenue,
        quantity,
        p.products_id,
        p.purchase_price
    FROM {{ source('raw', 'sales') }} s
    LEFT JOIN {{ source('raw', 'product') }} p
        ON s.pdt_id = p.products_id
),

shipping_data AS (
   
    SELECT
        s.orders_id,
        s.shipping_fee,
        s.log_cost,
        s.ship_cost
    FROM  {{ source('raw', 'ship') }} s
),


aggregated_data AS (
    SELECT
        sd.date_date,
        COUNT(DISTINCT sd.orders_id) AS total_transactions,
        SUM(sd.revenue) AS total_revenue,
        AVG(sd.revenue / NULLIF(sd.quantity, 0)) AS avg_basket,
        SUM(sd.revenue - (sd.quantity * sd.purchase_price)) AS operational_margin,  
        SUM(sh.shipping_fee) AS total_shipping_fee,
        SUM(sh.log_cost) AS total_log_cost,
        SUM(sd.quantity) AS total_quantity_sold
    FROM sales_data sd
    LEFT JOIN shipping_data sh
        ON sd.orders_id = sh.orders_id
    GROUP BY sd.date_date
)


SELECT *
FROM aggregated_data
ORDER BY date_date;
