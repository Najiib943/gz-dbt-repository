
 SELECT
    finance_days.date_date AS date,  -- Use date_date explicitly from finance_days
    ROUND(SUM(finance_days.operational_margin - int_campaigns_day.ads_cost), 0) AS ads_margin,  -- Calculate ads_margin
    ROUND(AVG(finance_days.revenue), 1) AS average_basket,  -- Calculate average basket
    ROUND(SUM(finance_days.operational_margin), 0) AS operational_margin,  -- Operational margin
    ROUND(SUM(int_campaigns_day.ads_cost), 0) AS ads_cost,  -- Ads cost
    ROUND(SUM(int_campaigns_day.ads_impression), 0) AS ads_impression,  -- Ads impressions
    ROUND(SUM(int_campaigns_day.ads_clicks), 0) AS ads_clicks,  -- Ads clicks
    SUM(finance_days.quantity) AS quantity,  -- Total quantity
    ROUND(SUM(finance_days.revenue), 0) AS revenue,  -- Total revenue
    ROUND(SUM(finance_days.purchase_cost), 0) AS purchase_cost,  -- Total purchase cost
    ROUND(SUM(finance_days.margin), 0) AS margin,  -- Total margin
    ROUND(SUM(finance_days.shipping_fee), 0) AS shipping_fee,  -- Total shipping fee
    ROUND(SUM(finance_days.logcost), 0) AS logcost,  -- Total logistics cost
    ROUND(SUM(finance_days.ship_cost), 0) AS ship_cost  -- Total shipping cost
FROM {{ ref("finance_days") }} AS finance_days
LEFT JOIN {{ ref("int_campaigns_day") }} AS int_campaigns_day
    ON finance_days.date_date = int_campaigns_day.date_date  -- Join on date_date
GROUP BY finance_days.date_date  -- Group by date_date for aggregation
ORDER BY finance_days.date_date DESC;  -- Sort by date_date in descending order
