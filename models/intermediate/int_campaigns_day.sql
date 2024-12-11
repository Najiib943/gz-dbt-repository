
  SELECT
    date_date,
    paid_source,
    campaign_key,
    campaign_name,
    SUM(ads_cost) AS total_ads_cost,  -- Sum of ads_cost
    SUM(impression) AS total_impression,  -- Sum of impressions
    SUM(click) AS total_click  -- Sum of clicks

 FROM {{ ref("int_campaigns") }}
GROUP BY
    date_date,
    paid_source,
    campaign_key,
    campaign_name
 ORDER BY date_date DESC;
 
