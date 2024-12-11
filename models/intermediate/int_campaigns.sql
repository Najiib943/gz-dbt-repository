-- models/intermediate/int_campaigns.sql

WITH adwords AS (
    SELECT *
    FROM {{ ref('stg_raw__adwords') }}
),
bing AS (
    SELECT *
    FROM {{ ref('stg_raw__bing') }}
),
criteo AS (
    SELECT *
    FROM {{ ref('stg_raw__criteo') }}
),
facebook AS (
    SELECT *
    FROM {{ ref('stg_raw__facebook') }}
)

SELECT * FROM adwords
UNION ALL
SELECT * FROM bing
UNION ALL
SELECT * FROM criteo
UNION ALL
SELECT * FROM facebook;
