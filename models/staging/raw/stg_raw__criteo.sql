with 

source as (

    select * from {{ source('raw', 'criteo') }}

),

renamed as (

    select
        date_date,
        paid_source,
        campaign_key,
        campgn_name AS campaign_name,  -- Correct the spelling of campgn_name to campaign_name using AS
        CAST(ads_cost AS FLOAT64) AS ads_cost,  -- Cast ads_cost to FLOAT64
        CAST(impression AS FLOAT64) AS impression,  -- Cast impression to FLOAT64
        CAST(click AS FLOAT64) AS click  -- Cast click to FLOAT64


    from source

)

select * from renamed
