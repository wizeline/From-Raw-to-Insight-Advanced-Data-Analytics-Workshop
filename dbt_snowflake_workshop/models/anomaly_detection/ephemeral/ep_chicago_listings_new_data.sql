{{
  config(
    materialized='ephemeral',
    schema='anomaly_detection'
  )
}}

    SELECT 
        LISTING_ID,
        BEDS,
        BATHS,
        STORIES,
        LISTPRICE,
        STATUS,
        LISTING_DATE,
        COMMUNITY_AREA_NUMBER       
    FROM {{ ref('vw_chicago_listings_training') }}
    WHERE listing_date >= '2025-08-01'