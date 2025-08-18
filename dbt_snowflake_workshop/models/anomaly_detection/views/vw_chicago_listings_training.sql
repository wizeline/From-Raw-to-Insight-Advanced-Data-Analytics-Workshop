{{
  config(
    materialized='view',
    schema='anomaly_detection'
  )
}}

SELECT
    CAST(listing_id AS VARCHAR) AS LISTING_ID,
    CAST(beds AS NUMBER(38,0)) AS BEDS,
    CAST(baths AS NUMBER(38,0)) AS BATHS,
    CAST(stories AS NUMBER(38,0)) AS STORIES,
    CAST(listprice AS NUMBER(38,0)) AS LISTPRICE,
    CAST(status AS VARCHAR(16777216)) AS STATUS,
    CAST(listing_date AS DATE) AS LISTING_DATE,
    CAST(community_area_number AS NUMBER(38,0)) AS COMMUNITY_AREA_NUMBER,
    CASE 
        WHEN LISTING_DATE::DATE < '2025-08-01' AND CAST(LISTPRICE AS INT) > 5000000                        
            THEN TRUE
        WHEN LISTING_DATE::DATE < '2025-08-01' AND CAST(LISTPRICE AS INT) <= 0
            THEN TRUE
        WHEN LISTING_DATE::DATE < '2025-08-01'
            THEN FALSE
    END AS IS_ANOMALY,
    CONCAT(COALESCE(CAST(beds AS NUMBER(38,0)), 0), 'bed_', COALESCE(CAST(baths AS NUMBER(38,0)), 0), 'bath')::VARCHAR(16777216) AS series_id,
    
FROM {{ source('raw_data', 'chicago_listings_raw') }}
QUALIFY COUNT(CASE WHEN listing_date < '2025-08-01' THEN 1 END) 
        OVER (PARTITION BY CONCAT(COALESCE(beds, 0), 'bed_', COALESCE(baths, 0), 'bath')) > 3;
        --Consider only events where timestamp is less than 2025-08-01 and series_id has at least 3 distinct timestamps.