{{
  config(
    materialized='view',
    schema='bronze'
  )
}}

-- Bronze layer: Raw real estate listings data
-- This model ingests raw data with minimal transformation
select 
    LISTING_ID,
    TYPE,
    TEXT as description,
    YEAR_BUILT,
    BEDS,
    BATHS,
    BATHS_FULL,
    BATHS_HALF,
    GARAGE,
    LOT_SQFT,
    SQFT,
    STORIES,
    LASTSOLDPRICE,
    SOLDON,
    LISTPRICE,
    STATUS,
    LISTING_DATE,
    COMMUNITY_AREA_NUMBER,
    current_timestamp() as ingested_at
from {{ source('raw_data', 'chicago_listings') }}
