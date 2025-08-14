{{
  config(
    materialized='view',
    schema='bronze'
  )
}}

-- Bronze layer: Raw census demographic data
-- This model ingests raw data with minimal transformation
select 
    COMMUNITY_AREA_NUMBER,
    COMMUNITY_AREA_NAME,
    PERCENT_OF_HOUSING_CROWDED,
    PERCENT_HOUSEHOLDS_BELOW_POVERTY,
    PERCENT_AGED_16__UNEMPLOYED,
    PERCENT_AGED_25__WITHOUT_HIGH_SCHOOL_DIPLOMA,
    PERCENT_AGED_UNDER_18_OR_OVER_64,
    PER_CAPITA_INCOME,
    HARDSHIP_INDEX,
    current_timestamp() as ingested_at
from {{ source('raw_data', 'chicago_census_raw') }}
