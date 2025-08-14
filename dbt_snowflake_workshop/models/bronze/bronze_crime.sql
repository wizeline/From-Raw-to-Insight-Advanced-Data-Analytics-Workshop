{{
  config(
    materialized='view',
    schema='bronze'
  )
}}

-- Bronze layer: Raw crime incident data
-- This model ingests raw data with minimal transformation
select 
    ID,
    CASE_NUMBER,
    DATE,
    BLOCK,
    IUCR,
    PRIMARY_TYPE,
    DESCRIPTION,
    LOCATION_DESCRIPTION,
    ARREST,
    DOMESTIC,
    BEAT,
    DISTRICT,
    WARD,
    COMMUNITY_AREA_NUMBER,
    FBICODE,
    X_COORDINATE,
    Y_COORDINATE,
    YEAR,
    LATITUDE,
    LONGITUDE,
    LOCATION,
    current_timestamp() as ingested_at
from {{ source('raw_data', 'chicago_crime_raw') }}
