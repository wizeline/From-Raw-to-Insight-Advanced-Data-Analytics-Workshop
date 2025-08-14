{{
  config(
    materialized='view',
    schema='silver'
  )
}}

-- Silver layer: Cleaned and standardized crime incident data
-- This model applies data quality checks and standardization
with bronze_crime as (
    select * from {{ ref('bronze_crime') }}
),

cleaned_crime as (
    select 
        ID,
        CASE_NUMBER,
        case 
            when DATE = '' or DATE is null then null
            else try_to_date(DATE, 'YYYY-MM-DD')
        end as incident_date,
        BLOCK,
        IUCR,
        PRIMARY_TYPE,
        DESCRIPTION,
        LOCATION_DESCRIPTION,
        case 
            when ARREST = 'TRUE' then true
            when ARREST = 'FALSE' then false
            else null
        end as arrest_made,
        case 
            when DOMESTIC = 'TRUE' then true
            when DOMESTIC = 'FALSE' then false
            else null
        end as domestic_incident,
        case 
            when BEAT = 0 or BEAT is null then null
            else BEAT 
        end as beat,
        case 
            when DISTRICT = 0 or DISTRICT is null then null
            else DISTRICT 
        end as district,
        case 
            when WARD = 0 or WARD is null then null
            else WARD 
        end as ward,
        case 
            when COMMUNITY_AREA_NUMBER = 0 or COMMUNITY_AREA_NUMBER is null then null
            else COMMUNITY_AREA_NUMBER 
        end as community_area_number,
        FBICODE,
        case 
            when X_COORDINATE = 0 or X_COORDINATE is null then null
            else X_COORDINATE 
        end as x_coordinate,
        case 
            when Y_COORDINATE = 0 or Y_COORDINATE is null then null
            else Y_COORDINATE 
        end as y_coordinate,
        case 
            when YEAR = 0 or YEAR is null then null
            else YEAR 
        end as year,
        case 
            when LATITUDE = 0 or LATITUDE is null then null
            else LATITUDE 
        end as latitude,
        case 
            when LONGITUDE = 0 or LONGITUDE is null then null
            else LONGITUDE 
        end as longitude,
        LOCATION,
        ingested_at
    from bronze_crime
    where ID is not null  -- Remove records without incident ID
)

select * from cleaned_crime
