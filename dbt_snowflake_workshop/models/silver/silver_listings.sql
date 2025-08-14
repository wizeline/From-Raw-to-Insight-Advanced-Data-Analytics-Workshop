{{
  config(
    materialized='view',
    schema='silver'
  )
}}

-- Silver layer: Cleaned and standardized real estate listings data
-- This model applies data quality checks and standardization
with bronze_listings as (
    select * from {{ ref('bronze_listings') }}
),

cleaned_listings as (
    select 
        LISTING_ID,
        TYPE,
        description,
        case 
            when YEAR_BUILT = 0 or YEAR_BUILT is null then null
            else YEAR_BUILT 
        end as year_built,
        case 
            when BEDS = 0 or BEDS is null then null
            else BEDS 
        end as beds,
        case 
            when BATHS = 0 or BATHS is null then null
            else BATHS 
        end as baths,
        case 
            when BATHS_FULL = 0 or BATHS_FULL is null then null
            else BATHS_FULL 
        end as baths_full,
        case 
            when BATHS_HALF = 0 or BATHS_HALF is null then null
            else BATHS_HALF 
        end as baths_half,
        case 
            when GARAGE = 0 or GARAGE is null then null
            else GARAGE 
        end as garage,
        case 
            when LOT_SQFT = 0 or LOT_SQFT is null then null
            else LOT_SQFT 
        end as lot_sqft,
        case 
            when SQFT = 0 or SQFT is null then null
            else SQFT 
        end as sqft,
        case 
            when STORIES = 0 or STORIES is null then null
            else STORIES 
        end as stories,
        case 
            when LASTSOLDPRICE = 0 or LASTSOLDPRICE is null then null
            else LASTSOLDPRICE 
        end as last_sold_price,
        case 
            when SOLDON = '' or SOLDON is null then null
            else try_to_date(SOLDON, 'YYYY-MM-DD')
        end as sold_on,
        case 
            when LISTPRICE <= 0 or LISTPRICE is null then null
            else LISTPRICE 
        end as list_price,
        STATUS,
        case 
            when LISTING_DATE = '' or LISTING_DATE is null then null
            else try_to_date(LISTING_DATE, 'YYYY-MM-DD')
        end as listing_date,
        case 
            when COMMUNITY_AREA_NUMBER = 0 or COMMUNITY_AREA_NUMBER is null then null
            else COMMUNITY_AREA_NUMBER 
        end as community_area_number,
        ingested_at
    from bronze_listings
    where LISTING_ID is not null  -- Remove records without listing ID
)

select * from cleaned_listings
