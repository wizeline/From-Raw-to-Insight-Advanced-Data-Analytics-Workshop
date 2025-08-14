{{
  config(
    materialized='view',
    schema='silver'
  )
}}

-- Silver layer: Cleaned and standardized census demographic data
-- This model applies data quality checks and standardization
with bronze_census as (
    select * from {{ ref('bronze_census') }}
),

cleaned_census as (
    select 
        case 
            when COMMUNITY_AREA_NUMBER = 0 or COMMUNITY_AREA_NUMBER is null then null
            else COMMUNITY_AREA_NUMBER 
        end as community_area_number,
        COMMUNITY_AREA_NAME,
        case 
            when PERCENT_OF_HOUSING_CROWDED < 0 or PERCENT_OF_HOUSING_CROWDED > 100 then null
            else PERCENT_OF_HOUSING_CROWDED 
        end as percent_housing_crowded,
        case 
            when PERCENT_HOUSEHOLDS_BELOW_POVERTY < 0 or PERCENT_HOUSEHOLDS_BELOW_POVERTY > 100 then null
            else PERCENT_HOUSEHOLDS_BELOW_POVERTY 
        end as percent_households_below_poverty,
        case 
            when PERCENT_AGED_16__UNEMPLOYED < 0 or PERCENT_AGED_16__UNEMPLOYED > 100 then null
            else PERCENT_AGED_16__UNEMPLOYED 
        end as percent_unemployed,
        case 
            when PERCENT_AGED_25__WITHOUT_HIGH_SCHOOL_DIPLOMA < 0 or PERCENT_AGED_25__WITHOUT_HIGH_SCHOOL_DIPLOMA > 100 then null
            else PERCENT_AGED_25__WITHOUT_HIGH_SCHOOL_DIPLOMA 
        end as percent_without_high_school,
        case 
            when PERCENT_AGED_UNDER_18_OR_OVER_64 < 0 or PERCENT_AGED_UNDER_18_OR_OVER_64 > 100 then null
            else PERCENT_AGED_UNDER_18_OR_OVER_64 
        end as percent_dependent_population,
        case 
            when PER_CAPITA_INCOME < 0 or PER_CAPITA_INCOME is null then null
            else PER_CAPITA_INCOME 
        end as per_capita_income,
        case 
            when HARDSHIP_INDEX < 0 or HARDSHIP_INDEX > 100 then null
            else HARDSHIP_INDEX 
        end as hardship_index,
        ingested_at
    from bronze_census
    where COMMUNITY_AREA_NUMBER is not null  -- Remove records without community area number
)

select * from cleaned_census
