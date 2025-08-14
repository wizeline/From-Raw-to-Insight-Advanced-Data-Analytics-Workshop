{{
  config(
    materialized='table',
    schema='gold'
  )
}}

-- Gold layer: Real estate summary analytics
-- This model provides business-ready insights for real estate analysis
with silver_listings as (
    select * from {{ ref('silver_listings') }}
),

silver_census as (
    select * from {{ ref('silver_census') }}
),

-- Property type analysis
property_type_summary as (
    select 
        type,
        count(*) as total_properties,
        count(case when status = 'sold' then 1 end) as sold_properties,
        count(case when status = 'for_sale' then 1 end) as active_properties,
        avg(list_price) as avg_list_price,
        avg(case when status = 'sold' then last_sold_price end) as avg_sold_price,
        avg(beds) as avg_bedrooms,
        avg(baths) as avg_bathrooms,
        avg(sqft) as avg_square_feet,
        avg(lot_sqft) as avg_lot_size,
        avg(year_built) as avg_year_built,
        avg(garage) as avg_garage_spaces
    from silver_listings
    where type is not null
    group by type
),

-- Price range analysis
price_range_summary as (
    select 
        case 
            when list_price < 200000 then 'Under $200k'
            when list_price < 400000 then '$200k - $400k'
            when list_price < 600000 then '$400k - $600k'
            when list_price < 800000 then '$600k - $800k'
            when list_price < 1000000 then '$800k - $1M'
            else 'Over $1M'
        end as price_range,
        count(*) as total_properties,
        count(case when status = 'sold' then 1 end) as sold_properties,
        avg(list_price) as avg_list_price,
        avg(case when status = 'sold' then last_sold_price end) as avg_sold_price,
        avg(beds) as avg_bedrooms,
        avg(baths) as avg_bathrooms,
        avg(sqft) as avg_square_feet
    from silver_listings
    where list_price is not null
    group by price_range
),

-- Community area real estate summary
community_real_estate as (
    select 
        l.community_area_number,
        c.community_area_name,
        c.per_capita_income,
        c.hardship_index,
        count(*) as total_listings,
        count(case when l.status = 'sold' then 1 end) as sold_listings,
        count(case when l.status = 'for_sale' then 1 end) as active_listings,
        avg(l.list_price) as avg_list_price,
        avg(case when l.status = 'sold' then l.last_sold_price end) as avg_sold_price,
        avg(l.beds) as avg_bedrooms,
        avg(l.baths) as avg_bathrooms,
        avg(l.sqft) as avg_square_feet,
        avg(l.lot_sqft) as avg_lot_size,
        count(case when l.type = 'condo' then 1 end) as condo_count,
        count(case when l.type = 'townhouse' then 1 end) as townhouse_count,
        count(case when l.type = 'single_family' then 1 end) as single_family_count,
        avg(l.year_built) as avg_year_built
    from silver_listings l
    left join silver_census c on l.community_area_number = c.community_area_number
    where l.community_area_number is not null
    group by l.community_area_number, c.community_area_name, c.per_capita_income, c.hardship_index
),

-- Market trends by year built
year_built_trends as (
    select 
        case 
            when year_built < 1900 then 'Pre-1900'
            when year_built < 1950 then '1900-1949'
            when year_built < 1980 then '1950-1979'
            when year_built < 2000 then '1980-1999'
            when year_built < 2010 then '2000-2009'
            when year_built < 2020 then '2010-2019'
            else '2020+'
        end as year_built_range,
        count(*) as total_properties,
        avg(list_price) as avg_list_price,
        avg(case when status = 'sold' then last_sold_price end) as avg_sold_price,
        avg(beds) as avg_bedrooms,
        avg(baths) as avg_bathrooms,
        avg(sqft) as avg_square_feet
    from silver_listings
    where year_built is not null
    group by year_built_range
)

-- Final real estate summary
select 
    'property_type_summary' as summary_type,
    type as category,
    total_properties,
    sold_properties,
    active_properties,
    avg_list_price,
    avg_sold_price,
    avg_bedrooms,
    avg_bathrooms,
    avg_square_feet,
    avg_lot_size,
    avg_year_built,
    avg_garage_spaces,
    null as community_area_number,
    null as community_area_name,
    null as per_capita_income,
    null as hardship_index,
    null as year_built_range,
    current_timestamp() as last_updated
from property_type_summary

union all

select 
    'price_range_summary' as summary_type,
    price_range as category,
    total_properties,
    sold_properties,
    null as active_properties,
    avg_list_price,
    avg_sold_price,
    avg_bedrooms,
    avg_bathrooms,
    avg_square_feet,
    null as avg_lot_size,
    null as avg_year_built,
    null as avg_garage_spaces,
    null as community_area_number,
    null as community_area_name,
    null as per_capita_income,
    null as hardship_index,
    null as year_built_range,
    current_timestamp() as last_updated
from price_range_summary

union all

select 
    'community_real_estate' as summary_type,
    community_area_name as category,
    total_listings as total_properties,
    sold_listings as sold_properties,
    active_listings as active_properties,
    avg_list_price,
    avg_sold_price,
    avg_bedrooms,
    avg_bathrooms,
    avg_square_feet,
    avg_lot_size,
    avg_year_built,
    null as avg_garage_spaces,
    community_area_number,
    community_area_name,
    per_capita_income,
    hardship_index,
    null as year_built_range,
    current_timestamp() as last_updated
from community_real_estate

union all

select 
    'year_built_trends' as summary_type,
    year_built_range as category,
    total_properties,
    null as sold_properties,
    null as active_properties,
    avg_list_price,
    avg_sold_price,
    avg_bedrooms,
    avg_bathrooms,
    avg_square_feet,
    null as avg_lot_size,
    null as avg_year_built,
    null as avg_garage_spaces,
    null as community_area_number,
    null as community_area_name,
    null as per_capita_income,
    null as hardship_index,
    year_built_range,
    current_timestamp() as last_updated
from year_built_trends
