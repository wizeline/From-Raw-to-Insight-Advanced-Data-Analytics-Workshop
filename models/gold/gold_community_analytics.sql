{{
  config(
    materialized='table',
    schema='gold'
  )
}}

-- Gold layer: Community analytics combining real estate, crime, census, and schools data
-- This model provides business-ready insights for community analysis
with silver_listings as (
    select * from {{ ref('silver_listings') }}
),

silver_crime as (
    select * from {{ ref('silver_crime') }}
),

silver_census as (
    select * from {{ ref('silver_census') }}
),

silver_schools as (
    select * from {{ ref('silver_schools') }}
),

-- Real estate metrics by community area
real_estate_metrics as (
    select 
        community_area_number,
        count(*) as total_listings,
        count(case when status = 'sold' then 1 end) as sold_listings,
        count(case when status = 'for_sale' then 1 end) as active_listings,
        count(case when status = 'pending' then 1 end) as pending_listings,
        avg(list_price) as avg_list_price,
        avg(case when status = 'sold' then last_sold_price end) as avg_sold_price,
        avg(beds) as avg_bedrooms,
        avg(baths) as avg_bathrooms,
        avg(sqft) as avg_square_feet,
        avg(lot_sqft) as avg_lot_size,
        count(case when type = 'condo' then 1 end) as condo_count,
        count(case when type = 'townhouse' then 1 end) as townhouse_count,
        count(case when type = 'single_family' then 1 end) as single_family_count
    from silver_listings
    where community_area_number is not null
    group by community_area_number
),

-- Crime metrics by community area
crime_metrics as (
    select 
        community_area_number,
        count(*) as total_incidents,
        count(case when arrest_made = true then 1 end) as arrests_made,
        count(case when domestic_incident = true then 1 end) as domestic_incidents,
        count(case when primary_type = 'THEFT' then 1 end) as theft_incidents,
        count(case when primary_type = 'BATTERY' then 1 end) as battery_incidents,
        count(case when primary_type = 'CRIMINAL DAMAGE' then 1 end) as criminal_damage_incidents,
        count(case when primary_type = 'ASSAULT' then 1 end) as assault_incidents,
        count(case when primary_type = 'DECEPTIVE PRACTICE' then 1 end) as deceptive_practice_incidents
    from silver_crime
    where community_area_number is not null
    group by community_area_number
),

-- School metrics by community area
school_metrics as (
    select 
        community_area_number,
        count(*) as total_schools,
        avg(safety_score) as avg_safety_score,
        avg(family_involvement_score) as avg_family_involvement_score,
        avg(environment_score) as avg_environment_score,
        avg(instruction_score) as avg_instruction_score,
        avg(leaders_score) as avg_leaders_score,
        avg(teachers_score) as avg_teachers_score,
        avg(parent_engagement_score) as avg_parent_engagement_score,
        avg(parent_environment_score) as avg_parent_environment_score,
        avg(average_student_attendance) as avg_student_attendance,
        avg(misconduct_rate_per_100_students) as avg_misconduct_rate,
        avg(average_teacher_attendance) as avg_teacher_attendance,
        avg(iep_compliance_rate) as avg_iep_compliance_rate,
        count(case when school_type = 'ES' then 1 end) as elementary_schools,
        count(case when school_type = 'MS' then 1 end) as middle_schools,
        count(case when school_type = 'HS' then 1 end) as high_schools
    from silver_schools
    where community_area_number is not null
    group by community_area_number
),

-- Combined community analytics
community_analytics as (
    select 
        c.community_area_number,
        c.community_area_name,
        c.per_capita_income,
        c.hardship_index,
        c.percent_housing_crowded,
        c.percent_households_below_poverty,
        c.percent_unemployed,
        c.percent_without_high_school,
        c.percent_dependent_population,
        
        -- Real estate metrics
        coalesce(re.total_listings, 0) as total_listings,
        coalesce(re.sold_listings, 0) as sold_listings,
        coalesce(re.active_listings, 0) as active_listings,
        coalesce(re.avg_list_price, 0) as avg_list_price,
        coalesce(re.avg_sold_price, 0) as avg_sold_price,
        coalesce(re.avg_bedrooms, 0) as avg_bedrooms,
        coalesce(re.avg_bathrooms, 0) as avg_bathrooms,
        coalesce(re.avg_square_feet, 0) as avg_square_feet,
        
        -- Crime metrics
        coalesce(cm.total_incidents, 0) as total_crime_incidents,
        coalesce(cm.arrests_made, 0) as arrests_made,
        coalesce(cm.domestic_incidents, 0) as domestic_incidents,
        coalesce(cm.theft_incidents, 0) as theft_incidents,
        coalesce(cm.battery_incidents, 0) as battery_incidents,
        
        -- School metrics
        coalesce(sm.total_schools, 0) as total_schools,
        coalesce(sm.avg_safety_score, 0) as avg_school_safety_score,
        coalesce(sm.avg_student_attendance, 0) as avg_student_attendance,
        coalesce(sm.avg_teacher_attendance, 0) as avg_teacher_attendance,
        coalesce(sm.elementary_schools, 0) as elementary_schools,
        coalesce(sm.middle_schools, 0) as middle_schools,
        coalesce(sm.high_schools, 0) as high_schools,
        
        -- Calculated metrics
        case 
            when c.per_capita_income > 50000 then 'High Income'
            when c.per_capita_income > 30000 then 'Medium Income'
            else 'Low Income'
        end as income_category,
        
        case 
            when c.hardship_index > 80 then 'Very High Hardship'
            when c.hardship_index > 60 then 'High Hardship'
            when c.hardship_index > 40 then 'Medium Hardship'
            when c.hardship_index > 20 then 'Low Hardship'
            else 'Very Low Hardship'
        end as hardship_category,
        
        case 
            when coalesce(re.avg_list_price, 0) > 500000 then 'High Value'
            when coalesce(re.avg_list_price, 0) > 300000 then 'Medium Value'
            else 'Low Value'
        end as property_value_category,
        
        case 
            when coalesce(cm.total_incidents, 0) > 100 then 'High Crime'
            when coalesce(cm.total_incidents, 0) > 50 then 'Medium Crime'
            else 'Low Crime'
        end as crime_category,
        
        current_timestamp() as last_updated
    from silver_census c
    left join real_estate_metrics re on c.community_area_number = re.community_area_number
    left join crime_metrics cm on c.community_area_number = cm.community_area_number
    left join school_metrics sm on c.community_area_number = sm.community_area_number
)

select * from community_analytics
