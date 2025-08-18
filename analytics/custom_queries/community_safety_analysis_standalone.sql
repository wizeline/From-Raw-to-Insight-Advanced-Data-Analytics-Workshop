-- Community Safety and Real Estate Correlation Analysis
-- Standalone SQL version - Run directly in Snowflake
-- This analysis explores the relationship between crime rates, school safety, and real estate values

-- Step 1: Get the base community analytics data
with community_analytics as (
    select 
        community_area_name,
        community_area_number,
        per_capita_income,
        hardship_index,
        total_crime_incidents,
        avg_school_safety_score,
        avg_list_price,
        avg_sold_price,
        total_schools
    from SNOWFLAKE_LEARNING_DB.workshop_gold.gold_community_analytics
    where community_area_name is not null
),

-- Step 2: Create safety and business categories
safety_analysis as (
    select 
        community_area_name,
        community_area_number,
        per_capita_income,
        hardship_index,
        total_crime_incidents,
        avg_school_safety_score,
        avg_list_price,
        avg_sold_price,
        total_schools,
        
        -- Safety metrics
        case 
            when total_crime_incidents = 0 then 'No Crime Reported'
            when total_crime_incidents <= 10 then 'Very Low Crime'
            when total_crime_incidents <= 25 then 'Low Crime'
            when total_crime_incidents <= 50 then 'Medium Crime'
            when total_crime_incidents <= 100 then 'High Crime'
            else 'Very High Crime'
        end as crime_level,
        
        case 
            when avg_school_safety_score >= 80 then 'Very Safe Schools'
            when avg_school_safety_score >= 60 then 'Safe Schools'
            when avg_school_safety_score >= 40 then 'Moderate Safety'
            when avg_school_safety_score >= 20 then 'Low Safety'
            else 'Very Low Safety'
        end as school_safety_level,
        
        -- Income categories
        case 
            when per_capita_income > 50000 then 'High Income'
            when per_capita_income > 30000 then 'Medium Income'
            else 'Low Income'
        end as income_category,
        
        -- Property value categories
        case 
            when avg_list_price > 500000 then 'High Value'
            when avg_list_price > 300000 then 'Medium Value'
            else 'Low Value'
        end as property_value_category
        
    from community_analytics
),

-- Step 3: Aggregate by safety categories
safety_correlations as (
    select 
        crime_level,
        school_safety_level,
        income_category,
        property_value_category,
        count(*) as community_count,
        avg(per_capita_income) as avg_income,
        avg(avg_list_price) as avg_property_value,
        avg(avg_school_safety_score) as avg_school_safety,
        avg(total_crime_incidents) as avg_crime_incidents
    from safety_analysis
    group by crime_level, school_safety_level, income_category, property_value_category
)

-- Final analysis results
select 
    crime_level,
    school_safety_level,
    income_category,
    property_value_category,
    community_count,
    round(avg_income, 2) as avg_income,
    round(avg_property_value, 2) as avg_property_value,
    round(avg_school_safety, 2) as avg_school_safety,
    round(avg_crime_incidents, 2) as avg_crime_incidents,
    case 
        when avg_school_safety >= 70 and avg_crime_incidents <= 20 then 'High Safety Community'
        when avg_school_safety >= 50 and avg_crime_incidents <= 40 then 'Moderate Safety Community'
        else 'Safety Improvement Needed'
    end as safety_assessment
from safety_correlations
order by avg_income desc, avg_property_value desc;

-- ===========================================
-- ALTERNATIVE: Direct Community-Level Analysis
-- ===========================================

-- If you want to see individual community results instead of aggregated categories:
/*
select 
    community_area_name,
    community_area_number,
    per_capita_income,
    hardship_index,
    total_crime_incidents,
    avg_school_safety_score,
    avg_list_price,
    avg_sold_price,
    total_schools,
    
    -- Safety metrics
    case 
        when total_crime_incidents = 0 then 'No Crime Reported'
        when total_crime_incidents <= 10 then 'Very Low Crime'
        when total_crime_incidents <= 25 then 'Low Crime'
        when total_crime_incidents <= 50 then 'Medium Crime'
        when total_crime_incidents <= 100 then 'High Crime'
        else 'Very High Crime'
    end as crime_level,
    
    case 
        when avg_school_safety_score >= 80 then 'Very Safe Schools'
        when avg_school_safety_score >= 60 then 'Safe Schools'
        when avg_school_safety_score >= 40 then 'Moderate Safety'
        when avg_school_safety_score >= 20 then 'Low Safety'
        else 'Very Low Safety'
    end as school_safety_level,
    
    -- Income categories
    case 
        when per_capita_income > 50000 then 'High Income'
        when per_capita_income > 30000 then 'Medium Income'
        else 'Low Income'
    end as income_category,
    
    -- Property value categories
    case 
        when avg_list_price > 500000 then 'High Value'
        when avg_list_price > 300000 then 'Medium Value'
        else 'Low Value'
    end as property_value_category,
    
    -- Safety assessment
    case 
        when avg_school_safety_score >= 70 and total_crime_incidents <= 20 then 'High Safety Community'
        when avg_school_safety_score >= 50 and total_crime_incidents <= 40 then 'Moderate Safety Community'
        else 'Safety Improvement Needed'
    end as safety_assessment
    
from SNOWFLAKE_LEARNING_DB.workshop_gold.gold_community_analytics
where community_area_name is not null
order by per_capita_income desc, avg_list_price desc;
*/ 