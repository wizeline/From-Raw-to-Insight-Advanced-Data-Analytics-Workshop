-- Simple Community Analysis - Test Query
-- Run this first to verify your data exists and see basic insights

-- ===========================================
-- QUERY 1: Check if data exists
-- ===========================================
SELECT 
    'Data Check' as query_type,
    COUNT(*) as total_communities,
    COUNT(CASE WHEN community_area_name IS NOT NULL THEN 1 END) as named_communities,
    COUNT(CASE WHEN per_capita_income > 0 THEN 1 END) as communities_with_income,
    COUNT(CASE WHEN total_crime_incidents > 0 THEN 1 END) as communities_with_crime_data,
    COUNT(CASE WHEN avg_list_price > 0 THEN 1 END) as communities_with_real_estate
FROM SNOWFLAKE_LEARNING_DB.workshop.gold_community_analytics;

-- ===========================================
-- QUERY 2: Top 10 Communities by Income
-- ===========================================
SELECT 
    'Top Income Communities' as query_type,
    community_area_name,
    community_area_number,
    ROUND(per_capita_income, 2) as per_capita_income,
    ROUND(avg_list_price, 2) as avg_list_price,
    total_crime_incidents,
    ROUND(avg_school_safety_score, 2) as school_safety_score
FROM SNOWFLAKE_LEARNING_DB.workshop.gold_community_analytics
WHERE community_area_name IS NOT NULL
ORDER BY per_capita_income DESC
LIMIT 10;

-- ===========================================
-- QUERY 3: Safety Overview
-- ===========================================
SELECT 
    'Safety Overview' as query_type,
    CASE 
        WHEN total_crime_incidents = 0 THEN 'No Crime Reported'
        WHEN total_crime_incidents <= 10 THEN 'Very Low Crime'
        WHEN total_crime_incidents <= 25 THEN 'Low Crime'
        WHEN total_crime_incidents <= 50 THEN 'Medium Crime'
        WHEN total_crime_incidents <= 100 THEN 'High Crime'
        ELSE 'Very High Crime'
    END as crime_level,
    COUNT(*) as community_count,
    ROUND(AVG(per_capita_income), 2) as avg_income,
    ROUND(AVG(avg_list_price), 2) as avg_property_value,
    ROUND(AVG(avg_school_safety_score), 2) as avg_school_safety
FROM SNOWFLAKE_LEARNING_DB.workshop.gold_community_analytics
WHERE community_area_name IS NOT NULL
GROUP BY crime_level
ORDER BY 
    CASE crime_level
        WHEN 'No Crime Reported' THEN 1
        WHEN 'Very Low Crime' THEN 2
        WHEN 'Low Crime' THEN 3
        WHEN 'Medium Crime' THEN 4
        WHEN 'High Crime' THEN 5
        WHEN 'Very High Crime' THEN 6
    END;

-- ===========================================
-- QUERY 4: Real Estate by Property Type
-- ===========================================
-- Note: This assumes you have property type data in your gold layer
-- If not, you can run this against the real estate summary table
SELECT 
    'Real Estate Overview' as query_type,
    COUNT(*) as total_listings,
    ROUND(AVG(avg_list_price), 2) as avg_list_price,
    ROUND(AVG(avg_sold_price), 2) as avg_sold_price,
    ROUND(AVG(avg_bedrooms), 2) as avg_bedrooms,
    ROUND(AVG(avg_bathrooms), 2) as avg_bathrooms
FROM SNOWFLAKE_LEARNING_DB.workshop.gold_community_analytics
WHERE avg_list_price > 0;

-- ===========================================
-- QUERY 5: School Performance vs Real Estate
-- ===========================================
SELECT 
    'School vs Real Estate' as query_type,
    CASE 
        WHEN avg_school_safety_score >= 80 THEN 'Very Safe Schools'
        WHEN avg_school_safety_score >= 60 THEN 'Safe Schools'
        WHEN avg_school_safety_score >= 40 THEN 'Moderate Safety'
        WHEN avg_school_safety_score >= 20 THEN 'Low Safety'
        ELSE 'Very Low Safety'
    END as school_safety_level,
    COUNT(*) as community_count,
    ROUND(AVG(avg_list_price), 2) as avg_property_value,
    ROUND(AVG(per_capita_income), 2) as avg_income,
    ROUND(AVG(total_crime_incidents), 2) as avg_crime_incidents
FROM SNOWFLAKE_LEARNING_DB.workshop.gold_community_analytics
WHERE avg_school_safety_score > 0
GROUP BY school_safety_level
ORDER BY 
    CASE school_safety_level
        WHEN 'Very Safe Schools' THEN 1
        WHEN 'Safe Schools' THEN 2
        WHEN 'Moderate Safety' THEN 3
        WHEN 'Low Safety' THEN 4
        WHEN 'Very Low Safety' THEN 5
    END; 