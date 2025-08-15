# Gold Level Tables: Complete Data Transformation Flow

## **Data Flow Architecture: Bronze → Silver → Gold**

The gold level tables represent the **final transformation layer** that creates business-ready analytics by combining cleaned data from multiple silver models. Here's how the complete flow works:

## **1. Data Transformation Chain**

### **Bronze Layer (Raw Ingestion)**
```
Raw Chicago Data → Bronze Models
├── chicago_listings_raw → bronze_listings
├── chicago_crime_raw → bronze_crime  
├── chicago_census_raw → bronze_census
└── chicago_schools_raw → bronze_schools
```

### **Silver Layer (Data Cleaning)**
```
Bronze Models → Silver Models (with data quality)
├── bronze_listings → silver_listings (cleaned, standardized)
├── bronze_crime → silver_crime (parsed dates, validated)
├── bronze_census → silver_census (percentage validations)
└── bronze_schools → silver_schools (score validations)
```

### **Gold Layer (Business Analytics)**
```
Silver Models → Gold Models (aggregated insights)
├── silver_* → gold_community_analytics
└── silver_* → gold_real_estate_summary
```

## **2. Gold Table 1: `gold_community_analytics`**

### **Purpose**: Community-level insights combining all data sources

### **Data Sources**:
- **`silver_census`** → Demographic & economic indicators
- **`silver_listings`** → Real estate metrics  
- **`silver_crime`** → Safety & crime statistics
- **`silver_schools`** → Education performance

### **Key Transformations**:
```sql
-- 1. Real Estate Aggregation by Community
real_estate_metrics as (
    select community_area_number,
           count(*) as total_listings,
           avg(list_price) as avg_list_price,
           count(case when type = 'condo' then 1 end) as condo_count
    from silver_listings
    group by community_area_number
)

-- 2. Crime Aggregation by Community  
crime_metrics as (
    select community_area_number,
           count(*) as total_incidents,
           count(case when arrest_made = true then 1 end) as arrests_made
    from silver_crime
    group by community_area_number
)

-- 3. School Aggregation by Community
school_metrics as (
    select community_area_number,
           avg(safety_score) as avg_safety_score,
           count(case when school_type = 'ES' then 1 end) as elementary_schools
    from silver_schools
    group by community_area_number
)
```

### **Business Logic Applied**:
- **Income Categories**: High (>$50k), Medium ($30k-$50k), Low (<$30k)
- **Hardship Categories**: Very High (>80), High (60-80), Medium (40-60), Low (20-40), Very Low (<20)
- **Property Value Categories**: High (>$500k), Medium ($300k-$500k), Low (<$300k)
- **Crime Categories**: High (>100 incidents), Medium (50-100), Low (<50)

## **3. Gold Table 2: `gold_real_estate_summary`**

### **Purpose**: Multi-dimensional real estate analytics

### **Data Sources**:
- **`silver_listings`** → Property details & pricing
- **`silver_census`** → Community demographics

### **Key Transformations**:

#### **A. Property Type Analysis**
```sql
property_type_summary as (
    select type,  -- condo, townhouse, single_family
           count(*) as total_properties,
           avg(list_price) as avg_list_price,
           avg(beds) as avg_bedrooms
    from silver_listings
    group by type
)
```

#### **B. Price Range Segmentation**
```sql
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
        avg(list_price) as avg_list_price
    from silver_listings
    group by price_range
)
```

#### **C. Community Real Estate Summary**
```sql
community_real_estate as (
    select l.community_area_number,
           c.community_area_name,
           c.per_capita_income,
           c.hardship_index,
           count(*) as total_listings,
           avg(l.list_price) as avg_list_price
    from silver_listings l
    left join silver_census c on l.community_area_number = c.community_area_number
    group by l.community_area_number, c.community_area_name, c.per_capita_income, c.hardship_index
)
```

#### **D. Market Trends by Year Built**
```sql
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
        avg(list_price) as avg_list_price
    from silver_listings
    group by year_built_range
)
```

## **4. Key Benefits of This Architecture**

### **Data Quality Assurance**:
- Bronze: Raw data preservation
- Silver: Data validation & standardization  
- Gold: Business rule enforcement

### **Performance Optimization**:
- Bronze: Views (real-time)
- Silver: Views (real-time)
- Gold: Tables (pre-computed aggregations)

### **Business Intelligence**:
- **Community Planning**: Safety scores, school quality, crime rates
- **Real Estate Investment**: Market trends, price analysis, neighborhood insights
- **Public Policy**: Economic indicators, hardship mapping, resource allocation

### **Scalability**:
- Modular design allows independent updates
- Clear separation of concerns
- Easy to add new data sources or business logic

## **5. Complete Model Dependencies**

### **Bronze Layer Dependencies**:
- `bronze_census` ← `source('raw_data', 'chicago_census_raw')`
- `bronze_crime` ← `source('raw_data', 'chicago_crime_raw')`
- `bronze_listings` ← `source('raw_data', 'chicago_listings_raw')`
- `bronze_schools` ← `source('raw_data', 'chicago_public_schools_raw')`

### **Silver Layer Dependencies**:
- `silver_census` ← `ref('bronze_census')`
- `silver_crime` ← `ref('bronze_crime')`
- `silver_listings` ← `ref('bronze_listings')`
- `silver_schools` ← `ref('bronze_schools')`

### **Gold Layer Dependencies**:
- `gold_community_analytics` ← `ref('silver_census')`, `ref('silver_listings')`, `ref('silver_crime')`, `ref('silver_schools')`
- `gold_real_estate_summary` ← `ref('silver_listings')`, `ref('silver_census')`

## **6. Business Use Cases**

### **For Real Estate Professionals**:
- Market analysis by neighborhood
- Property type performance comparison
- Price trend analysis by property age
- Community safety assessment

### **For Community Planners**:
- Resource allocation based on hardship indices
- School performance correlation with demographics
- Crime pattern analysis by community
- Economic development planning

### **For Data Analysts**:
- Multi-dimensional data exploration
- Correlation analysis between different metrics
- Trend identification across time periods
- Data quality monitoring and validation

This architecture transforms raw Chicago data into **actionable business intelligence** that can drive real estate decisions, community planning, and public policy initiatives. 