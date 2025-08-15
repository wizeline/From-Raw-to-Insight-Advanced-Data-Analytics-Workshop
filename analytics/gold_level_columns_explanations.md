# Gold Layer Tables: Complete Column Descriptions

## **Table 1: `gold_community_analytics`**

This table provides comprehensive community-level insights by combining all data sources. It's materialized as a **table** for performance optimization.

### **Primary Key & Location**
- **`community_area_number`** (INTEGER): Unique identifier for Chicago community areas (1-77)
- **`community_area_name`** (STRING): Human-readable name of the community area

### **Demographic & Economic Indicators (from Census)**
- **`per_capita_income`** (DECIMAL): Average income per person in the community
- **`hardship_index`** (INTEGER): Socioeconomic hardship score (0-100, higher = more hardship)
- **`percent_housing_crowded`** (DECIMAL): Percentage of overcrowded housing units
- **`percent_households_below_poverty`** (DECIMAL): Percentage of households below poverty line
- **`percent_unemployed`** (DECIMAL): Percentage of unemployed population (age 16+)
- **`percent_without_high_school`** (DECIMAL): Percentage without high school diploma (age 25+)
- **`percent_dependent_population`** (DECIMAL): Percentage of population under 18 or over 64

### **Real Estate Metrics (Aggregated by Community)**
- **`total_listings`** (INTEGER): Total number of property listings in the community
- **`sold_listings`** (INTEGER): Count of sold properties
- **`active_listings`** (INTEGER): Count of properties currently for sale
- **`avg_list_price`** (DECIMAL): Average listing price of all properties
- **`avg_sold_price`** (DECIMAL): Average sale price of sold properties
- **`avg_bedrooms`** (DECIMAL): Average number of bedrooms per property
- **`avg_bathrooms`** (DECIMAL): Average number of bathrooms per property
- **`avg_square_feet`** (DECIMAL): Average square footage per property

### **Crime & Safety Metrics (Aggregated by Community)**
- **`total_crime_incidents`** (INTEGER): Total number of reported crime incidents
- **`arrests_made`** (INTEGER): Count of incidents where arrests were made
- **`domestic_incidents`** (INTEGER): Count of domestic violence incidents
- **`theft_incidents`** (INTEGER): Count of theft-related crimes
- **`battery_incidents`** (INTEGER): Count of battery/assault incidents
- **`criminal_damage_incidents`** (INTEGER): Count of property damage crimes
- **`assault_incidents`** (INTEGER): Count of assault crimes
- **`deceptive_practice_incidents`** (INTEGER): Count of fraud/deception crimes

### **Education & School Metrics (Aggregated by Community)**
- **`total_schools`** (INTEGER): Total number of schools in the community
- **`avg_school_safety_score`** (DECIMAL): Average safety rating across all schools
- **`avg_student_attendance`** (DECIMAL): Average student attendance rate
- **`avg_teacher_attendance`** (DECIMAL): Average teacher attendance rate
- **`elementary_schools`** (INTEGER): Count of elementary schools (ES)
- **`middle_schools`** (INTEGER): Count of middle schools (MS)
- **`high_schools`** (INTEGER): Count of high schools (HS)

### **Business Intelligence Categories (Calculated Fields)**
- **`income_category`** (STRING): 
  - 'High Income' (>$50k)
  - 'Medium Income' ($30k-$50k)
  - 'Low Income' (<$30k)

- **`hardship_category`** (STRING):
  - 'Very High Hardship' (>80)
  - 'High Hardship' (60-80)
  - 'Medium Hardship' (40-60)
  - 'Low Hardship' (20-40)
  - 'Very Low Hardship' (<20)

- **`property_value_category`** (STRING):
  - 'High Value' (>$500k)
  - 'Medium Value' ($300k-$500k)
  - 'Low Value' (<$300k)

- **`crime_category`** (STRING):
  - 'High Crime' (>100 incidents)
  - 'Medium Crime' (50-100 incidents)
  - 'Low Crime' (<50 incidents)

### **Metadata**
- **`last_updated`** (TIMESTAMP): When the record was last refreshed

---

## **Table 2: `gold_real_estate_summary`**

This table provides multi-dimensional real estate analytics through a unified view of different analysis perspectives. It's materialized as a **table** and uses UNION ALL to combine four different summary types.

### **Common Columns Across All Summary Types**
- **`summary_type`** (STRING): Identifies the analysis perspective:
  - 'property_type_summary'
  - 'price_range_summary'
  - 'community_real_estate'
  - 'year_built_trends'

- **`category`** (STRING): The specific category within each summary type
- **`total_properties`** (INTEGER): Total count of properties in the category
- **`avg_list_price`** (DECIMAL): Average listing price
- **`avg_sold_price`** (DECIMAL): Average sale price (when applicable)
- **`avg_bedrooms`** (DECIMAL): Average number of bedrooms
- **`avg_bathrooms`** (DECIMAL): Average number of bathrooms
- **`avg_square_feet`** (DECIMAL): Average square footage
- **`last_updated`** (TIMESTAMP): Record refresh timestamp

### **Property Type Summary Columns**
- **`sold_properties`** (INTEGER): Count of sold properties by type
- **`active_properties`** (INTEGER): Count of active listings by type
- **`avg_lot_size`** (DECIMAL): Average lot size in square feet
- **`avg_year_built`** (DECIMAL): Average construction year
- **`avg_garage_spaces`** (DECIMAL): Average number of garage spaces

### **Price Range Summary Columns**
- **`sold_properties`** (INTEGER): Count of sold properties in price range
- **`price_range`** (STRING): Price segmentation:
  - 'Under $200k'
  - '$200k - $400k'
  - '$400k - $600k'
  - '$600k - $800k'
  - '$800k - $1M'
  - 'Over $1M'

### **Community Real Estate Summary Columns**
- **`community_area_number`** (INTEGER): Community area identifier
- **`community_area_name`** (STRING): Community area name
- **`per_capita_income`** (DECIMAL): Community income level
- **`hardship_index`** (INTEGER): Community hardship score
- **`sold_listings`** (INTEGER): Count of sold properties
- **`active_listings`** (INTEGER): Count of active listings
- **`avg_lot_size`** (DECIMAL): Average lot size
- **`avg_year_built`** (DECIMAL): Average construction year
- **`condo_count`** (INTEGER): Count of condominiums
- **`townhouse_count`** (INTEGER): Count of townhouses
- **`single_family_count`** (INTEGER): Count of single-family homes

### **Year Built Trends Columns**
- **`year_built_range`** (STRING): Construction era categories:
  - 'Pre-1900'
  - '1900-1949'
  - '1950-1979'
  - '1980-1999'
  - '2000-2009'
  - '2010-2019'
  - '2020+'

## **Key Business Insights from Gold Layer Columns**

### **Community Planning**
- **Safety Assessment**: Crime categories + school safety scores
- **Economic Development**: Income categories + hardship indices
- **Resource Allocation**: School counts + demographic percentages

### **Real Estate Investment**
- **Market Analysis**: Price ranges + property type performance
- **Neighborhood Insights**: Community metrics + property values
- **Historical Trends**: Year built analysis + price evolution

### **Data Quality & Governance**
- **Null Handling**: COALESCE functions ensure no missing values
- **Business Rules**: Categorical classifications with clear thresholds
- **Audit Trail**: Timestamp tracking for data freshness

## **Data Type Summary**

### **Numeric Types**
- **INTEGER**: Counts, identifiers, indices
- **DECIMAL**: Prices, percentages, averages, scores
- **TIMESTAMP**: Audit timestamps

### **String Types**
- **STRING**: Categories, names, classifications, ranges

### **Null Handling**
- **COALESCE Functions**: Ensure business metrics default to 0 instead of NULL
- **Conditional Logic**: Business rules applied through CASE statements
- **Data Validation**: Thresholds and ranges enforced at the gold layer

The gold layer transforms raw data into **business-ready analytics** with standardized naming, calculated business metrics, and comprehensive coverage across all key business dimensions. 