# Chicago Analytics dbt Project

A comprehensive dbt project for analyzing Chicago real estate, crime, census, and public school data using a bronze/silver/gold architecture with Snowflake.

## Project Overview

This dbt project transforms and analyzes four key Chicago datasets:
- **Real Estate Listings**: Property listings with details like price, type, location
- **Crime Incidents**: Chicago Police Department crime data
- **Census Demographics**: Community area demographic information
- **Public Schools**: Chicago Public Schools performance data

## Architecture

The project follows a **bronze/silver/gold** data architecture:

### Bronze Layer (`models/bronze/`)
- Raw data ingestion with minimal transformation
- Preserves original data structure
- Adds ingestion timestamps

### Silver Layer (`models/silver/`)
- Data cleaning and standardization
- Data quality checks and validation
- Consistent naming conventions
- Type casting and formatting

### Gold Layer (`models/gold/`)
- Business-ready aggregated data
- Cross-dataset analytics
- Calculated metrics and KPIs
- Optimized for reporting and analysis

## Setup Instructions

### Prerequisites
- Python 3.8+
- dbt Core 1.7.0+
- Snowflake account and credentials

### Installation

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Snowflake connection**:
   - Update `profiles.yml` with your Snowflake credentials
   - Replace placeholder values:
     - `your_snowflake_account`
     - `your_username`
     - `your_password`
     - `your_role`
     - `your_database`
     - `your_warehouse`
     - `your_schema`

3. **Load source data**:
   - Upload the CSV files from the `raw/` directory to your Snowflake database
   - Place them in the `raw` schema as defined in `models/sources.yml`

## Running the Project

### Initial Setup
```bash
# Install dbt dependencies
dbt deps

# Run all models
dbt run

# Run tests
dbt test
```

### Development Workflow
```bash
# Run specific model
dbt run --select model_name

# Run specific layer
dbt run --select bronze+
dbt run --select silver+
dbt run --select gold+

# Run tests for specific model
dbt test --select model_name

# Generate documentation
dbt docs generate
dbt docs serve
```

## Data Models

### Bronze Models
- `bronze_listings`: Raw real estate listings data
- `bronze_crime`: Raw crime incident data
- `bronze_census`: Raw census demographic data
- `bronze_schools`: Raw public schools data

### Silver Models
- `silver_listings`: Cleaned and standardized listings data
- `silver_crime`: Cleaned and standardized crime data
- `silver_census`: Cleaned and standardized census data
- `silver_schools`: Cleaned and standardized schools data

### Gold Models
- `gold_community_analytics`: Comprehensive community analysis combining all datasets
- `gold_real_estate_summary`: Real estate market analysis and trends

## Data Quality Tests

### Generic Tests (defined in `models/sources.yml`)
- `unique`: Ensures primary keys are unique
- `not_null`: Ensures required fields are not null
- `relationships`: Validates foreign key relationships

### Custom Tests (`tests/`)
- `test_listing_prices_positive`: Ensures listing prices are positive
- `test_crime_dates_valid`: Validates crime incident dates
- `test_census_percentages_valid`: Ensures percentage values are 0-100
- `test_school_scores_valid`: Ensures school scores are 0-100

## Macros

### Data Processing Macros (`macros/`)
- `validate_percentage`: Validates percentage values (0-100)
- `parse_date`: Parses date strings to proper date format

## Key Insights

The project enables analysis of:

### Real Estate Market
- Property type distribution and pricing
- Market trends by year built
- Community area real estate performance
- Price range analysis

### Community Analytics
- Income and hardship correlations
- Crime patterns by community
- School performance metrics
- Cross-dataset community insights

### Data Quality
- Comprehensive data validation
- Automated quality checks
- Data lineage tracking
- Audit trails

## Project Structure

```
chicago_analytics/
├── dbt_project.yml          # Project configuration
├── profiles.yml             # Snowflake connection settings
├── requirements.txt         # Python dependencies
├── README.md               # Project documentation
├── models/
│   ├── sources.yml         # Source definitions and tests
│   ├── bronze/             # Bronze layer models
│   ├── silver/             # Silver layer models
│   └── gold/               # Gold layer models
├── tests/                  # Custom data quality tests
├── macros/                 # Reusable SQL macros
├── seeds/                  # Static data files
├── snapshots/              # Type 2 SCD snapshots
└── analyses/               # Ad-hoc analyses
```

## Contributing

1. Follow the bronze/silver/gold architecture
2. Add appropriate tests for new models
3. Update documentation
4. Run tests before submitting changes

## Support

For questions or issues, please refer to the dbt documentation or create an issue in the project repository.

---

**Note**: This project uses placeholder Snowflake credentials. Update `profiles.yml` with your actual Snowflake connection details before running.

