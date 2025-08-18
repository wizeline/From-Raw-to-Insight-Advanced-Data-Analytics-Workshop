# Anomaly Detection with Snowflake ML Functions

## What is Anomaly Detection?

Anomaly detection is a machine learning technique used to identify unusual patterns or outliers in data that don't conform to expected behavior. In the context of Snowflake ML Functions, anomaly detection helps you automatically identify data points that are statistically different from the normal patterns in your time series data.

### Key Concepts:

- **Time Series Data**: Data points collected over time (e.g., daily sales, hourly temperatures, listing prices)
- **Anomalies**: Data points that significantly deviate from the expected pattern
- **Training Data**: Historical data used to learn normal patterns
- **Detection**: Process of identifying anomalies in new data

## Snowflake ML Anomaly Detection Features

Snowflake's `SNOWFLAKE.ML.ANOMALY_DETECTION` function provides:

- **Automatic Model Training**: No need to manually tune hyperparameters
- **Multi-series Support**: Handle multiple related time series simultaneously
- **Real-time Detection**: Detect anomalies as new data arrives
- **Confidence Intervals**: Provide uncertainty estimates for predictions
- **Scalable Processing**: Leverage Snowflake's distributed computing power

## Workshop Implementation: Chicago Real Estate Anomaly Detection

This workshop demonstrates anomaly detection on Chicago real estate listing data to identify unusual property prices.

### Data Flow Overview

```
Raw Data → Training View → Model Creation → New Data → Anomaly Detection → Results Table
```

## Step-by-Step Implementation

### Step 1: Create Training Data View (`vw_chicago_listings_training`)

**Purpose**: Prepare historical data for model training by creating a structured view with proper series identification.

**Key Features**:
- Converts listing data into time series format
- Creates `series_id` based on bedroom/bathroom combinations (e.g., "3bed_2bath")
- Filters data before August 1, 2025 for training
- Ensures each series has at least 3 data points for reliable training
- Adds anomaly labels for validation

**What it does**:
```sql
-- Groups properties by bedroom/bathroom combinations
-- Creates time series for each property type
-- Filters to training period (before 2025-08-01)
-- Ensures sufficient data points per series
```

### Step 2: Prepare New Data (`vw_chicago_listings_new_data`)

**Purpose**: Create a view containing new data for anomaly detection.

**Key Features**:
- References the training view
- Filters data from August 1, 2025 onwards
- Maintains consistent schema with training data
- Includes series_id for proper anomaly detection
- Uses view materialization for better performance

**What it does**:
```sql
-- Extracts new listings from August 1, 2025 onwards
-- Maintains same structure as training data including series_id
-- Prepares data for anomaly detection
```

### Step 3: Create and Train Anomaly Detection Model (`create_anomaly_object` macro)

**Purpose**: Create and train the Snowflake ML anomaly detection model using the prepared training data.

**Key Features**:
- Uses `SNOWFLAKE.ML.ANOMALY_DETECTION` function
- Trains on historical data (before 2025-08-01)
- Configures series, timestamp, and target columns
- Automatically learns normal price patterns for each property type
- Uses hardcoded reference to training view for simplicity

**Model Configuration**:
- **Series Column**: `series_id` (bedroom/bathroom combinations)
- **Timestamp Column**: `listing_date`
- **Target Column**: `listprice`
- **Training Data**: Historical listings before 2025-08-01
- **Source View**: `SNOWFLAKE_LEARNING_DB.WORKSHOP_ANOMALY_DETECTION.VW_CHICAGO_LISTINGS_TRAINING`

**Enhanced Features**:
- **Logging**: The macro logs database, schema, and model names for debugging
- **Dynamic SQL**: Uses dbt's `run_query` function for better error handling
- **Execution Logging**: Logs the generated SQL for transparency

**What it does**:
```sql
-- Creates anomaly detection model
-- Trains on historical listing data from the training view
-- Learns normal price patterns for each property type
-- Stores model in Snowflake for future use
-- Logs execution details for debugging and transparency
```

### Step 4: Detect Anomalies (`anomaly_detection_results` table)

**Purpose**: Apply the trained model to new data and store anomaly detection results.

**Key Features**:
- Uses the trained model to detect anomalies in new data
- Applies 95% prediction interval for anomaly detection
- Stores comprehensive results including confidence bounds
- Provides anomaly scores and percentiles
- Uses dbt variables for flexible model configuration

**Required Variables**:
The model requires the following dbt variables to be set:
- `database`: The database containing the trained anomaly detection model
- `schema`: The schema containing the trained model
- `model_name`: The name of the trained anomaly detection model

**Result Columns**:
- `SERIES`: Property type (e.g., "3bed_2bath")
- `TS`: Timestamp of the listing
- `Y`: Actual listing price
- `FORECAST`: Predicted normal price
- `LOWER_BOUND`/`UPPER_BOUND`: Confidence interval bounds
- `IS_ANOMALY`: Boolean flag for detected anomalies
- `PERCENTILE`: Anomaly score (0-1)
- `DISTANCE`: Distance from normal pattern

**What it does**:
```sql
-- Applies trained model to new data
-- Detects price anomalies
-- Stores results with confidence intervals
-- Provides anomaly scores and flags
```

## Running the Workshop

### Prerequisites
1. Ensure you have access to Snowflake with ML functions enabled
2. Verify your dbt project is configured with proper Snowflake credentials
3. Confirm raw Chicago listings data is available

### Execution Commands

```bash
# Step 1: Create training view
dbt run --select vw_chicago_listings_training

# Step 2: Prepare new data
dbt run --select vw_chicago_listings_new_data

# Step 3: Create and train the model
dbt run-operation create_anomaly_object --args '{database: "SNOWFLAKE_LEARNING_DB", schema: "WORKSHOP_ANOMALY_DETECTION", model_name: "DETECT_PRICE_ANOMALY_MULTI_SERIES"}'

# Step 4: Detect anomalies and store results
dbt run --select anomaly_detection_results --vars '{"database": "SNOWFLAKE_LEARNING_DB", "schema": "WORKSHOP_ANOMALY_DETECTION", "model_name": "DETECT_PRICE_ANOMALY_MULTI_SERIES"}'
```

## Interpreting Results

### Anomaly Detection Output

The `anomaly_detection_results` table contains:

- **Normal Listings**: Prices within expected ranges
- **Anomalous Listings**: Prices significantly above/below normal patterns
- **Confidence Intervals**: Uncertainty around predictions
- **Anomaly Scores**: Quantitative measure of how unusual each listing is

### Example Analysis

```sql
-- View detected anomalies
SELECT * FROM anomaly_detection_results 
WHERE IS_ANOMALY = TRUE 
ORDER BY Y ASC;

-- Analyze by property type
SELECT SERIES, COUNT(*) as total_listings, 
       SUM(CASE WHEN IS_ANOMALY THEN 1 ELSE 0 END) as anomalies
FROM anomaly_detection_results 
GROUP BY SERIES;
```

## Benefits of This Approach

1. **Automated Detection**: No manual threshold setting required
2. **Multi-series Support**: Handles different property types simultaneously
3. **Real-time Processing**: Can detect anomalies as new data arrives
4. **Scalable**: Leverages Snowflake's distributed computing
5. **Interpretable**: Provides confidence intervals and anomaly scores

## Use Cases

This anomaly detection approach can be applied to:

- **Real Estate**: Detect unusual property prices
- **E-commerce**: Identify pricing anomalies
- **Financial Data**: Detect unusual trading patterns
- **IoT Data**: Identify sensor malfunctions
- **Sales Data**: Detect unusual sales patterns

This workshop demonstrates how to leverage Snowflake's ML capabilities to build a production-ready anomaly detection system for real estate data analysis.
