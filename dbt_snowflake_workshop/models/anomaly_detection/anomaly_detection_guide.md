# Anomaly Detection with Snowflake ML Functions

## Overview

Anomaly detection identifies unusual patterns in time series data. Snowflake ML provides automated anomaly detection with multi-series support, confidence intervals, and scalable processing.

## Workshop Implementation: Chicago Real Estate Anomaly Detection

Detect unusual property prices using historical listing data.


### Data Flow Overview

```
Raw Data → Training View → Model Creation → New Data → Anomaly Detection → Results Table → AI Summary
```

## Step-by-Step Implementation

### Step 1: Create Training Data View (`vw_chicago_listings_training`)

**Purpose**: Prepare historical data for model training.

**Key Features**:
- Creates time series by bedroom/bathroom combinations
- Filters data before August 1, 2025 for training
- Ensures minimum 3 data points per series
- Adds anomaly labels for validation

### Step 2: Prepare New Data (`vw_chicago_listings_new_data`)

**Purpose**: Create a view containing new data for anomaly detection.

**Key Features**:
- Filters data from August 1, 2025 onwards
- Includes series_id for proper anomaly detection
- Uses view materialization for better performance

### Step 3: Create and Train Anomaly Detection Model (`create_anomaly_object` macro)

**Purpose**: Create and train the Snowflake ML anomaly detection model.

**Key Features**:
- Uses `SNOWFLAKE.ML.ANOMALY_DETECTION` function
- Trains on historical data (before 2025-08-01)
- Configures series, timestamp, and target columns
- Includes logging for debugging and transparency

**Model Configuration**:
- **Series Column**: `series_id` (bedroom/bathroom combinations)
- **Timestamp Column**: `listing_date`
- **Target Column**: `listprice`
- **Source View**: `SNOWFLAKE_LEARNING_DB.WORKSHOP_ANOMALY_DETECTION.VW_CHICAGO_LISTINGS_TRAINING`

### Step 4: Detect Anomalies (`anomaly_detection_results` table)

**Purpose**: Apply the trained model to new data and store anomaly detection results.

**Key Features**:
- Uses the trained model to detect anomalies in new data
- Applies 95% prediction interval for anomaly detection
- Uses dbt variables for flexible model configuration

**Required Variables**:
- `database`: Database containing the trained model
- `schema`: Schema containing the trained model
- `model_name`: Name of the trained model

**Result Columns**:
- `SERIES`: Property type (e.g., "3bed_2bath")
- `TS`: Timestamp of the listing
- `Y`: Actual listing price
- `FORECAST`: Predicted normal price
- `IS_ANOMALY`: Boolean flag for detected anomalies
- `PERCENTILE`: Anomaly score (0-1)

### Step 5: Generate AI Summary (`cortex_daily_summary` table)

**Purpose**: Create AI-powered daily summaries of detected anomalies using Snowflake Cortex.

**Key Features**:
- Uses Snowflake Cortex with Claude-3-5-Sonnet model
- Aggregates anomalies by detection period
- Provides business-friendly summaries with percent differences
- Orders anomalies by importance
- Includes actionable suggestions
- Configurable date filtering via dbt variables

**Required Variables**:
- `listing_datetime`: Target date for anomaly analysis (default: "2025-09-17 00:00:00.000")

**What it does**:
- Processes anomaly detection results for specific dates
- Generates concise bullet-point summaries for stakeholders
- Compares actual vs forecasted values with percent differences
- Provides business recommendations based on findings

## Running the Workshop

### Prerequisites
1. Snowflake with ML functions enabled
2. dbt project configured with Snowflake credentials
3. Raw Chicago listings data available

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

# Step 5: Generate AI summary
dbt run --select cortex_daily_summary --vars '{"listing_datetime": "2025-09-17 00:00:00.000"}'
```

## Interpreting Results

### Anomaly Detection Output

The `anomaly_detection_results` table contains normal and anomalous listings with confidence intervals and anomaly scores.

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

### AI Summary Output

The `cortex_daily_summary` table provides:
- Business-friendly summaries of detected anomalies
- Percent differences between actual and forecasted values
- Prioritized anomalies by importance
- Actionable recommendations for stakeholders

## Benefits

1. **Automated Detection**: No manual threshold setting required
2. **Multi-series Support**: Handles different property types simultaneously
3. **AI-Powered Insights**: Automated business summaries with recommendations
4. **Scalable**: Leverages Snowflake's distributed computing
5. **Interpretable**: Provides confidence intervals and anomaly scores

## Use Cases

- **Real Estate**: Detect unusual property prices
- **E-commerce**: Identify pricing anomalies
- **Financial Data**: Detect unusual trading patterns
- **IoT Data**: Identify sensor malfunctions
- **Sales Data**: Detect unusual sales patterns

This workshop demonstrates how to leverage Snowflake's ML capabilities to build a production-ready anomaly detection system for real estate data analysis.
