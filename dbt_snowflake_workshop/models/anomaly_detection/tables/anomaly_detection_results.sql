{{
  config(
    materialized='table',
    schema='anomaly_detection'
  )
}}

SELECT 
    'DETECT_PRICE_ANOMALY_MULTI_SERIES' AS MODEL_NAME,
    SYSDATE() AS EXECUTION_TIME,
    SERIES,
    TS,
    Y,
    FORECAST,
    LOWER_BOUND,
    UPPER_BOUND,
    IS_ANOMALY,
    PERCENTILE,
    DISTANCE
    FROM TABLE(
        {{ var("database") }}.{{ var("schema") }}.{{ var("model_name") }}!DETECT_ANOMALIES(         
            INPUT_DATA => TABLE({{ ref('ep_chicago_listings_new_data') }}),
            SERIES_COLNAME => 'series_id',
            TIMESTAMP_COLNAME =>'listing_date',
            TARGET_COLNAME => 'listprice',
            CONFIG_OBJECT => {'prediction_interval': 0.95 }
        )
    );
