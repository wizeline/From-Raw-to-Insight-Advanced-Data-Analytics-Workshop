{{config(
    materialized='table',
    schema='anomaly_detection'
)}}

with source as (
    Select 
    model_name
    ,execution_time
    ,series
    ,ts
    ,y
    ,forecast
    ,lower_bound
    ,upper_bound
    ,is_anomaly
    ,percentile
    ,distance
    from {{ ref('anomaly_detection_results') }}
    WHERE ts = '2025-09-17 00:00:00.000'
   )
   ,anomalies as (
    Select 
        date_trunc(day,ts) as detection_period,
        array_agg(object_construct(s.*))::string as anomalies 
    from source s 
    GROUP BY ALL
    )

Select 
detection_period,
SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet',concat('You are a senior data analyst for a real estate company. You have been tasked to review the output of an anomaly detection model which scopes a combination of beds and baths as series and listing price as the target feature. Please summarize in bullet format the anomalies that were found, as if reporting to a business stakeholder. Make this a daily summary with the most relevant anomalies you found. Please do not be too verbose, as the readers have a low attention span. Order the anomalies detected by order of importance. When summarizing an anomaly, compare the actual value to the forecast, and provide a percent difference to make it easier to read. Provide some suggestions based on the inputs. \n \n',anomalies)) as daily_summary,
SYSDATE() as load_date
from anomalies