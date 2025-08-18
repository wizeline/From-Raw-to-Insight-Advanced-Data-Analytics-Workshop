{% macro create_anomaly_object(database, schema, model_name) %}

{% do log("Database Name: " ~ database, info=True) %}
{% do log("Schema Name: " ~ schema, info=True) %}
{% do log("Model Name: " ~ model_name, info=True) %}

{% set sql -%}
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION 
    {{ database }}.{{ schema }}.{{ model_name }}(
        INPUT_DATA =>
            TABLE(
                SELECT 
                    series_id,
                    listprice,
                    listing_date,
                    beds,
                    baths
                FROM  SNOWFLAKE_LEARNING_DB.WORKSHOP_ANOMALY_DETECTION.VW_CHICAGO_LISTINGS_TRAINING
                WHERE listing_date::DATE < '2025-08-01'
            ),
        SERIES_COLNAME => 'series_id',
        TIMESTAMP_COLNAME => 'listing_date',
        TARGET_COLNAME => 'listprice',
        LABEL_COLNAME => ''
    )
{% endset %}

{% set results = run_query(sql) %}
{% if execute %}
    {% do log(sql, info=True) %}
{% endif %}

{% endmacro %}