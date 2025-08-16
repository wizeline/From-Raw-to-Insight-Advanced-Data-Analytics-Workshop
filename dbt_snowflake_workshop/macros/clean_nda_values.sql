{% macro clean_nda_values(column_name) %}
    case when {{ column_name }} = 'NDA' then null else {{ column_name }} end
{% endmacro %}

