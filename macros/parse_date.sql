{% macro parse_date(date_field) %}
    case 
        when {{ date_field }} = '' or {{ date_field }} is null then null
        else try_to_date({{ date_field }}, 'YYYY-MM-DD')
    end
{% endmacro %}
