{% macro validate_percentage(percentage_field) %}
    case 
        when {{ percentage_field }} < 0 or {{ percentage_field }} > 100 then null
        else {{ percentage_field }}
    end
{% endmacro %}
