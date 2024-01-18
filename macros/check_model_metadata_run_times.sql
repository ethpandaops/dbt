{% macro check_model_metadata_run_times(model, current_time='NOW()', interval='1 HOUR', delay_from_now='5 MINUTE') %}
    {% if execute %}

        -- Use ClickHouse's formatDateTime function to format the datetime
        {% set last_run_query %}
            SELECT 
                CASE 
                    WHEN MAX(target_date_time) = '1970-01-01 00:00:00' THEN formatDateTime({{ current_time }} - INTERVAL {{ interval }} - INTERVAL {{ delay_from_now }}, '%Y-%m-%d %H:%i:%S')
                    ELSE formatDateTime(MAX(target_date_time), '%Y-%m-%d %H:%i:%S')
                END as start_time,
                CASE 
                    WHEN MAX(target_date_time) = '1970-01-01 00:00:00' THEN formatDateTime({{ current_time }} - INTERVAL {{ delay_from_now }}, '%Y-%m-%d %H:%i:%S')
                    ELSE formatDateTime(MAX(target_date_time) + INTERVAL {{ interval }}, '%Y-%m-%d %H:%i:%S')
                END as end_time,
                parseDateTime64BestEffortOrNull(end_time) <= {{ current_time }} - INTERVAL {{ delay_from_now }} as should_run
            FROM {{ target.schema }}.model_metadata 
            WHERE model = '{{ model }}'
        {% endset %}

        {% set last_run_result = run_query(last_run_query) %}

        {% if last_run_result is none %}
            {{ log("Query did not return any results.", "warn") }}
            {{ return({'start_time': None, 'end_time': None, 'should_run': False}) }}
        {% else %}
            {% set start_time = last_run_result.columns[0].values()[0] %}
            {% set end_time = last_run_result.columns[1].values()[0] %}
            {% set should_run = last_run_result.columns[2].values()[0] %}

            -- Return start_time and end_time
            {{ return({'start_time': start_time, 'end_time': end_time, 'should_run': should_run}) }}
        {% endif %}
    {% else %}
        {{ return({'start_time': None, 'end_time': None, 'should_run': False}) }}
    {% endif %}
{% endmacro %}
