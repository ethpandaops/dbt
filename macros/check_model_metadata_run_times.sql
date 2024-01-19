{% macro check_model_metadata_run_times(model, current_time='NOW()', interval='1 HOUR', delay_from_now='5 MINUTE') %}
    {% if execute %}

        -- Use ClickHouse's formatDateTime function to format the datetime
        {% set last_run_query %}
            SELECT 
                CASE 
                    WHEN MAX(last_run_date_time) = '1970-01-01 00:00:00' THEN formatDateTime({{ current_time }} - INTERVAL {{ interval }}, '%Y-%m-%d %H:%i:%S')
                    ELSE formatDateTime(MAX(last_run_date_time), '%Y-%m-%d %H:%i:%S')
                END as start_time,
                CASE 
                    WHEN MAX(last_run_date_time) = '1970-01-01 00:00:00' THEN formatDateTime(parseDateTime64BestEffortOrNull({{ current_time }}), '%Y-%m-%d %H:%i:%S')
                    ELSE formatDateTime(LEAST(MAX(last_run_date_time) + INTERVAL 1 HOUR, parseDateTime64BestEffortOrNull({{ current_time }})), '%Y-%m-%d %H:%i:%S')
                END as end_time
            FROM {{ target.schema }}.model_metadata FINAL
            WHERE model = '{{ model }}'
        {% endset %}

        {% set last_run_result = run_query(last_run_query) %}

        {% if last_run_result is none %}
            {{ log("Query did not return any results.", "warn") }}
            {{ return({'start_time': None, 'end_time': None}) }}
        {% else %}
            {% set start_time = last_run_result.columns[0].values()[0] %}
            {% set end_time = last_run_result.columns[1].values()[0] %}

            -- Return start_time and end_time
            {{ return({'start_time': start_time, 'end_time': end_time}) }}
        {% endif %}
    {% else %}
        {{ return({'start_time': None, 'end_time': None}) }}
    {% endif %}
{% endmacro %}
