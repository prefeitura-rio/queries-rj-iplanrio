{% macro incremental_filter(column_name='data_particao') %}
    {% if is_incremental() or (target.name not in ['pr_prod', 'prod']) %}
        where {{ column_name }} in (
            CAST(current_date AS STRING), 
            CAST(date_sub(current_date, interval 1 day) AS STRING)
        )
    {% endif %}
{% endmacro %}