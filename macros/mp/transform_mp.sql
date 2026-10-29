{% macro transform_mp(column) %}
    CASE
        WHEN {{ column }} IS NULL OR {{ column }} = '' OR {{ column }} = ' - ' THEN NULL
        ELSE CAST(INITCAP(replace({{ column }}, '_', ' ')) AS STRING)
    END
{% endmacro %}
