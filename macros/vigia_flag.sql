{% macro vigia_flag(column) -%}
    CASE
        WHEN LOWER(CAST({{ column }} AS STRING)) IN ('1', '1.0', 'true') THEN 1
        WHEN LOWER(CAST({{ column }} AS STRING)) IN ('0', '0.0', 'false') THEN 0
    END
{%- endmacro %}
