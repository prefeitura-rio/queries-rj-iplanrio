{% macro padronize_id(column) %}
    safe_cast(regexp_replace(cast({{ column }} as string), r'\.0$', '') as string)
{% endmacro %}
