{% macro debug_var(var_name) %}
  {% set value = var(var_name, 'undefined') %}
  {{ log(var_name ~ ' = ' ~ value, info=True) }}
{% endmacro %}
